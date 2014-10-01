########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import random, time, re, os, glob, shutil, sys, base64, bz2, tempfile, stat, string, thread

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils  import ConnectionUtils
from BigDataDIRAC.WorkloadManagementSystem.Client.ServerUtils       import BigDataDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils              import jobDB
from DIRAC.Core.Utilities.File                                      import getGlobbedTotalSize, getGlobbedFiles
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient       import SandboxStoreClient
from DIRAC.AccountingSystem.Client.Types.Job                        import Job as AccountingJob

# DIRAC
import DIRAC
from DIRAC                                                    import S_OK, S_ERROR
from DIRAC                                                    import gConfig, gLogger
from datetime                                                 import datetime
import threading
import time

exitFlag = 0

class InteractiveJobMonitorThread ( threading.Thread ):

    def __init__( self, user , publicIP, looptime , parentthread, output, getinfo ):
      threading.Thread.__init__( self )
      self.sshConnect = ConnectionUtils( user , publicIP )
      self.looptime = looptime
      self.parentthread = parentthread
      self.output = output
      self.getinfo = getinfo

      """
      #SandBox Settings
      """
      self.sandboxClient = SandboxStoreClient()
      self.failedFlag = True
      self.sandboxSizeLimit = 1024 * 1024 * 10

    def run( self ):
      self.log = gLogger.getSubLogger( "InteractiveJobMonitorThread" )
      self.monitoring( self.looptime, self.parentthread, self.output )

    def monitoring( self, loop, parentthread, output ):

      self.initialTiming = os.times()
      accountingReport = AccountingJob()
      accountingReport.setStartTime()

      numberJobsFlag = True
      numberJobs = 0
      numberStartedJobsDict = {}
      numberEndingJobsDict = {}

      job_pattern = re.compile( 'Job =.*?,' )
      job_pattern_2 = re.compile( 'Job =.*?\n' )
      jobid = int( re.split( "_", re.split( "/", output )[int( len( re.split( "/", output ) ) - 1 )] )[0] )

      cmd = '/bin/chmod 555 ' + self.getinfo
      returned = self.commandLaunch( cmd )

      while parentthread.isAlive():
        time.sleep( loop )
        if numberJobsFlag:
          cmd = self.getinfo + ' -c step1'
          returned = self.commandLaunch( cmd )
          self.log.info( 'InteractiveJobMonitorThread:step1:numJobs:', returned )
          if returned != None:
            if ( returned['Value'][1] != "" ):
              if re.split( "=", returned['Value'][1] )[1].strip().isdigit():
                numberJobs = int( re.split( "=", returned['Value'][1] )[1] )
            if ( numberJobs != 0 ):
              numberJobsFlag = False
              BigDataDB.setJobStatus( jobid, "Running" )
        else:
          cmd = self.getinfo + ' -c step2'
          returned = self.commandLaunch( cmd )
          self.log.info( 'InteractiveJobMonitorThread:step2:startedJobs:', returned )
          if returned != "":
            if ( returned['Value'][1] != "" ):
              startedJobs = job_pattern.findall( returned['Value'][1] )
              self.log.info( 'step2:startedJobs:', startedJobs )
          cmd = self.getinfo + ' -c step3'
          returned = self.commandLaunch( cmd )
          self.log.info( 'InteractiveJobMonitorThread:step3:endedJobs:', returned )
          if returned != "":
            if ( returned['Value'][1] != "" ):
              finishedJobs = job_pattern_2.findall( returned['Value'][1] )
              self.log.info( 'step3:finishedJobs:', finishedJobs )
              if ( len( finishedJobs ) == numberJobs ):
                BigDataDB.setJobStatus( jobid, "Done" )
                BigDataDB.setHadoopID( jobid, finishedJobs )
                self.__updateSandBox( jobid, output )

                #Update Accounting                
                EXECUTION_RESULT = {}
                EXECUTION_RESULT['CPU'] = []
                finalStat = os.times()
                for i in range( len( finalStat ) ):
                  EXECUTION_RESULT['CPU'].append( finalStat[i] - self.initialTiming[i] )
                utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
                cpuTime = utime + stime + cutime + cstime
                execTime = elapsed
                result = jobDB.getJobAttributes( jobid )
                getting = result['Value']
                acData = {
                        'User' : getting['Owner'],
                        'UserGroup' : getting['OwnerGroup'],
                        'JobGroup' : 'cesga',
                        'JobType' : 'User',
                        'JobClass' : 'unknown',
                        'ProcessingType' : 'unknown',
                        'FinalMajorStatus' : getting['Status'],
                        'FinalMinorStatus' : getting['MinorStatus'],
                        'CPUTime' : cpuTime,
                        'Site' : getting['Site'],
                        # Based on the factor to convert raw CPU to Normalized units (based on the CPU Model)
                        'NormCPUTime' : 0,
                        'ExecTime' : cpuTime,
                        'InputDataSize' : 0,
                        'OutputDataSize' : 0,
                        'InputDataFiles' : 0,
                        'OutputDataFiles' : 0,
                        'DiskSpace' : 0,
                        'InputSandBoxSize' : 0,
                        'OutputSandBoxSize' : 0,
                        'ProcessedEvents' : 0
                        }
                accountingReport.setEndTime()
                accountingReport.setValuesFromDict( acData )
                result = accountingReport.commit()



    def commandLaunch( self, cmd ):
      return self.sshConnect.sshCall( 100, cmd )

    def __updateSandBox( self, jobid, output ):

      jobInfo = BigDataDB.getJobIDInfo( jobid )
      result = self.sshConnect.scpCall( 100, output, output, False )

      if not result['OK']:
        self.log.error( 'Error to get the data from BigData Software DFS:', result )

      file_paths = []
      file_paths.append( output )
      outputSandbox = file_paths

      resolvedSandbox = self.__resolveOutputSandboxFiles( outputSandbox )
      if not resolvedSandbox['OK']:
        self.log.warn( 'Output sandbox file resolution failed:' )
        self.log.warn( resolvedSandbox['Message'] )
        self.__report( 'Failed', 'Resolving Output Sandbox' )
      fileList = resolvedSandbox['Value']['Files']
      missingFiles = resolvedSandbox['Value']['Missing']
      if missingFiles:
        self.jobReport.setJobParameter( 'OutputSandboxMissingFiles', ', '.join( missingFiles ), sendFlag = False )

      if fileList and jobid:
        self.outputSandboxSize = getGlobbedTotalSize( fileList )
        self.log.info( 'Attempting to upload Sandbox with limit:', self.sandboxSizeLimit )

        result = self.sandboxClient.uploadFilesAsSandboxForJob( fileList, jobid,
                                                           'Output', self.sandboxSizeLimit ) # 1024*1024*10
        if not result['OK']:
          self.log.error( 'Output sandbox upload failed with message', result['Message'] )
          if result.has_key( 'SandboxFileName' ):
            outputSandboxData = result['SandboxFileName']
            self.log.info( 'Attempting to upload %s as output data' % ( outputSandboxData ) )
            outputData.append( outputSandboxData )
            self.jobReport.setJobParameter( 'OutputSandbox', 'Sandbox uploaded to grid storage', sendFlag = False )
            self.jobReport.setJobParameter( 'OutputSandboxLFN',
                                            self.__getLFNfromOutputFile( outputSandboxData )[0], sendFlag = False )
          else:
            self.log.info( 'Could not get SandboxFileName to attempt upload to Grid storage' )
            return S_ERROR( 'Output sandbox upload failed and no file name supplied for failover to Grid storage' )
        else:
          # Do not overwrite in case of Error
          if not self.failedFlag:
            self.__report( 'Completed', 'Output Sandbox Uploaded' )
          self.log.info( 'Sandbox uploaded successfully' )

      return "OK"

    def __resolveOutputSandboxFiles( self, outputSandbox ):
      """Checks the output sandbox file list and resolves any specified wildcards.
         Also tars any specified directories.
      """
      missing = []
      okFiles = []
      for i in outputSandbox:
        self.log.verbose( 'Looking at OutputSandbox file/directory/wildcard: %s' % i )
        globList = glob.glob( i )
        for check in globList:
          if os.path.isfile( check ):
            self.log.verbose( 'Found locally existing OutputSandbox file: %s' % check )
            okFiles.append( check )
          if os.path.isdir( check ):
            self.log.verbose( 'Found locally existing OutputSandbox directory: %s' % check )
            cmd = ['tar', 'cf', '%s.tar' % check, check]
            result = systemCall( 60, cmd )
            if not result['OK']:
              self.log.error( 'Failed to create OutputSandbox tar', result['Message'] )
            elif result['Value'][0]:
              self.log.error( 'Failed to create OutputSandbox tar', result['Value'][2] )
            if os.path.isfile( '%s.tar' % ( check ) ):
              self.log.verbose( 'Appending %s.tar to OutputSandbox' % check )
              okFiles.append( '%s.tar' % ( check ) )
            else:
              self.log.warn( 'Could not tar OutputSandbox directory: %s' % check )
              missing.append( check )

      for i in outputSandbox:
        if not i in okFiles:
          if not '%s.tar' % i in okFiles:
            if not re.search( '\*', i ):
              if not i in missing:
                missing.append( i )

      result = {'Missing':missing, 'Files':okFiles}
      return S_OK( result )
