########################################################################
# $HeadURL$
# File :   BigDataJobMonitoring.py
# Author : Victor Fernandez
########################################################################

"""  This class is in charge of monitoring of all submitted and running Jobs

"""

import random, time, re, os, glob, shutil
from DIRAC import S_OK
import DIRAC

from numpy.random import poisson
from random       import shuffle

# DIRAC
from DIRAC                                             import gConfig
from DIRAC.Core.Base.AgentModule                       import AgentModule
from DIRAC.Core.Utilities.ThreadPool                   import ThreadPool

from BigDataDIRAC.Resources.BigData.BigDataDirector           import BigDataDirector
from BigDataDIRAC.WorkloadManagementSystem.Client.ServerUtils import BigDataDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import jobDB

from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient   import SandboxStoreClient
from DIRAC.Core.Utilities.File                              import getGlobbedTotalSize, getGlobbedFiles

from DIRAC.Interfaces.API.Dirac import Dirac

__RCSID__ = "$Id: $"

#FIXME: why do we need the random seed ?
random.seed()

class BigDataJobMonitoring( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """
    import threading

    self.am_setOption( "PollingTime", 5 )

    self.am_setOption( "ThreadStartDelay", 1 )
    self.am_setOption( "SubmitPools", [] )
    self.am_setOption( "DefaultSubmitPools", [] )

    self.am_setOption( "minThreadsInPool", 0 )
    self.am_setOption( "maxThreadsInPool", 2 )
    self.am_setOption( "totalThreadsInPool", 40 )

    self.callBackLock = threading.Lock()
    self.pendingJobs = {}
    self.monitoringEndPoints = {}

    """
    #SandBox Settings
    """
    self.__tmpSandBoxDir = "/tmp/"
    self.sandboxClient = SandboxStoreClient()
    self.failedFlag = True
    self.sandboxSizeLimit = 1024 * 1024 * 10

    self.cleanDataAfterFinish = True

    return DIRAC.S_OK()

  def execute( self ):
    """Main Agent code:
      1.- Query BigDataDB for existing Running, Queue, or Submitted jobs
      2.- Ask about the status
      3.- Change the status into DB in the case of had changed
    """

    self.pendingJobs['Submitted'] = BigDataDB.getBigDataJobsByStatus( "Submitted" )
    self.pendingJobs['Running'] = BigDataDB.getBigDataJobsByStatus( "Running" )
    self.pendingJobs['Unknown'] = BigDataDB.getBigDataJobsByStatus( "Unknown" )

    self.__getMonitoringPools()
    self.log.verbose( 'monitoring pools', self.monitoringEndPoints )

    for status in self.pendingJobs:
      self.log.verbose( 'Analizing %s jobs' % status )
      JobStatus = 0
      if self.pendingJobs[status]['OK']:
        for jobId in self.pendingJobs[status]['Value']:
          self.log.verbose( 'Analizing job %s' % jobId )
          getSoftIdAndSiteName = BigDataDB.getSoftwareJobIDByJobID( jobId[0] )
          self.log.verbose( 'Site and SoftID:', getSoftIdAndSiteName )
          for runningEndPoint in  self.monitoringEndPoints:
            if ( ( self.monitoringEndPoints[runningEndPoint]['NameNode'] == getSoftIdAndSiteName[0][1] ) and
                 ( getSoftIdAndSiteName[0][0] != "" ) ):
              #Depending on the BigData Software the Query should be different
              if self.monitoringEndPoints[runningEndPoint]['BigDataSoftware'] == 'hadoop':
                if self.monitoringEndPoints[runningEndPoint]['BigDataSoftwareVersion'] == 'hdv1':
                  if self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLName'] == 'none':
                    self.log.info( "Hadoop V.1 Monitoring submmission command with Hadoop jobID: ", getSoftIdAndSiteName[0][0] )
                    from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV1Client import HadoopV1Client
                    HadoopV1cli = HadoopV1Client( self.monitoringEndPoints[runningEndPoint]['User'] ,
                                                  self.monitoringEndPoints[runningEndPoint]['PublicIP'] )
                    JobStatus = HadoopV1cli.jobStatus( getSoftIdAndSiteName[0][0],
                                                       self.monitoringEndPoints[runningEndPoint]['User'],
                                                       self.monitoringEndPoints[runningEndPoint]['PublicIP'] )
                    if JobStatus['OK'] == True:
                      if ( JobStatus['Value'][1].strip() == "Succeded" ):
                        BigDataDB.setJobStatus( jobId[0], "Done" )
                        self.__updateSandBox( jobId[0],
                                             self.monitoringEndPoints[runningEndPoint]['BigDataSoftware'],
                                             self.monitoringEndPoints[runningEndPoint]['BigDataSoftwareVersion'] ,
                                             self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLName'],
                                             self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLVersion'],
                                             HadoopV1cli )
                        if cleanDataAfterFinish:
                          self.__deleteData( jobId[0], HadoopV1cli )
                      if ( JobStatus['Value'][1].strip() == "Unknown" ):
                        BigDataDB.setJobStatus( jobId[0], "Submitted" )
                      if ( JobStatus['Value'][1].strip() == "Running" ):
                        BigDataDB.setJobStatus( jobId[0], "Running" )

              if self.monitoringEndPoints[runningEndPoint]['BigDataSoftware'] == 'hadoop':
                if self.monitoringEndPoints[runningEndPoint]['BigDataSoftwareVersion'] == 'hdv2':
                  if self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLName'] == 'none':
                    self.log.info( "Hadoop V.2 Monitoring submmission command with Hadoop jobID: ", getSoftIdAndSiteName[0][0] )
                    from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2Client import HadoopV2Client
                    HadoopV2cli = HadoopV2Client( self.monitoringEndPoints[runningEndPoint]['User'] ,
                                                  self.monitoringEndPoints[runningEndPoint]['PublicIP'] )
                    JobStatus = HadoopV2cli.jobStatus( getSoftIdAndSiteName[0][0],
                                                       self.monitoringEndPoints[runningEndPoint]['User'],
                                                       self.monitoringEndPoints[runningEndPoint]['PublicIP'] )
                    if JobStatus['OK'] == True:
                      if ( JobStatus['Value'] == "Succeded" ):
                        BigDataDB.setJobStatus( jobId[0], "Done" )
                        self.__updateSandBox( jobId[0],
                                             self.monitoringEndPoints[runningEndPoint]['BigDataSoftware'],
                                             self.monitoringEndPoints[runningEndPoint]['BigDataSoftwareVersion'] ,
                                             self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLName'],
                                             self.monitoringEndPoints[runningEndPoint]['HighLevelLanguage']['HLLVersion'],
                                             HadoopV2cli )
                        if cleanDataAfterFinish:
                          self.__deleteData( jobId[0], HadoopV2cli )
                      if ( JobStatus['Value'] == "Unknown" ):
                        BigDataDB.setJobStatus( jobId[0], "Submitted" )
                      if ( JobStatus['Value'] == "Running" ):
                        BigDataDB.setJobStatus( jobId[0], "Running" )


    return DIRAC.S_OK()

  def __deleteData( self, jobid, cli ):
    source = self.__tmpSandBoxDir + str( jobid )
    shutil.rmtree( source )
    result = cli.delData( source )
    if not result['OK']:
      self.log.error( 'Error the data on BigData cluster could not be deleted', result )
      continue
    return 'Data deleted'

  def __updateSandBox( self, jobid, software, version, hll, hllversion, cli ):
    jobInfo = BigDataDB.getJobIDInfo( jobid )

    source = self.__tmpSandBoxDir + str( jobid ) + "/InputSandbox" + str( jobid ) + "/" + \
      self.__getJobName( jobInfo[0][0] ) + "_" + str( jobid )
    dest = self.__tmpSandBoxDir + str( jobid ) + "/" + \
      self.__getJobName( jobInfo[0][0] ) + "_" + str( jobid )
    result = 0
    if ( ( software == "hadoop" ) and ( version == "hdv1" ) and ( hll == "none" ) ):
      result = cli.getData( source , dest )
    if ( ( software == "hadoop" ) and ( version == "hdv2" ) and ( hll == "none" ) ):
      result = cli.getData( source , dest )
    if not result['OK']:
      self.log.error( 'Error to get the data from BigData Software DFS:', result )

    result = cli.getdata( dest, dest )
    if not result['OK']:
      self.log.error( 'Error to get the data from BigData Cluster to DIRAC:', result )

    outputSandbox = self.get_filepaths( dest )

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

  def __getLFNfromOutputFile( self, outputFile, outputPath = '' ):
    """Provides a generic convention for VO output data
       files if no path is specified.
    """

    if not re.search( '^LFN:', outputFile ):
      localfile = outputFile
      initial = self.owner[:1]
      vo = getVOForGroup( self.userGroup )
      if not vo:
        vo = 'dirac'
      basePath = '/' + vo + '/user/' + initial + '/' + self.owner
      if outputPath:
        # If output path is given, append it to the user path and put output files in this directory
        if outputPath.startswith( '/' ):
          outputPath = outputPath[1:]
      else:
        # By default the output path is constructed from the job id 
        subdir = str( self.jobID / 1000 )
        outputPath = subdir + '/' + str( self.jobID )
      lfn = os.path.join( basePath, outputPath, os.path.basename( localfile ) )
    else:
      # if LFN is given, take it as it is
      localfile = os.path.basename( outputFile.replace( "LFN:", "" ) )
      lfn = outputFile.replace( "LFN:", "" )

    return ( lfn, localfile )

  def get_filepaths( self, directory ):
    """
    This function will generate the file names in a directory
    """
    file_paths = []
    for root, directories, files in os.walk( directory ):
        for filename in files:
            filepath = os.path.join( root, filename )
            file_paths.append( filepath )
    return file_paths

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

  def __getJobName( self, jobName ):
    result = re.split( "_", jobName )
    return result[0]

  def __getMonitoringPools( self ):

    for monitoringPool in self.am_getOption( 'SubmitPools' ):
      self.log.verbose( 'Monitoring Pools', monitoringPool )
      pathPools = self.am_getModuleParam( 'section' ) + '/' + monitoringPool + '/EndPointMonitoring'
      monitorings = gConfig.getValue( pathPools )
      splitted = re.split( ",", monitorings )
      for endpoint in splitted:
        self.configureFromSection( "/Resources/BigDataEndpoints/" , endpoint )

    return "OK"



  def configureFromSection( self, mySection, endPoint ):
    """
      get CS for monitoring endpoints
    """
    self.log.debug( 'Configuring from %s' % mySection )

    monitoringBDEndPointDict = BigDataDB.getRunningEnPointDict( endPoint )
    if not monitoringBDEndPointDict['OK']:
      self.log.error( 'Error in RunninggBDEndPointDict: %s' % monitoringBDEndPointDict['Message'] )
      return monitoringBDEndPointDict
    self.log.verbose( 'Trying to configure RunningBDEndPointDict:', monitoringBDEndPointDict )
    monitoringBDEndPointDict = monitoringBDEndPointDict[ 'Value' ]
    for option in ['NameNode', 'Port', 'SiteName', 'BigDataSoftware',
                   'BigDataSoftwareVersion', 'HighLevelLanguage',
                   'LimitQueueJobsEndPoint', 'URL', 'PublicIP']:
      if option not in monitoringBDEndPointDict.keys():
        self.log.error( 'Missing option in "%s" EndPoint definition:' % endPoint, option )
        continue

    self.monitoringEndPoints[endPoint] = {}
    self.monitoringEndPoints[endPoint]['NameNode'] = monitoringBDEndPointDict['NameNode']
    self.monitoringEndPoints[endPoint]['Port'] = int ( monitoringBDEndPointDict['Port'] )
    self.monitoringEndPoints[endPoint]['SiteName'] = monitoringBDEndPointDict['SiteName']
    self.monitoringEndPoints[endPoint]['BigDataSoftware'] = monitoringBDEndPointDict['BigDataSoftware']
    self.monitoringEndPoints[endPoint]['BigDataSoftwareVersion'] = monitoringBDEndPointDict['BigDataSoftwareVersion']
    self.monitoringEndPoints[endPoint]['LimitQueueJobsEndPoint'] = int( monitoringBDEndPointDict['LimitQueueJobsEndPoint'] )
    self.monitoringEndPoints[endPoint]['URL'] = monitoringBDEndPointDict['URL']
    self.monitoringEndPoints[endPoint]['User'] = monitoringBDEndPointDict['User']
    self.monitoringEndPoints[endPoint]['PublicIP'] = monitoringBDEndPointDict['PublicIP']

    self.monitoringEndPoints[endPoint]['HighLevelLanguage'] = monitoringBDEndPointDict['HighLevelLanguage']

