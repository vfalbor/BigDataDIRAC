########################################################################
# $HeadURL$
# File :   BigDataJobMonitoring.py
# Author : Victor Fernandez
########################################################################

"""  This class is in charge of monitoring of all submitted and running Jobs

"""

import random, time, re
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
                      if ( JobStatus['Value'] == "Unknown" ):
                        BigDataDB.setJobStatus( jobId[0], "Submitted" )
                      if ( JobStatus['Value'] == "Running" ):
                        BigDataDB.setJobStatus( jobId[0], "Running" )


    return DIRAC.S_OK()

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

