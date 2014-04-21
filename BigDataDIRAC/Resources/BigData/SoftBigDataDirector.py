########################################################################
# $HeadURL$
# File :   SoftBigDataDirector.py
# Author : Victor Fernandez
########################################################################

# DIRAC
from DIRAC import alarmMail, errorMail, gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Core.Utilities import DEncode, Time

import DIRAC

# DIRACBigData
from BigDataDIRAC.WorkloadManagementSystem.Client.ServerUtils import BigDataDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import jobDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import taskQueueDB

class SoftBigDataDirector:
  def __init__( self, submitPool ):

    self.log = gLogger.getSubLogger( '%sDirector' % submitPool )

    self.errorMailAddress = errorMail
    self.alarmMailAddress = alarmMail
    self.mailFromAddress = "support@diracgrid.org"


  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for BigData Director
    """

    self.runningEndPoints = {}

    # csSection comming from BigDataJobScheduler call
    self.configureFromSection( csSection )
    # reload will add SubmitPool to csSection to get the runningEndPoints of a Director
    self.reloadConfiguration( csSection, submitPool )

    self.log.info( '===============================================' )
    self.log.info( 'Configuration:' )
    for endpoint in self.runningEndPoints:
      self.log.info( '===============================================' )
      self.log.info( 'Endpoint:', endpoint )
      for data in self.runningEndPoints[endpoint]:
        self.log.info( '%s : %s' % ( data, self.runningEndPoints[endpoint][data] ) )
    self.log.info( '===============================================' )
    if self.runningEndPoints:
      self.log.info( ', '.join( self.runningEndPoints ) )
    else:
      self.log.info( ' None' )

  def reloadConfiguration( self, csSection, submitPool ):
    """
     For the SubmitPool
    """
    mySection = csSection + '/' + submitPool
    self.configureFromSection( mySection )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    self.log.debug( 'Configuring from %s' % mySection )
    self.errorMailAddress = gConfig.getValue( mySection + '/ErrorMailAddress' , self.errorMailAddress )
    self.alarmMailAddress = gConfig.getValue( mySection + '/AlarmMailAddress' , self.alarmMailAddress )
    self.mailFromAddress = gConfig.getValue( mySection + '/MailFromAddress'  , self.mailFromAddress )

    # following will do something only when call from reload including SubmitPool as mySection
    requestedRunningEndPoints = gConfig.getValue( mySection + '/RunningEndPoints', self.runningEndPoints.keys() )

    for runningEndPointName in requestedRunningEndPoints:
      self.log.verbose( 'Trying to configure RunningEndPoint:', runningEndPointName )
      if runningEndPointName in self.runningEndPoints:
        continue
      RunningBDEndPointDict = BigDataDB.getRunningEnPointDict( runningEndPointName )
      if not RunningBDEndPointDict['OK']:
        self.log.error( 'Error in RunningBDEndPointDict: %s' % RunningBDEndPointDict['Message'] )
        return RunningBDEndPointDict
      self.log.verbose( 'Trying to configure RunningBDEndPointDict:', RunningBDEndPointDict )
      RunningBDEndPointDict = RunningBDEndPointDict[ 'Value' ]
      for option in ['NameNode', 'Port', 'SiteName', 'BigDataSoftware',
                     'BigDataSoftwareVersion', 'HighLevelLanguage',
                     'LimitQueueJobsEndPoint', 'URL', 'PublicIP']:
        if option not in RunningBDEndPointDict.keys():
          self.log.error( 'Missing option in "%s" EndPoint definition:%s' % ( runningEndPointName, option ) )
          continue

      self.runningEndPoints[runningEndPointName] = {}
      self.runningEndPoints[runningEndPointName]['NameNode'] = RunningBDEndPointDict['NameNode']
      self.runningEndPoints[runningEndPointName]['Port'] = int ( RunningBDEndPointDict['Port'] )
      self.runningEndPoints[runningEndPointName]['SiteName'] = RunningBDEndPointDict['SiteName']
      self.runningEndPoints[runningEndPointName]['BigDataSoftware'] = RunningBDEndPointDict['BigDataSoftware']
      self.runningEndPoints[runningEndPointName]['BigDataSoftwareVersion'] = RunningBDEndPointDict['BigDataSoftwareVersion']
      self.runningEndPoints[runningEndPointName]['LimitQueueJobsEndPoint'] = int( RunningBDEndPointDict['LimitQueueJobsEndPoint'] )
      self.runningEndPoints[runningEndPointName]['URL'] = RunningBDEndPointDict['URL']
      self.runningEndPoints[runningEndPointName]['User'] = RunningBDEndPointDict['User']
      self.runningEndPoints[runningEndPointName]['PublicIP'] = RunningBDEndPointDict['PublicIP']

      self.runningEndPoints[runningEndPointName]['HighLevelLanguage'] = RunningBDEndPointDict['HighLevelLanguage']

      self.runningEndPoints[runningEndPointName]['Requirements'] = RunningBDEndPointDict['Requirements']
      self.runningEndPoints[runningEndPointName]['Requirements']['CPUTime'] = int ( self.runningEndPoints[runningEndPointName]['Requirements']['CPUTime'] )

  def submitBigDataJobs( self, endpoint, numBigDataJobsAllowed, runningSiteName, NameNode,
                         BigDataSoftware, BigDataSoftwareVersion, HLLName, HLLVersion,
                         PublicIP, Port, jobIds , runningEndPointName, JobName, User ):
    """
    Big Data job submission with all the parameters of SITE and Job
    """

    self.log.info( 'Director:submitBigDataJobs:JobSubmisionProcess' )

    if ( numBigDataJobsAllowed <= 0 ):
      return S_ERROR( "Number of slots reached for %s in the NameNode " % runningSiteName, NameNode )
    if NameNode not in self.runningEndPoints[endpoint]['NameNode']:
      return S_ERROR( 'Unknown NameNode: %s' % NameNode )

    newJob = BigDataDB.insertBigDataJob( jobIds, JobName, Time.toString(), NameNode,
                                              runningSiteName, PublicIP, "", "",
                                              "", BigDataSoftware, BigDataSoftwareVersion, HLLName,
                                              HLLVersion, "Submitted" )

    self.log.info( 'Director:submitBigDataJobs:SubmitJob' )
    dictBDJobSubmitted = self._submitBigDataJobs( NameNode, Port, jobIds, PublicIP,
                                                  runningEndPointName, User, JobName )

    if not dictBDJobSubmitted[ 'OK' ]:
      return dictBDJobSubmitted
    bdjobID = dictBDJobSubmitted['Value']
    result = BigDataDB.setHadoopID( jobIds, bdjobID )
    self.log.info( 'Director:submitBigDataJobs:JobSubmitted' )

    return S_OK( "OK" )

  def exceptionCallBack( self, threadedJob, exceptionInfo ):
    self.log.exception( 'Error in BigDataJob Submission:', lExcInfo = exceptionInfo )
