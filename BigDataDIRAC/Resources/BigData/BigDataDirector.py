########################################################################
# $HeadURL$
# File :   BigDataDirector.py
# Author : Victor Fernandez
########################################################################

from DIRAC import S_OK, S_ERROR, gConfig

from BigDataDIRAC.Resources.BigData.SoftBigDataDirector                      import SoftBigDataDirector
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV1                   import HadoopV1
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2                   import HadoopV2
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV1Interactive        import HadoopV1Interactive
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2Interactive        import HadoopV2Interactive
from BigDataDIRAC.WorkloadManagementSystem.Client.HiveV1                     import HiveV1
from BigDataDIRAC.WorkloadManagementSystem.Client.Twister                    import Twister

__RCSID__ = '$Id: $'

class BigDataDirector( SoftBigDataDirector ):
  def __init__( self, submitPool ):

    SoftBigDataDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for BigData Director
    """
    SoftBigDataDirector.configure( self, csSection, submitPool )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    SoftBigDataDirector.configureFromSection( self, mySection )

  def _submitBigDataJobs( self, NameNode, Port, jobID, PublicIP, runningEndPointName, User, JobName, dataset, UsePilot, IsInteractive ):

    endpointsPath = "/Resources/BigDataEndPoints"
    self.log.info( 'BigDataDirector:submitBigDataJobs:getConfigInfo:' )

    runningEndPointCSPath = endpointsPath + "/" + runningEndPointName
    endPointDict = gConfig.getOptionsDict( runningEndPointCSPath )
    if not endPointDict[ 'OK' ]:
      return endPointDict
    endPointDictValues = endPointDict['Value']
    driver = endPointDictValues['BigDataSoftware']
    driverversion = endPointDictValues['BigDataSoftwareVersion']
    runningHighLevelLanguajeDict = gConfig.getOptionsDict( '%s/HighLevelLanguage' % runningEndPointCSPath )
    if not runningHighLevelLanguajeDict[ 'OK' ]:
      return runningHighLevelLanguajeDict
    runningHighLevelLanguajeValues = runningHighLevelLanguajeDict['Value']
    HHLName = runningHighLevelLanguajeValues['HLLName']
    HHLVersion = runningHighLevelLanguajeValues['HLLVersion']

    self.log.info( 'BigDataDirector:submitBigDataJobs:getConfigInfo:driver:', driver )
    self.log.info( 'BigDataDirector:submitBigDataJobs:getConfigInfo:driverversion:', driverversion )
    self.log.info( 'BigDataDirector:submitBigDataJobs:getConfigInfo:HHLName:', HHLName )
    self.log.info( 'BigDataDirector:submitBigDataJobs:getConfigInfo:HHLVersion:', HHLVersion )

    if driver == 'hadoop':
      if driverversion == "hdv1":
        if HHLName == "none":
          self.log.info( "Hadoop Job Submission" )
          hdv1 = HadoopV1( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
          if ( UsePilot == '1' ):
            result = hdv1.submitNewBigPilot()
          if ( IsInteractive == '1' ):
            hdv1 = HadoopV1Interactive( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
            result = hdv1.submitNewBigJob()
          else:
            result = hdv1.submitNewBigJob()
          if not result[ 'OK' ]:
            return result
          bdjobID = result['Value']
          return S_OK( bdjobID )
        if HHLName == "pig":
          self.log.info( "Hadoop Pig Job Submission" )
        if HHLName == "hive":
          self.log.info( "Hadoop-Hive Job Submission" )
          hive1 = HiveV1( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
          result = hive1.submitNewBigJob()
          if not result[ 'OK' ]:
            return result
          bdjobID = result['Value']
          return S_OK( bdjobID )
      if driverversion == "hdv2":
        if HHLName == "none":
          self.log.info( "Hadoop Job Submission" )
          hdv2 = HadoopV2( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
          if ( UsePilot == '1' ):
            result = hdv2.submitNewBigPilot()
          if ( IsInteractive == '1' ):
            hdv2 = HadoopV2Interactive( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
            result = hdv2.submitNewBigJob()
          else:
            result = hdv2.submitNewBigJob()
          if not result[ 'OK' ]:
            return result
          bdjobID = result['Value']
          return S_OK( bdjobID )
        if HHLName == "pig":
          self.log.info( "Hadoop Pig Job Submission" )
        if HHLName == "hive":
          self.log.info( "Hadoop-Hive Job Submission" )
          hive1 = HiveV1( NameNode, Port, jobID, PublicIP, User, JobName, dataset )
          result = hive1.submitNewBigJob()
          if not result[ 'OK' ]:
            return result
          bdjobID = result['Value']
          return S_OK( bdjobID )

    if driver == 'twister':
      twister = Twister( endpoint )
      result = twister.submitNewBigJob()
      if not result[ 'OK' ]:
        return result
      bdjobID = result['Value'][0]
      return S_OK( bdjobID )

    return S_ERROR( 'Unknown DIRAC BigData driver %s' % driver )

