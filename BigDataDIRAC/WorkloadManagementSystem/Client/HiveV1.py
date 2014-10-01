# $HeadURL$

import time, os, sys, re
import types
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from DIRAC.WorkloadManagementSystem.Client.ServerUtils              import jobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient                import gProxyManager
from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils  import ConnectionUtils
from BigDataDIRAC.WorkloadManagementSystem.Client.HiveV1Client      import HiveV1Client
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                      import ClassAd

from DIRAC.Interfaces.API.Dirac                                     import Dirac

__RCSID__ = '$ID: $'

class HiveV1:
  """
  An Hadoop V.1 provides the functionality of Hadoop that is required to use it for infrastructure.
  """

  def __init__( self, NameNode, Port, jobID, PublicIP, User, JobName, Dataset ):

    self.__tmpSandBoxDir = "/tmp/hive_jobs/"

    self.__NameNode = NameNode
    self.__Port = Port
    self.__jobID = jobID
    self.__publicIP = PublicIP
    self.__User = User
    self.__JobName = JobName
    self.__Dataset = Dataset

    self.defaultProxyLength = gConfig.getValue( '/Registry/DefaultProxyLifeTime', 86400 * 5 )

    self.log = gLogger.getSubLogger( "Hadoop Version 1, Hive HHL, NameNode: %s" % ( NameNode ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, Port: %s" % ( Port ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, jobID: %s" % ( jobID ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, PublicIP: %s" % ( PublicIP ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, User: %s" % ( User ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, JobName: %s" % ( JobName ) )
    self.log.info( "Hadoop Version 1, Hive Version 1, Dataset: %s" % ( Dataset ) )

  def submitNewBigJob( self ):

    result = jobDB.getJobJDL( str( self.__jobID ) , True )
    classAdJob = ClassAd( result['Value'] )
    executableFile = ""
    if classAdJob.lookupAttribute( 'Executable' ):
      executableFile = classAdJob.getAttributeString( 'Executable' )

    tempPath = self.__tmpSandBoxDir
    dirac = Dirac()
    if not os.path.exists( tempPath ):
      os.makedirs( tempPath )

    settingJobSandBoxDir = dirac.getInputSandbox( self.__jobID, tempPath )
    self.log.info( 'Writting temporal SandboxDir in Server', settingJobSandBoxDir )
    moveData = self.__tmpSandBoxDir + "/InputSandbox" + str( self.__jobID )

    HiveV1Cli = HiveV1Client( self.__User , self.__publicIP )
    returned = HiveV1Cli.dataCopy( moveData, self.__tmpSandBoxDir )
    self.log.info( 'Copy the job contain to the Hadoop Master with HIVE: ', returned )

    jobInfo = jobDB.getJobAttributes( self.__jobID )
    if not jobInfo['OK']:
      return S_ERROR( jobInfo['Value'] )
    proxy = ""
    jobInfo = jobInfo['Value']
    if gProxyManager.userHasProxy( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] ):
      proxy = gProxyManager.downloadProxyToFile( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )
    else:
      proxy = self.__requestProxyFromProxyManager( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )

    HiveJob = "InputSandbox" + str( self.__jobID ) + "/" + executableFile
    HiveJobOutput = str( self.__jobID ) + "_" + executableFile + "_out"

    returned = HiveV1Cli.jobSubmit( tempPath, HiveJob, proxy['chain'], HiveJobOutput )
    self.log.info( 'Launch Hadoop-Hive job to the Master: ', returned )

    if not returned['OK']:
      return S_ERROR( returned['Message'] )
    else:
      self.log.info( 'Hadoop-Hive Job ID: ', returned['Value'] )

    return S_OK( returned['Value'] )

  def __requestProxyFromProxyManager( self, ownerDN, ownerGroup ):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """
    self.log.info( "Requesting proxy for %s@%s" % ( ownerDN, ownerGroup ) )
    token = gConfig.getValue( "/Security/ProxyToken", "" )
    if not token:
      self.log.info( "No token defined. Trying to download proxy without token" )
      token = False
    retVal = gProxyManager.getPayloadProxyFromDIRACGroup( ownerDN, ownerGroup,
                                                          self.defaultProxyLength, token )
    if not retVal[ 'OK' ]:
      self.log.error( 'Could not retrieve proxy' )
      self.log.warn( retVal )
      os.system( 'dirac-proxy-info' )
      sys.stdout.flush()
      return S_ERROR( 'Error retrieving proxy' )
    chain = retVal[ 'Value' ]
    return S_OK( chain )
