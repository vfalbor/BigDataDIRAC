# $HeadURL$

import time, os, sys, re
import types
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from DIRAC.WorkloadManagementSystem.Client.ServerUtils              import jobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient                import gProxyManager
from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils  import ConnectionUtils
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2InteractiveClient      import HadoopV2InteractiveClient
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                      import ClassAd

from DIRAC.Interfaces.API.Dirac                                     import Dirac

__RCSID__ = '$ID: $'

class HadoopV2Interactive:
  """
  An Hadoop V.1 provides the functionality of Hadoop that is required to use it for infrastructure.
  """

  def __init__( self, NameNode, Port, jobID, PublicIP, User, JobName, Dataset ):

    self.__tmpSandBoxDir = "/tmp/"

    self.__NameNode = NameNode
    self.__Port = Port
    self.__jobID = jobID
    self.__publicIP = PublicIP
    self.__User = User
    self.__JobName = JobName
    self.__Dataset = Dataset

    self.defaultProxyLength = gConfig.getValue( '/Registry/DefaultProxyLifeTime', 86400 * 5 )

    self.log = gLogger.getSubLogger( "Hadoop Version 2, HadoopInteractive HHL, NameNode: %s" % ( NameNode ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, Port: %s" % ( Port ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, jobID: %s" % ( jobID ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, PublicIP: %s" % ( PublicIP ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, User: %s" % ( User ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, JobName: %s" % ( JobName ) )
    self.log.info( "Hadoop Version 2, HadoopInteractive Version 2, Dataset: %s" % ( Dataset ) )

  def submitNewBigJob( self ):

    #1.- Creamos carpeta temporal
    self.log.debug( 'Step1::: mkdir temp folder' )
    tempPath = self.__tmpSandBoxDir + str( self.__jobID ) + "/"
    dirac = Dirac()
    if not os.path.exists( tempPath ):
      os.makedirs( tempPath )

    #2.- Introducimos el contenido del inputsandbox en la carpeta temporal
    self.log.debug( 'Step2::: download inputsand to temp folder' )
    settingJobSandBoxDir = dirac.getInputSandbox( self.__jobID, tempPath )
    self.log.info( 'Writting temporal SandboxDir in Server', settingJobSandBoxDir )
    moveData = tempPath + "/InputSandbox" + str( self.__jobID )

    #3.- Move the data to client
    self.log.debug( 'Step2::: download inputsandbox to temp folder' )
    HadoopV2InteractiveCli = HadoopV2InteractiveClient( self.__User , self.__publicIP )
    returned = HadoopV2InteractiveCli.dataCopy( tempPath, self.__tmpSandBoxDir )
    self.log.debug( 'Returned of copy the job contain to the Hadoop Master with HadoopInteractive::: ', returned )

    #3.- Get executable file
    result = jobDB.getJobJDL( str( self.__jobID ) , True )
    classAdJob = ClassAd( result['Value'] )
    executableFile = ""
    if classAdJob.lookupAttribute( 'Executable' ):
      executableFile = classAdJob.getAttributeString( 'Executable' )
    self.log.debug( 'Step3::: Get executable file: ', executableFile )

    jobInfo = jobDB.getJobAttributes( self.__jobID )
    if not jobInfo['OK']:
      return S_ERROR( jobInfo['Value'] )
    proxy = ""
    jobInfo = jobInfo['Value']
    if gProxyManager.userHasProxy( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] ):
      proxy = gProxyManager.downloadProxyToFile( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )
    else:
      proxy = self.__requestProxyFromProxyManager( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )

    HadoopInteractiveJob = "InputSandbox" + str( self.__jobID ) + "/" + executableFile
    HadoopInteractiveJobCommand = "InputSandbox" + str( self.__jobID ) + "/" + executableFile + " " + self.__JobName
    HadoopInteractiveJobOutput = tempPath + str( self.__jobID ) + "_" + executableFile + "_out"

    #4.- Creating second part of the job name
    if ( len( re.split( " ", self.__JobName ) ) > 1 ):
    #(name for random writter with -D)name_job = re.split( " ", self.__JobName )[0] + " " + re.split( " ", self.__JobName )[1] + " " + re.split( " ", self.__JobName )[2]
      name_job = re.split( " ", self.__JobName )[0] + " " + re.split( " ", self.__JobName )[1]
    #(name for random writter with -D)output_job = moveData + "/" + re.split( " ", self.__JobName )[3]

    #(name for random writter with -D)cfg_job = ""
    #(name for random writter with -D)if ( len( re.split( " ", self.__JobName ) ) > 4 ):
    #(name for random writter with -D)  cfg_job = moveData + "/" + re.split( " ", self.__JobName )[4]

    #5.- Parsing execution command
    #cmd = "hadoop jar " + tempPath + HadoopInteractiveJob + " " + name_job + " " + output_job + " " + cfg_job
      cmd = "hadoop jar " + tempPath + HadoopInteractiveJob + " " + name_job + " " + tempPath + "/dataset-USC-a-grep '[and]+'"
    else:
      dataset = re.split( "/", self.__Dataset )
      count = 0
      datasetname = ""
      for dir in dataset:
        count = count + 1
        if ( count > 2 ):
          datasetname = datasetname + "/" + dir
      cmd = "hadoop jar " + tempPath + HadoopInteractiveJob + " " + self.__JobName + " " + datasetname + " " + tempPath + "/" + self.__JobName.replace( " ", "" ) + "_" + str( self.__jobID )
    self.log.debug( 'Step4::: Making CMD for submission: ', cmd )

    self.log.debug( 'Step5::: Submit file to hadoop: ' )
    returned = HadoopV2InteractiveCli.jobSubmit( tempPath, HadoopInteractiveJob, proxy['chain'],
                                                 HadoopInteractiveJobOutput, cmd )
    self.log.info( 'Launch Hadoop-HadoopInteractive job to the Master: ', returned )

    if not returned['OK']:
      return S_ERROR( returned['Message'] )
    else:
      self.log.info( 'Hadoop-HadoopInteractive Job ID: ', returned['Value'] )

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
