# $HeadURL$

import time, os, sys, re
import types
from urlparse import urlparse
# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Client.ServerUtils        import jobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient        import gProxyManager

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2Client import HadoopV2Client

from DIRAC.Interfaces.API.Dirac import Dirac

__RCSID__ = '$ID: $'

class HadoopV2:
  """
  An Hadoop V.2 provides the functionality of Hadoop that is required to use it for infrastructure.
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

    self.log = gLogger.getSubLogger( "Hadoop Version 2, no HHL, NameNode: %s" % ( NameNode ) )
    self.log.info( "Hadoop Version 2, no HHL, Port: %s" % ( Port ) )
    self.log.info( "Hadoop Version 2, no HHL, jobID: %s" % ( jobID ) )
    self.log.info( "Hadoop Version 2, no HHL, PublicIP: %s" % ( PublicIP ) )
    self.log.info( "Hadoop Version 2, no HHL, User: %s" % ( User ) )
    self.log.info( "Hadoop Version 2, no HHL, JobName: %s" % ( JobName ) )
    self.log.info( "Hadoop Version 2, no HHL, Dataset: %s" % ( Dataset ) )

  def submitNewBigJob( self ):

    tempPath = self.__tmpSandBoxDir + str( self.__jobID )
    dirac = Dirac()
    if not os.path.exists( tempPath ):
      os.makedirs( tempPath )

    settingJobSandBoxDir = dirac.getInputSandbox( self.__jobID, tempPath )
    self.log.info( 'Writting temporal SandboxDir in Server', settingJobSandBoxDir )

    jobXMLName = "job:" + str( self.__jobID ) + '.xml'
    with open( os.path.join( tempPath, jobXMLName ), 'wb' ) as temp_file:
        temp_file.write( self.jobWrapper() )
    self.log.info( 'Writting temporal Hadoop Job.xml' )

    HadoopV1cli = HadoopV2Client( self.__User , self.__publicIP )
    returned = HadoopV1cli.dataCopy( tempPath, self.__tmpSandBoxDir )
    self.log.info( 'Copy the job contain to the Hadoop Master: ', returned )

    jobInfo = jobDB.getJobAttributes( self.__jobID )
    if not jobInfo['OK']:
      return S_ERROR( jobInfo['Value'] )
    proxy = ""
    jobInfo = jobInfo['Value']
    if gProxyManager.userHasProxy( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] ):
      proxy = gProxyManager.downloadProxyToFile( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )
    else:
      proxy = self.__requestProxyFromProxyManager( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )

    returned = HadoopV1cli.jobSubmit( tempPath, jobXMLName, proxy['chain'] )
    self.log.info( 'Launch Hadoop job to the Hadoop Master: ', returned )

    if not returned['OK']:
      return S_ERROR( returned['Message'] )
    else:
      self.log.info( 'Hadoop Job ID: ', returned['Value'] )

    return S_OK( returned['Value'] )

  def submitNewBigPilot( self ):

    tempPath = self.__tmpSandBoxDir + str( self.__jobID )
    dirac = Dirac()
    if not os.path.exists( tempPath ):
      os.makedirs( tempPath )

    settingJobSandBoxDir = dirac.getInputSandbox( self.__jobID, tempPath )
    self.log.info( 'Writting temporal SandboxDir in Server', settingJobSandBoxDir )

    jobXMLName = "job:" + str( self.__jobID ) + '.xml'
    with open( os.path.join( tempPath, jobXMLName ), 'wb' ) as temp_file:
        temp_file.write( self.jobWrapper() )
    self.log.info( 'Writting temporal Hadoop Job.xml' )

    HadoopV2cli = HadoopV2Client( self.__User , self.__publicIP )
    #returned = HadoopV1cli.dataCopy( tempPath, self.__tmpSandBoxDir )
    #self.log.info( 'Copy the job contain to the Hadoop Master: ', returned )

    jobInfo = jobDB.getJobAttributes( self.__jobID )
    if not jobInfo['OK']:
      return S_ERROR( jobInfo['Value'] )
    proxy = ""
    jobInfo = jobInfo['Value']
    if gProxyManager.userHasProxy( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] ):
      proxy = gProxyManager.downloadProxyToFile( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )
    else:
      proxy = self.__requestProxyFromProxyManager( jobInfo["OwnerDN"], jobInfo["OwnerGroup"] )

    returned = HadoopV2cli.submitPilotJob( tempPath, jobXMLName, proxy['chain'] )

    self.log.info( 'Launch Hadoop pilot to the Hadoop Master: ', returned )

    if not returned['OK']:
      return S_ERROR( returned['Value'] )
    else:
      self.log.info( 'Hadoop Job ID: ', returned['Value'] )

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

  def jobWrapper( self ):
    tempPath = self.__tmpSandBoxDir + str( self.__jobID ) + "/InputSandbox" + str( self.__jobID )

    dataset = re.split( "/", self.__Dataset )
    count = 0
    datasetname = ""
    for dir in dataset:
      count = count + 1
      if ( count > 2 ):
        datasetname = datasetname + "/" + dir

    wrapperContent = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
                        <configuration>
                          <property>
                            <name>mapred.input.dir</name>
                            <value>%(datasetname)s</value>
                          </property>
                          <property>
                            <name>mapred.output.dir</name>
                            <value>%(outputfile)s</value>
                          </property>
                          <property>
                            <name>mapred.job.name</name>
                            <value>%(jobname)s</value>
                          </property>
                          <property>
                            <name>mapred.job.classpath</name>
                            <value>%(jarsPath)s</value>
                          </property>
                        </configuration> """ % { 'datasetname': datasetname, \
                                               'outputfile': tempPath + "/" + self.__JobName.replace( " ", "" ) + "_" + str( self.__jobID ), \
                                               'jobname': self.__JobName, \
                                               'jarsPath': tempPath }
    return wrapperContent
