# $HeadURL$

import time, os, sys, re
import types
from urlparse import urlparse

# DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV1Client import HadoopV1Client

from DIRAC.Interfaces.API.Dirac import Dirac

__RCSID__ = '$ID: $'

class HadoopV1:
  """
  An Hadoop V.1 provides the functionality of Hadoop that is required to use it for infrastructure.
  """

  def __init__( self, NameNode, Port, jobID, PublicIP, User, JobName ):

    self.__tmpSandBoxDir = "/tmp/"

    self.__NameNode = NameNode
    self.__Port = Port
    self.__jobID = jobID
    self.__publicIP = PublicIP
    self.__User = User
    self.__JobName = JobName

    self.log = gLogger.getSubLogger( "Hadoop Version 1, no HHL, NameNode: %s" % ( NameNode ) )
    self.log.info( "Hadoop Version 1, no HHL, Port: %s" % ( Port ) )
    self.log.info( "Hadoop Version 1, no HHL, jobID: %s" % ( jobID ) )
    self.log.info( "Hadoop Version 1, no HHL, PublicIP: %s" % ( PublicIP ) )
    self.log.info( "Hadoop Version 1, no HHL, User: %s" % ( User ) )
    self.log.info( "Hadoop Version 1, no HHL, JobName: %s" % ( JobName ) )

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

    HadoopV1cli = HadoopV1Client( self.__User , self.__publicIP )
    returned = HadoopV1cli.dataCopy( tempPath, self.__tmpSandBoxDir )
    self.log.info( 'Copy the job contain to the Hadoop Master: ', returned )
    returned = HadoopV1cli.jobSubmit( tempPath, jobXMLName )
    self.log.info( 'Launch Hadoop job to the Hadoop Master: ', returned )

    if not returned['OK'] or returned['Value'][0] != 0:
      return S_ERROR( returned['Value'] )
    else:
      hadoopID = self.getHadoopJobValue( returned['Value'] )
      self.log.info( 'Hadoop Job ID: ', hadoopID )

    return S_OK( hadoopID )

  def getHadoopJobValue( self, jobOutput ):
    value = re.split( " ", jobOutput[1] )
    return value[2]

  def jobWrapper( self ):
    #XXXXXXXXXXXXchangeXXXXXXXXXXXXXXXX
    jobNameSplitted = re.split( '_', self.__JobName )
    #XXXXXXXXXXXXchangeXXXXXXXXXXXXXXXX

    tempPath = self.__tmpSandBoxDir + str( self.__jobID ) + "/InputSandbox" + str( self.__jobID )

    #XXXXXXXXXXXXchangeXXXXXXXXXXXXXXXX
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
                        </configuration> """ % { 'datasetname': jobNameSplitted[5], \
                                               'outputfile': tempPath + "/" + jobNameSplitted[0] + "_" + str( self.__jobID ), \
                                               'jobname': jobNameSplitted[0], \
                                               'jarsPath': tempPath }
    #XXXXXXXXXXXXchangeXXXXXXXXXXXXXXXX
    return wrapperContent
