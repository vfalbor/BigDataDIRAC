########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################
import random, time, re, os, glob, shutil, sys, base64, bz2, tempfile, stat, string
from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils
# DIRAC
import DIRAC
from DIRAC                                                    import S_OK, S_ERROR
from DIRAC                                                    import gConfig, gLogger

__RCSID__ = '$Id: $'
SOURCE_ENCODING = "iso-8859-1"

# Classes
###################
class HadoopV2Client:

  def __init__( self, User, PublicIP ):
    self.log = gLogger.getSubLogger( "HadoopV2Client" )
    self.user = User
    self.publicIP = PublicIP
    self.sshConnect = ConnectionUtils( self.user , self.publicIP )

  def getData( self, temSRC, tempDest ):
    cmdSeq = "hadoop dfs -get " + temSRC + " " + tempDest
    return self.sshConnect.sshCall( 100, cmdSeq )


  def jobSubmit( self, tempPath, jobXMLName, proxy ):
   """ Method to submit job
   """
   executableFile = tempPath + "/" + jobXMLName
    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
   if proxy:
    self.log.verbose( 'Setting up proxy for payload' )
    compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
    compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( executableFile, "rb" ).read(), 9 ) ).replace( '\n', '' )

    wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2, shutil
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'BigDat_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executable)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedExecutable)s" ) ) )
  os.chmod('proxy',0600)
  os.chmod('%(executable)s',0700)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "hadoop job -submit %(executable)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )              
shutil.rmtree( workingDirectory )
            """ % { 'compressedAndEncodedProxy': compressedAndEncodedProxy, \
                    'compressedAndEncodedExecutable': compressedAndEncodedExecutable, \
                    'executable': executableFile }

    fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'BigDat_', dir = tempPath )
    wrapper = os.fdopen( fd, 'w' )
    wrapper.write( wrapperContent )
    wrapper.close()

    submitFile = name

   else: # no proxy
     submitFile = executableFile

   # Copy the executable
   os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
   sFile = os.path.basename( submitFile )
   result = self.sshConnect.scpCall( 10, submitFile, '%s/%s' % ( tempPath, os.path.basename( submitFile ) ) )

   # submit submitFile to the batch system
   cmd = submitFile

   self.log.verbose( 'BigData submission command: %s' % ( cmd ) )

   result = self.sshConnect.sshCall( 10, cmd )

   if not result['OK']:
     self.log.warn( '===========> SSH BigData Hadoop V.2 result NOT OK' )
     self.log.debug( result )
     return S_ERROR( result )
   else:
     self.log.debug( 'BigData Hadoop V.2 result OK' )
   return S_OK( result )

  def delData( self, tempPath ):
    cmdSeq = "rm -Rf " + tempPath
    return self.sshConnect.sshCall( 100, cmdSeq )

  def dataCopy( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCall( 100, tempPath, tmpSandBoxDir )

  def getdata( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCall( 100, tempPath, tmpSandBoxDir, False )

  def jobStatus( self, jobId, user, host ):
    cmdSeq = "ssh -l " + user + " " + host + " 'hadoop job -status " + jobId + "| grep state'"

    gLogger.info( 'Command Submitted: ', cmdSeq )

    result = self.sshConnect.sshOnlyCall( 10, cmdSeq )
    if result['OK'] == True:
      a = re.split( "\\n", result['Value'][1] )
      matching = [s for s in a if "state" in s]
      if ( matching[0] == "Job state: SUCCEEDED" ):
        return DIRAC.S_OK( "Succeded" )
      if ( matching[0] == "Job state: RUNNING" ):
        return DIRAC.S_OK( "Running" )
      else:
        return DIRAC.S_OK( "Unknown" )
    else:
      return DIRAC.S_ERROR( result )
