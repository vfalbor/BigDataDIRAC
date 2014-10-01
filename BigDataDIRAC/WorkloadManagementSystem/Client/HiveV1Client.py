########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import random, time, re, os, glob, shutil, sys, base64, bz2, tempfile, stat, string, thread

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils

from BigDataDIRAC.WorkloadManagementSystem.private.InteractiveJobLaunch import InteractiveJobLaunchThread
from BigDataDIRAC.WorkloadManagementSystem.private.InteractiveJobMonitor import InteractiveJobMonitorThread

from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, getVO, Registry, Operations, Resources
# DIRAC
import DIRAC
from DIRAC                                                    import S_OK, S_ERROR
from DIRAC                                                    import gConfig, gLogger

__RCSID__ = '$Id: $'
SOURCE_ENCODING = "iso-8859-1"
ERROR_TOKEN = 'Invalid proxy token request'

# Classes
###################
class HiveV1Client( object ):
  jobid = 0
  user = ""
  ip = ""

  def __init__( self, User, PublicIP ):

    #Loop of monitoring each 5 (sec.)
    self.monitoringloop = 5

    self.log = gLogger.getSubLogger( "HIVEV1Client" )
    self.user = User
    self.publicIP = PublicIP
    self.sshConnect = ConnectionUtils( self.user , self.publicIP )

  def jobSubmit( self, tempPath, HiveJob, proxy, HiveJobOutput ):
   """ Method to submit job
   """
    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
   HiveJobPath = tempPath + HiveJob
   if proxy:
    self.log.verbose( 'Setting up proxy for payload' )
    compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) ).replace( '\n', '' )
    compressedAndEncodedExecutable = base64.encodestring( bz2.compress( open( HiveJobPath, "rb" ).read(), 9 ) ).replace( '\n', '' )

    wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2, shutil
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'BigDat_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedProxy)s" ) ) )
  open( '%(executablepath)s', "w" ).write(bz2.decompress( base64.decodestring( "%(compressedAndEncodedExecutable)s" ) ) )
  os.chmod('proxy',0600)
  os.chmod('%(executablepath)s',0700)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "cd /tmp/hive_jobs/; hive -f %(executable)s > %(HiveJobOutput)s 2>&1"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )              
shutil.rmtree( workingDirectory )
              """ % { 'compressedAndEncodedProxy': compressedAndEncodedProxy, \
                    'compressedAndEncodedExecutable': compressedAndEncodedExecutable, \
                    'executablepath': HiveJobPath, \
                    'executable': HiveJob, \
                    'HiveJobOutput': HiveJobOutput }

    fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'BigDat_', dir = tempPath )
    wrapper = os.fdopen( fd, 'w' )
    wrapper.write( wrapperContent )
    wrapper.close()

    submitFile = name

    wrapperContent = """#!/usr/bin/env python
# Wrapper script for executable and proxy
import os, tempfile, sys, base64, bz2, shutil, getopt,re

def main(argv):
   inputfile = ''
   command = ''
   try:
      opts, args = getopt.getopt(argv,'h:c:',[''])
   except getopt.GetoptError:
      print 'name.py -c <command>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'name.py -c <command>'
         sys.exit()
      elif opt in ('-c', '--command'):
         command = arg
   if (command == 'step1'):
        cmd = 'grep "Total MapReduce jobs" %(HiveJobOutput)s'
        returned = os.system(cmd)
   if (command == 'step2'):
        cmd = 'grep "Starting Job" %(HiveJobOutput)s'
        returned = os.system(cmd)
   if (command == 'step3'):
        cmd = 'grep "Ended Job" %(HiveJobOutput)s'
        returned = os.system(cmd)

if __name__ == '__main__':
   main(sys.argv[1:])     
              """ % { 'HiveJobOutput': tempPath + HiveJobOutput }

    fd, name = tempfile.mkstemp( suffix = '_getInfo.py', prefix = 'BigDat_', dir = tempPath )
    wrapper = os.fdopen( fd, 'w' )
    wrapper.write( wrapperContent )
    wrapper.close()

    submitFile2 = name

   else: # no proxy
     submitFile = HiveJob

   # Copy the executable
   os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
   sFile = os.path.basename( submitFile )
   returned = self.sshConnect.scpCall( 10, submitFile, '%s/%s' % ( tempPath, os.path.basename( submitFile ) ) )
   returned2 = self.sshConnect.scpCall( 10, submitFile2, '%s/%s' % ( tempPath, os.path.basename( submitFile2 ) ) )

   if not returned['OK']:
      return S_ERROR( returned['Message'] )
   if not returned2['OK']:
      return S_ERROR( returned2['Message'] )

   # submit submitFile to the batch system
   cmd = submitFile

   self.log.verbose( 'BigData submission command: %s' % ( cmd ) )

   thread1 = InteractiveJobLaunchThread( self.user, self.publicIP , cmd )
   thread2 = InteractiveJobMonitorThread( self.user, self.publicIP, self.monitoringloop, thread1, tempPath + HiveJobOutput, submitFile2 )

   thread1.start()
   thread2.start()

   self.log.debug( 'BigData Hadoop-HIVE V.1 result OK', thread1.getName() )
   if not thread1.isAlive():
     self.log.warn( '===========> SSH BigData Hadoop-Hive thread V.1 result NOT OK' )
     return S_ERROR( "Error launching Hadoop-Hive Thread" )
   else:
     self.log.debug( 'BigData Hadoop-Hive Thread V.1 result OK' )
     return S_OK( " Hadoop-Hive Job" )

  def getData( self, temSRC, tempDest ):
    cmdSeq = "hadoop dfs - get " + temSRC + " " + tempDest
    return self.sshConnect.sshCall( 86400, cmdSeq )

  def delData( self, tempPath ):
    cmdSeq = "rm -Rf " + tempPath
    return self.sshConnect.sshCall( 100, cmdSeq )

  def dataCopy( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCall( 100, tempPath, tmpSandBoxDir )

  def getdata( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCall( 100, tempPath, tmpSandBoxDir, False )

  def jobStatus( self, jobId, user, host ):
    cmdSeq = "ssh - l " + user + " " + host + " 'hadoop job -list all | awk -v job_id=" + jobId.strip() + " "\
        " '\"'\"'BEGIN{OFS=\"\t\"; FS=\"\t\"; final_state=\"Unknown\"} "\
        "$0 == \"States are:\" {getline; for(i=1;i<=NF;i++) { split($i,s,\" \"); states[s[3]] = s[1] }} $1==job_id { final_state=states[$2]; exit} END{print final_state}'\"'\""

    gLogger.info( 'Command Submitted: ', cmdSeq )
    return self.sshConnect.sshOnlyCall( 10, cmdSeq )
