########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import random, time, re, os, glob, shutil, sys, base64, bz2, tempfile, stat, string

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils

from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals, getVO, Registry, Operations, Resources
# DIRAC
import DIRAC
from DIRAC                                                    import S_OK, S_ERROR
from DIRAC                                                    import gConfig, gLogger

__RCSID__ = '$Id: $'
SOURCE_ENCODING = "iso-8859-1"
DIRAC_PILOT = os.path.join( DIRAC.rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py' )
DIRAC_INSTALL = os.path.join( DIRAC.rootPath, 'DIRAC', 'Core', 'scripts', 'dirac-install.py' )
TRANSIENT_PILOT_STATUS = ['Submitted', 'Waiting', 'Running', 'Scheduled', 'Ready']
WAITING_PILOT_STATUS = ['Submitted', 'Waiting', 'Scheduled', 'Ready']
FINAL_PILOT_STATUS = ['Aborted', 'Failed', 'Done']
ERROR_TOKEN = 'Invalid proxy token request'

# Classes
###################
class HadoopV1Client:

  def __init__( self, User, PublicIP, Port ):

    self.queueDict = {}
    self.pilot = DIRAC_PILOT
    self.install = DIRAC_INSTALL
    self.genericPilotGroup = 'lhcb_dirac'
    self.genericPilotDN = '/DC=es/DC=irisgrid/O=cesga/CN=victor-fernandez'
    self.genericPilotGroup = 'lhcb_pilot'
    self.pilotLogLevel = 'DEBUG'


    self.log = gLogger.getSubLogger( "HadoopV1Client" )
    self.user = User
    self.publicIP = PublicIP
    self.port = Port
    self.sshConnect = ConnectionUtils( self.user , self.publicIP, self.port )

  def getData( self, temSRC, tempDest ):
    cmdSeq = "hadoop dfs -get " + temSRC + " " + tempDest
    return self.sshConnect.sshCallByPort( 100, cmdSeq )

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
   result = self.sshConnect.scpCallByPort( 10, submitFile, '%s/%s' % ( tempPath, os.path.basename( submitFile ) ) )

   # submit submitFile to the batch system
   cmd = submitFile

   self.log.verbose( 'BigData submission command: %s' % ( cmd ) )

   result = self.sshConnect.sshCallByPort( 10, cmd )

   self.log.debug( 'BigData Hadoop V.1 result OK', result )
   if not result['OK']:
     for getit in result['Value']:
       resulting = re.search( "job_(\d+)_(\d+)", str( getit ) )
       if ( resulting != None ):
         self.log.debug( 'BigData Hadoop V.1 result OK' )
         return S_OK( resulting.group( 0 ).rstrip() )
     self.log.warn( '===========> SSH BigData Hadoop V.1 result NOT OK' )
     self.log.debug( result )
     return S_ERROR( result )
   else:
     self.log.debug( 'BigData Hadoop V.1 result OK' )

   for getit in result['Value']:
     resulting = re.search( "job_(\d+)_(\d+)", str( getit ) )
     if ( resulting != None ):
       return S_OK( resulting.group( 0 ).rstrip() )
   return S_ERROR( result['Value'] )

  def delHadoopData( self, tempPath ):
    cmdSeq = "hadoop dfs -rmr " + tempPath
    #cmdSeq = "hadoop dfs -ls " + tempPath
    return self.sshConnect.sshCallByPort( 100, cmdSeq )

  def delData( self, tempPath ):
    cmdSeq = "rm -Rf " + tempPath
    return self.sshConnect.sshCallByPort( 100, cmdSeq )

  def dataCopy( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCallByPort( 100, tempPath, tmpSandBoxDir )

  def getdata( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCallByPort( 100, tempPath, tmpSandBoxDir, False )

  def jobStatus( self, jobId, user, host ):

    cmdSeq = "ssh -p " + str( self.port ) + " -l " + user + " " + host + " 'hadoop job -list all | awk -v job_id=" + jobId.strip() + " "\
            " '\"'\"'BEGIN{OFS=\"\\t\"; FS=\"\\t\"; final_state=\"Unknown\"}" \
            "$0 == \"States are:\" {getline; for(i=1;i<=NF;i++) { split($i,s,\" \"); states[s[3]] = s[1] }} $1==job_id { final_state=states[$2]; exit} END{print final_state}'\"'\""

    gLogger.info( 'Command Submitted: ', cmdSeq )
    return self.sshConnect.sshOnlyCall( 10, cmdSeq )

  def newJob( self, path, jobDiracId, bdJobId ):

    cmdSeq = "ssh -p " + str( self.port ) + " -l " + self.user + " " + self.publicIP + " /" + path + "/" + str( jobDiracId ) + "/BigDat_*_getInfo.py -c step1 | wc -l"
    gLogger.info( 'Command Submitted: ', cmdSeq )
    returned = self.sshConnect.sshOnlyCall( 10, cmdSeq )
    gLogger.info( 'Command Submitted: ', returned )
    if returned != None:
      if ( returned['Value'][1] != "" ):
        if ( returned['Value'][1] > 1 ):
          cmdSeq = "ssh -p " + str( self.port ) + " -l " + self.user + " " + self.publicIP + " /" + path + "/" + str( jobDiracId ) + "/BigDat_*_getInfo.py -c step1 | tail -n1"
          gLogger.info( 'Command Submitted: ', cmdSeq )
          returned = self.sshConnect.sshOnlyCall( 10, cmdSeq )
          gLogger.info( 'Command Submitted: ', returned )
          if returned != None:
            if ( returned['Value'][1] != "" ):
              resulting = re.search( "job_+([^:]+)", returned['Value'][1] ).group( 0 ).rstrip()
              if ( bdJobId == resulting ):
                gLogger.info( 'The BD job id is the same' )
                return S_ERROR( "Same JobID" )
              else:
                gLogger.info( 'The new BD job id is: ', resulting )
                return S_OK( resulting )
    return S_ERROR( returned )

  def jobCompleteStatus( self, jobId ):

    cmdSeq = "hadoop job -status " + jobId

    gLogger.info( 'Command Submitted: ', cmdSeq )
    return self.sshConnect.sshCallByPort( 100, cmdSeq )

#################################################################################################################
  def submitPilotJob( self, tempPath, jobXMLName, proxy ):
    executableFile = self.__getExecutable( proxy, tempPath )
    self.log.verbose( "Executable file path: %s" % executableFile )

    if executableFile['OK']:
      executableFile = executableFile['Value']
    else:
      S_ERROR( executableFile )

    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    self.log.verbose( "Copy file " )
    self.dataCopy( tempPath, "/tmp" )

    executableFile = "sh -c " + executableFile + " $*  > " + tempPath + "/3J5jVr.out 2> " + tempPath + "/3J5jVr.err"
    self.log.verbose( "Executable file command: ", executableFile )
    result = self.sshConnect.sshCallByPort( 100, executableFile )

    if not result['OK']:
      self.log.warn( '===========> SSH BigData Hadoop V.1 result NOT OK' )
      self.log.debug( result )
      return S_ERROR( result )
    else:
      self.log.debug( 'BigData Hadoop V.1 result OK' )
    return S_OK( result )

  def __getExecutable( self, proxy, tempPath ):
    #######################################cambiar    

    self.queueDict[114] = {}
    self.queueDict[114]['ParametersDict'] = {}
    self.queueDict[114]['ParametersDict']['CPUTime'] = '1600'
    self.queueDict[114]['CEName'] = 'CesgaHadoop'
    self.queueDict[114]['ParametersDict']['Site'] = 'CESGA'
    #######################################
    pilotOptions = self.__getPilotOptions( 114, 1 )
    if pilotOptions is None:
      return S_ERROR( 'Errors in compiling pilot options' )
    executable = self.__writePilotScript( tempPath, pilotOptions, proxy, "", tempPath )
    result = S_OK( executable )
    return result

  def __writePilotScript( self, workingDirectory, pilotOptions, proxy = '', httpProxy = '', pilotExecDir = '' ):
    """ Bundle together and write out the pilot executable script, admixt the proxy if given
    """

    try:
      compressedAndEncodedProxy = ''
      proxyFlag = 'False'
      if proxy:
        compressedAndEncodedProxy = base64.encodestring( bz2.compress( proxy.dumpAllToString()['Value'] ) )
        proxyFlag = 'True'
      compressedAndEncodedPilot = base64.encodestring( bz2.compress( open( self.pilot, "rb" ).read(), 9 ) )
      compressedAndEncodedInstall = base64.encodestring( bz2.compress( open( self.install, "rb" ).read(), 9 ) )
    except:
      self.log.exception( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )
      return S_ERROR( 'Exception during file compression of proxy, dirac-pilot or dirac-install' )

    localPilot = """#!/bin/bash
/usr/bin/env python << EOF
#
import os, tempfile, sys, shutil, base64, bz2
try:
  pilotExecDir = '%(pilotExecDir)s'
  if not pilotExecDir:
    pilotExecDir = None
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix = 'DIRAC_', dir = pilotExecDir )
  pilotWorkingDirectory = os.path.realpath( pilotWorkingDirectory )
  os.chdir( pilotWorkingDirectory )
  if %(proxyFlag)s:
    open( 'proxy', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedProxy)s\"\"\" ) ) )
    os.chmod("proxy",0600)
    os.environ["X509_USER_PROXY"]=os.path.join(pilotWorkingDirectory, 'proxy')
  open( '%(pilotScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedPilot)s\"\"\" ) ) )
  open( '%(installScript)s', "w" ).write(bz2.decompress( base64.decodestring( \"\"\"%(compressedAndEncodedInstall)s\"\"\" ) ) )
  os.chmod("%(pilotScript)s",0700)
  os.chmod("%(installScript)s",0700)
  if "LD_LIBRARY_PATH" not in os.environ:
    os.environ["LD_LIBRARY_PATH"]=""
  if "%(httpProxy)s":
    os.environ["HTTP_PROXY"]="%(httpProxy)s"
  os.environ["X509_CERT_DIR"]=os.path.join(pilotWorkingDirectory, 'etc/grid-security/certificates')
  # TODO: structure the output
  print '==========================================================='
  print 'Environment of execution host'
  for key in os.environ.keys():
    print key + '=' + os.environ[key]
  print '==========================================================='
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "python %(pilotScript)s %(pilotOptions)s"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( pilotWorkingDirectory )

EOF
""" % { 'compressedAndEncodedProxy': compressedAndEncodedProxy,
        'compressedAndEncodedPilot': compressedAndEncodedPilot,
        'compressedAndEncodedInstall': compressedAndEncodedInstall,
        'httpProxy': httpProxy,
        'pilotExecDir': pilotExecDir,
        'pilotScript': os.path.basename( self.pilot ),
        'installScript': os.path.basename( self.install ),
        'pilotOptions': ' '.join( pilotOptions ),
        'proxyFlag': proxyFlag }

    fd, name = tempfile.mkstemp( suffix = '_pilotwrapper.py', prefix = 'DIRAC_', dir = workingDirectory )
    pilotWrapper = os.fdopen( fd, 'w' )
    pilotWrapper.write( localPilot )
    pilotWrapper.close()
    return name

  def updatePilotStatus( self ):
    """ Update status of pilots in transient states
    """
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']
      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']

      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'Status':TRANSIENT_PILOT_STATUS} )
      if not result['OK']:
        self.log.error( 'Failed to select pilots: %s' % result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue

      #print "AT >>> pilotRefs", pilotRefs

      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
        continue
      pilotDict = result['Value']

      #print "AT >>> pilotDict", pilotDict

      stampedPilotRefs = []
      for pRef in pilotDict:
        if pilotDict[pRef]['PilotStamp']:
          stampedPilotRefs.append( pRef + ":::" + pilotDict[pRef]['PilotStamp'] )
        else:
          stampedPilotRefs = list( pilotRefs )
          break

      result = ce.getJobStatus( stampedPilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots status from CE: %s' % result['Message'] )
        continue
      pilotCEDict = result['Value']

      #print "AT >>> pilotCEDict", pilotCEDict

      for pRef in pilotRefs:
        newStatus = ''
        oldStatus = pilotDict[pRef]['Status']
        ceStatus = pilotCEDict[pRef]
        if oldStatus == ceStatus:
          # Status did not change, continue
          continue
        elif ceStatus == "Unknown" and not oldStatus in FINAL_PILOT_STATUS:
          # Pilot finished without reporting, consider it Aborted
          newStatus = 'Aborted'
        elif ceStatus != 'Unknown' :
          # Update the pilot status to the new value
          newStatus = ceStatus

        if newStatus:
          self.log.info( 'Updating status to %s for pilot %s' % ( newStatus, pRef ) )
          result = pilotAgentsDB.setPilotStatus( pRef, newStatus, '', 'Updated by SiteDirector' )
        # Retrieve the pilot output now
        if newStatus in FINAL_PILOT_STATUS:
          if pilotDict[pRef]['OutputReady'].lower() == 'false' and self.getOutput:
            self.log.info( 'Retrieving output for pilot %s' % pRef )
            pilotStamp = pilotDict[pRef]['PilotStamp']
            pRefStamp = pRef
            if pilotStamp:
              pRefStamp = pRef + ':::' + pilotStamp
            result = ce.getJobOutput( pRefStamp )
            if not result['OK']:
              self.log.error( 'Failed to get pilot output: %s' % result['Message'] )
            else:
              output, error = result['Value']
              result = pilotAgentsDB.storePilotOutput( pRef, output, error )
              if not result['OK']:
                self.log.error( 'Failed to store pilot output: %s' % result['Message'] )

    # The pilot can be in Done state set by the job agent check if the output is retrieved
    for queue in self.queueDict:
      ce = self.queueDict[queue]['CE']

      if not ce.isProxyValid( 120 ):
        result = gProxyManager.getPilotProxyFromDIRACGroup( self.genericPilotDN, self.genericPilotGroup, 1000 )
        if not result['OK']:
          return result
        ce.setProxy( self.proxy, 940 )

      ceName = self.queueDict[queue]['CEName']
      queueName = self.queueDict[queue]['QueueName']
      ceType = self.queueDict[queue]['CEType']
      siteName = self.queueDict[queue]['Site']
      result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                           'Queue':queueName,
                                           'GridType':ceType,
                                           'GridSite':siteName,
                                           'OutputReady':'False',
                                           'Status':FINAL_PILOT_STATUS} )

      if not result['OK']:
        self.log.error( 'Failed to select pilots: %s' % result['Message'] )
        continue
      pilotRefs = result['Value']
      if not pilotRefs:
        continue
      result = pilotAgentsDB.getPilotInfo( pilotRefs )
      if not result['OK']:
        self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
        continue
      pilotDict = result['Value']
      if self.getOutput:
        for pRef in pilotRefs:
          self.log.info( 'Retrieving output for pilot %s' % pRef )
          pilotStamp = pilotDict[pRef]['PilotStamp']
          pRefStamp = pRef
          if pilotStamp:
            pRefStamp = pRef + ':::' + pilotStamp
          result = ce.getJobOutput( pRefStamp )
          if not result['OK']:
            self.log.error( 'Failed to get pilot output: %s' % result['Message'] )
          else:
            output, error = result['Value']
            result = pilotAgentsDB.storePilotOutput( pRef, output, error )
            if not result['OK']:
              self.log.error( 'Failed to store pilot output: %s' % result['Message'] )

      # Check if the accounting is to be sent
      if self.sendAccounting:
        result = pilotAgentsDB.selectPilots( {'DestinationSite':ceName,
                                             'Queue':queueName,
                                             'GridType':ceType,
                                             'GridSite':siteName,
                                             'AccountingSent':'False',
                                             'Status':FINAL_PILOT_STATUS} )

        if not result['OK']:
          self.log.error( 'Failed to select pilots: %s' % result['Message'] )
          continue
        pilotRefs = result['Value']
        if not pilotRefs:
          continue
        result = pilotAgentsDB.getPilotInfo( pilotRefs )
        if not result['OK']:
          self.log.error( 'Failed to get pilots info: %s' % result['Message'] )
          continue
        pilotDict = result['Value']
        result = self.sendPilotAccounting( pilotDict )
        if not result['OK']:
          self.log.error( 'Failed to send pilot agent accounting' )

    return S_OK()

  def __getPilotOptions( self, queue, pilotsToSubmit ):
    """ Prepare pilot options
    """

    queueDict = self.queueDict[queue]['ParametersDict']
    pilotOptions = []

    setup = gConfig.getValue( "/DIRAC/Setup", "unknown" )
    if setup == 'unknown':
      self.log.error( 'Setup is not defined in the configuration' )
      return None
    pilotOptions.append( '-S %s' % setup )
    opsHelper = Operations.Operations( group = self.genericPilotGroup, setup = setup )

    #Installation defined?
    installationName = opsHelper.getValue( "Pilot/Installation", "" )
    if installationName:
      pilotOptions.append( '-V %s' % installationName )

    #Project defined?
    projectName = opsHelper.getValue( "Pilot/Project", "" )
    if projectName:
      pilotOptions.append( '-l %s' % projectName )
    else:
      self.log.info( 'DIRAC project will be installed by pilots' )

    #Request a release
    diracVersion = opsHelper.getValue( "Pilot/Version", [] )
    #####borrar
    diracVersion = "v6r4"
    if not diracVersion:
      self.log.error( 'Pilot/Version is not defined in the configuration' )
      return None
    #diracVersion is a list of accepted releases. Just take the first one
    pilotOptions.append( '-r %s' % diracVersion )

    ownerDN = self.genericPilotDN
    ownerGroup = self.genericPilotGroup
    result = gProxyManager.requestToken( ownerDN, ownerGroup, pilotsToSubmit * 5 )
    if not result[ 'OK' ]:
      self.log.error( ERROR_TOKEN, result['Message'] )
      return S_ERROR( ERROR_TOKEN )
    ( token, numberOfUses ) = result[ 'Value' ]
    pilotOptions.append( '-o /Security/ProxyToken=%s' % token )
    # Use Filling mode
    pilotOptions.append( '-M %s' % 5 )

    # Debug
    if self.pilotLogLevel.lower() == 'debug':
      pilotOptions.append( '-d' )
    # CS Servers
    csServers = gConfig.getValue( "/DIRAC/Configuration/Servers", [] )
    pilotOptions.append( '-C %s' % ",".join( csServers ) )
    # DIRAC Extensions
   # extensionsList = CSGlobals.getCSExtensions()
   # if extensionsList:
   #   pilotOptions.append( '-e %s' % ",".join( extensionsList ) )
    # Requested CPU time
    pilotOptions.append( '-T %s' % queueDict['CPUTime'] )
    # CEName
    pilotOptions.append( '-N %s' % self.queueDict[queue]['CEName'] )
    # SiteName
    pilotOptions.append( '-n %s' % queueDict['Site'] )
    if 'ClientPlatform' in queueDict:
      pilotOptions.append( "-p '%s'" % queueDict['ClientPlatform'] )

    if 'SharedArea' in queueDict:
      pilotOptions.append( "-o '/LocalSite/SharedArea=%s'" % queueDict['SharedArea'] )

    group = "lhcb_pilot"
    if group:
      pilotOptions.append( '-G %s' % group )

    self.log.verbose( "pilotOptions: ", ' '.join( pilotOptions ) )

    return pilotOptions

  def sendPilotAccounting( self, pilotDict ):
    """ Send pilot accounting record
    """
    for pRef in pilotDict:
      self.log.verbose( 'Preparing accounting record for pilot %s' % pRef )
      pA = PilotAccounting()
      pA.setEndTime( pilotDict[pRef][ 'LastUpdateTime' ] )
      pA.setStartTime( pilotDict[pRef][ 'SubmissionTime' ] )
      retVal = CS.getUsernameForDN( pilotDict[pRef][ 'OwnerDN' ] )
      if not retVal[ 'OK' ]:
        userName = 'unknown'
        self.log.error( "Can't determine username for dn:", pilotDict[pRef][ 'OwnerDN' ] )
      else:
        userName = retVal[ 'Value' ]
      pA.setValueByKey( 'User', userName )
      pA.setValueByKey( 'UserGroup', pilotDict[pRef][ 'OwnerGroup' ] )
      result = getSiteForCE( pilotDict[pRef][ 'DestinationSite' ] )
      if result['OK'] and result[ 'Value' ].strip():
        pA.setValueByKey( 'Site', result['Value'].strip() )
      else:
        pA.setValueByKey( 'Site', 'Unknown' )
      pA.setValueByKey( 'GridCE', pilotDict[pRef][ 'DestinationSite' ] )
      pA.setValueByKey( 'GridMiddleware', pilotDict[pRef][ 'GridType' ] )
      pA.setValueByKey( 'GridResourceBroker', pilotDict[pRef][ 'Broker' ] )
      pA.setValueByKey( 'GridStatus', pilotDict[pRef][ 'Status' ] )
      if not 'Jobs' in pilotDict[pRef]:
        pA.setValueByKey( 'Jobs', 0 )
      else:
        pA.setValueByKey( 'Jobs', len( pilotDict[pRef]['Jobs'] ) )
      self.log.info( "Adding accounting record for pilot %s" % pilotDict[pRef][ 'PilotID' ] )
      retVal = gDataStoreClient.addRegister( pA )
      if not retVal[ 'OK' ]:
        self.log.error( 'Failed to send accounting info for pilot %s' % pRef )
      else:
        # Set up AccountingSent flag
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot %s' % pRef )

    self.log.info( 'Committing accounting records for %d pilots' % len( pilotDict ) )
    result = gDataStoreClient.commit()
    if result['OK']:
      for pRef in pilotDict:
        self.log.verbose( 'Setting AccountingSent flag for pilot %s' % pRef )
        result = pilotAgentsDB.setAccountingFlag( pRef )
        if not result['OK']:
          self.log.error( 'Failed to set accounting flag for pilot %s' % pRef )
    else:
      return result

    return S_OK()
