########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################
import random, time, re
from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils
# DIRAC
from DIRAC import gLogger
import DIRAC

__RCSID__ = '$Id: $'
SOURCE_ENCODING = "iso-8859-1"

# Classes
###################
class HadoopV2Client:

  def __init__( self, User, PublicIP ):
    self.user = User
    self.publicIP = PublicIP
    self.sshConnect = ConnectionUtils( self.user , self.publicIP )

  def jobSubmit( self, tempPath, jobXMLName ):
    cmdSeq = "hadoop job -submit " + tempPath + "/" + jobXMLName
    return self.sshConnect.sshCall( 100, cmdSeq )

  def dataCopy( self, tempPath, tmpSandBoxDir ):
    return self.sshConnect.scpCall( 100, tempPath, tmpSandBoxDir )

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
