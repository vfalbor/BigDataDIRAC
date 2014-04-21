########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils
# DIRAC
from DIRAC import gLogger

__RCSID__ = '$Id: $'
SOURCE_ENCODING = "iso-8859-1"

# Classes
###################
class HadoopV1Client:

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
    cmdSeq = "ssh -l " + user + " " + host + " 'hadoop job -list all | awk -v job_id=" + jobId.strip() + " "\
        " '\"'\"'BEGIN{OFS=\"\t\"; FS=\"\t\"; final_state=\"Unknown\"} "\
        "$0 == \"States are:\" {getline; for(i=1;i<=NF;i++) { split($i,s,\" \"); states[s[3]] = s[1] }} $1==job_id { final_state=states[$2]; exit} END{print final_state}'\"'\""

    gLogger.info( 'Command Submitted: ', cmdSeq )
    return self.sshConnect.sshOnlyCall( 10, cmdSeq )
