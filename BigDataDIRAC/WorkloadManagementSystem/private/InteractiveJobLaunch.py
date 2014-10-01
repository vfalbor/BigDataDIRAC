########################################################################
# $HeadURL$
# File :   HadoopV1Client.py
# Author : Victor Fernandez ( victormanuel.fernandez@usc.es )
########################################################################

import random, time, re, os, glob, shutil, sys, base64, bz2, tempfile, stat, string, thread

from BigDataDIRAC.WorkloadManagementSystem.private.ConnectionUtils import ConnectionUtils

# DIRAC
from DIRAC                                                    import S_OK, S_ERROR
from DIRAC                                                    import gConfig, gLogger

import threading
import time

exitFlag = 0

class InteractiveJobLaunchThread ( threading.Thread ):
    def __init__( self, user , publicIP , jobname ):
        threading.Thread.__init__( self )
        self.jobname = jobname
        self.sshConnect = ConnectionUtils( user , publicIP )

    def run( self ):
        self.log = gLogger.getSubLogger( "InteractiveJobLaunchThread" )
        self.jobLaunch( self.jobname )

    def jobLaunch( self, jobname ):
        self.sshConnect.sshCall( 84600, jobname )

