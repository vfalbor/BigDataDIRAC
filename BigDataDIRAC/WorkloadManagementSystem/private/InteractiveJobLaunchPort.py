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

class InteractiveJobLaunchPortThread ( threading.Thread ):
    def __init__( self, user , publicIP , jobname, Port ):
        threading.Thread.__init__( self )
        self.jobname = jobname
        self.sshConnect = ConnectionUtils( user , publicIP, Port )

    def run( self ):
        self.log = gLogger.getSubLogger( "InteractiveJobLaunchThreadPort" )
        self.log.warn( ':::::::::::::InteractiveJobLaunchPortThread2insiderun', )
        self.jobLaunch( self.jobname )

    def jobLaunch( self, jobname ):
        result = self.sshConnect.sshCallByPort( 84600, jobname )
        self.log.warn( ':::::::::::::InteractiveJobLaunchPortThread3', result )

