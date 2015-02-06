########################################################################
# $HeadURL$
# File :   MoveToAccounting.py
# Author : Victor Fernandez
########################################################################
"""
  This python class move the data from BigDataDB to AccountingJobDB
"""

__RCSID__ = "$Id: $"

from DIRAC.AccountingSystem.Client.Types.Job  import Job as AccountingJob
from DIRAC.Core.Utilities.File                import getGlobbedTotalSize, getGlobbedFiles

class MoveToAccounting:

  #############################################################################
  def __init__( self ):
    """ Standard constructor
    """
accountingReport = AccountingJob()
accountingReport.setStartTime()

jobID = 489
status = "Done"
minorStatus = "Completed"
if status:
  wmsMajorStatus = status
if minorStatus:
  wmsMinorStatus = minorStatus

accountingReport.setEndTime()
#CPUTime and ExecTime
if not 'CPU' in EXECUTION_RESULT:
  # If the payload has not started execution (error with input data, SW, SB,...)
  # Execution result is not filled use initialTiming
  log.info( 'EXECUTION_RESULT[CPU] missing in sendWMSAccounting' )
  finalStat = os.times()
  EXECUTION_RESULT['CPU'] = []
  for i in range( len( finalStat ) ):
    EXECUTION_RESULT['CPU'].append( finalStat[i] - initialTiming[i] )

log.info( 'EXECUTION_RESULT[CPU] in sendWMSAccounting', str( EXECUTION_RESULT['CPU'] ) )

utime, stime, cutime, cstime, elapsed = EXECUTION_RESULT['CPU']
cpuTime = utime + stime + cutime + cstime
execTime = elapsed
#diskSpaceConsumed = getGlobbedTotalSize( os.path.join( root, str( jobID ) ) )
#Fill the data
                acData = {
                           'User' : getting['Owner'],
                           'UserGroup' : getting['OwnerGroup'],
                           'JobGroup' : 'cesga',
                           'JobType' : 'User',
                           'JobClass' : 'unknown',
                           'ProcessingType' : 'unknown',
                           'FinalMajorStatus' : getting['Status'],
                           'FinalMinorStatus' : getting['MinorStatus'],
                           'CPUTime' : cpuTime,
                           'Site' : getting['Site'],
                           # Based on the factor to convert raw CPU to Normalized units (based on the CPU Model)
                           'NormCPUTime' : cpuTime * 0.0,
                           'ExecTime' : execTime,
                           'InputDataSize' : 0,
                           'OutputDataSize' : 0,
                           'InputDataFiles' : 0,
                           'OutputDataFiles' : 0,
                           'DiskSpace' : 0,
                           'InputSandBoxSize' : 0,
                           'OutputSandBoxSize' : 0,
                           'ProcessedEvents' : 0
                         }
log.verbose( 'Accounting Report is:' )
log.verbose( acData )
accountingReport.setValuesFromDict( acData )
result = accountingReport.commit()
# Even if it fails a failover request will be created
wmsAccountingSent = True
