# $HeadURL$
""" BigDataJobHandler provides remote access to BigDataDB

    The following methods are available in the Service interface:    

"""

from types import DictType, FloatType, IntType, ListType, LongType, StringType, TupleType, UnicodeType

# DIRAC
from DIRAC                                import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Core.DISET.RequestHandler      import RequestHandler
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

# DIRACBigData
from BigDataDIRAC.WorkloadManagementSystem.DB.BigDataDB import BigDataDB

__RCSID__ = '$Id: $'

# This is a global instance of the BigDataDB class
gBigDataDB = False

def initializeBigDataJobManagerHandler( _serviceInfo ):

  global gBigDataDB

  gBigDataDB = BigDataDB()
  gBigDataDB.declareStalledJobs()

  if gBigDataDB._connected:
    gThreadScheduler.addPeriodicTask( 60 * 30, gBigDataDB.declareStalledJobs )
    return S_OK()

  return S_ERROR()

class BigDataJobManagerHandler( RequestHandler ):

  def initialize( self ):

     credDict = self.getRemoteCredentials()
     self.rpcProperties = credDict[ 'properties' ]

  @staticmethod
  def __logResult( methodName, result ):
    '''
    Method that writes to log error messages 
    '''
    if not result[ 'OK' ]:
      gLogger.error( '%s%s' % ( methodName, result[ 'Message' ] ) )
#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
