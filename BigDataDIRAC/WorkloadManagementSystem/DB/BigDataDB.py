########################################################################
# $HeadURL$
# File :   BigDataDB.py
# Author : Victor Fernandez
########################################################################
""" BigDataDB class is a front-end to the big data softwares DB

  Life cycle of VMs Images in DB
  - New:       Inserted by Director (Name - Requirements - Status = New ) if not existing when launching a new Job
  - Validated: Declared by VMMonitoring Server when an Job reports back correctly
  - Error:     Declared by VMMonitoring Server when an Job reports back wrong requirements

  Life cycle of VMs Instances in DB
  - New:       Inserted by Director before launching a new Job, to check if image is valid
  - Submitted: Inserted by Director (adding UniqueID) when launches a new Job
  - Wait_ssh_context:     Declared by Director for submitted Job wich need later contextualization using ssh (VirtualMachineContextualization will check)
  - Contextualizing:     on the waith_ssh_context path is the next status before Running
  - Running:   Declared by VMMonitoring Server when an Job reports back correctly (add LastUpdate, publicIP and privateIP)
  - Stopping:  Declared by VMManager Server when an Job has been deleted outside of the VM (f.e "Delete" button on Browse Instances)
  - Halted:    Declared by VMMonitoring Server when an Job reports halting
  - Stalled:   Declared by VMManager Server when detects Job no more running
  - Error:     Declared by VMMonitoring Server when an Job reports back wrong requirements or reports as running when Halted

"""

import types

# DIRAC
from DIRAC                import gConfig, S_ERROR, S_OK
from DIRAC.Core.Base.DB   import DB
from DIRAC.Core.Utilities import DEncode, Time

class BigDataDB( DB ):

  validJobStates = [ 'Submitted', 'Mapping', 'Reduccing',
                          'Queue', 'Done', 'Stalled', 'Error' ]

  validSoftware = ['hdv1,hdv2,twister']

  # In seconds !
  stallingInterval = 30 * 60

  tablesDesc = {}

  tablesDesc[ 'bd_Jobs' ] = { 'Fields' : { 'BdJobID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'Name' : 'VARCHAR(32) NOT NULL',
                                          'NameNode' : 'VARCHAR(32) NOT NULL',
                                          'SiteName' : 'VARCHAR(32) NOT NULL',
                                          'Status' : 'VARCHAR(32) NOT NULL',
                                          'LastUpdate' : 'DATETIME',
                                          'PublicIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                          'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdSoftware' : 'INT NOT NULL',
                                          'BdInputData' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdOutputData' : 'VARCHAR(255) NOT NULL DEFAULT ""'
                                             },
                                   'PrimaryKey' : 'BdJobID',
                                   'Indexes': { 'Status': [ 'Status' ] },
                                 }

  tablesDesc[ 'bd_Software' ] = { 'Fields' : { 'BdSoftID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'Name' : 'VARCHAR(32) NOT NULL',
                                          'Version' : 'VARCHAR(32) NOT NULL',
                                          'HLevelLang' : 'INT NOT NULL'
                                             },
                                   'PrimaryKey' : 'BdSoftID'
                                 }

  tablesDesc[ 'bd_HLevelLang' ] = { 'Fields' : { 'BdHLevelID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'Name' : 'VARCHAR(32) NOT NULL',
                                          'Version' : 'VARCHAR(32) NOT NULL'
                                             },
                                   'PrimaryKey' : 'BdHLevelID'
                                 }


  tablesDesc[ 'temp_DataSetCatalog' ] = { 'Fields' : { 'BdLFNID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'LFN' : 'VARCHAR(32) NOT NULL',
                                          'SiteName' : 'VARCHAR(32) NOT NULL',
                                             },
                                   'PrimaryKey' : 'BdLFNID'
                                 }

  def __init__( self, maxQueueSize = 10 ):

    DB.__init__( self, 'BigDataDB', 'WorkloadManagement/BigDataDB', maxQueueSize )
    if not self._MySQL__initialized:
      raise Exception( 'Can not connect to BigDataDB, exiting...' )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( 'Can\'t create tables: %s' % result[ 'Message' ] )

  def __getTypeTuple( self, element ):
    """
    return tuple of (tableName, validStates, idName) for object
    """
    # defaults
    tableName, validStates, idName = '', [], ''

    if element == 'job':
      tableName = 'bd_Jobs'
      validStates = self.validJobStates
      idName = 'BdJobID'
    elif element == 'software':
      tableName = 'bd_Software'
      validStates = self.validSoftware
      idName = 'BdSoftID'

    return ( tableName, validStates, idName )

  def getBigDataJobsByStatus( self, status ):
    """
    Get dictionary of Job Names with BdJobID in given status 
    """
    if status not in self.validInstanceStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    runningJobs = self._getFields( tableName, [ 'BdJobID', 'UniqueID' ], ['Status'], [status] )
    if not runningJobs[ 'OK' ]:
      return runningJobs
    runningJobs = runningJobs[ 'Value' ]

    jobsDict = {}

    jobsDict = runningJobs[ 'Value' ][ 0 ][ 0 ]

    return S_OK( jobsDict )

  def getBigDataJobsByStatusAndEndpoint( self, status, endpoint ):
    """
    Get dictionary of Job Names with BdJobID in given status 
    """
    if status not in self.validInstanceStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    runningJobs = self._getFields( tableName, [ 'VMImageID', 'UniqueID' ],
                                        [ 'Status', 'Endpoint' ], [ status, endpoint ] )
    if not runningJobs[ 'OK' ]:
      return runningJobs
    runningJobs = runningJobs[ 'Value' ]

    jobsDict = {}

    jobsDict = runningJobs[ 'Value' ][ 0 ][ 0 ]

    return S_OK( jobsDict )


  def __initializeDB( self ):
    """
    Create the tables
    """
    tables = self._query( "show tables" )
    if not tables[ 'OK' ]:
      return tables

    tablesInDB = [ table[0] for table in tables[ 'Value' ] ]

    tablesToCreate = {}
    for tableName in self.tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def declareStalledJobs( self ):
     """
     Check last Heart Beat for all Running jobs and declare them Stalled if older than interval
     """
     oldJobs = self.__getOldJobIDs( self.stallingInterval, 'Stalled' )
     if not oldJobs[ 'OK' ]:
       return oldJobs

     stallingJobs = []

     if not oldJobs[ 'Value' ]:
       return S_OK( stallingJobs )

     for jobID in oldJobs['Value']:
       jobID = jobID[ 0 ]
       stalled = self.__setState( 'Job', jobID, 'Stalled' )
       if not stalled[ 'OK' ]:
         continue

       stallingJobs.append( jobID )

     return S_OK( stallingJobs )

  def __getOldJobIDs( self, secondsIdle, states ):
     """
     Return list of jobs IDs that have not updated after the given time stamp
     they are required to be in one of the given states
     """
     tableName, _validStates, idName = self.__getTypeTuple( 'job' )

     sqlCond = []
     sqlCond.append( 'TIMESTAMPDIFF( SECOND, `LastUpdate`, UTC_TIMESTAMP() ) > % d' % secondsIdle )
     sqlCond.append( 'Status IN ( "%s" )' % '", "'.join( states ) )

     sqlSelect = 'SELECT %s from `%s` WHERE %s' % ( idName, tableName, " AND ".join( sqlCond ) )

     return self._query( sqlSelect )

