########################################################################
# $HeadURL$
# File :   BigDataDB.py
# Author : Victor Fernandez
########################################################################
""" BigDataDB class is a front-end to the big data softwares DB
"""

import types

# DIRAC
from DIRAC                import gConfig, S_ERROR, S_OK
from DIRAC.Core.Base.DB   import DB
from DIRAC.Core.Utilities import DEncode, Time

from DIRAC.WorkloadManagementSystem.Client.ServerUtils        import jobDB
from DIRAC.Core.DISET.RPCClient                               import RPCClient

class BigDataDB( DB ):

  validJobStates = [ 'Submitted', 'Running', 'Done', 'Stalled', 'Error' ]

  validSoftware = ['hadoop', 'twister']
  validSoftwareVersion = ['hdv1', 'hdv2', 'tw1']

  validHighLevelLang = ['pig', 'hive', 'none']
  validHighLevelLangVersion = ['1']

  # In seconds !
  stallingInterval = 30 * 60

  tablesDesc = {}

  tablesDesc[ 'BD_Jobs' ] = { 'Fields' : { 'BdJobID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'BdJobName' : 'VARCHAR(255) NOT NULL',
                                          'BdJobStatus' : 'VARCHAR(32) NOT NULL',
                                          'BdJobLastUpdate' : 'DATETIME',
                                          'NameNode' : 'VARCHAR(255) NOT NULL',
                                          'SiteName' : 'VARCHAR(32) NOT NULL',
                                          'PublicIP' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                          'ErrorMessage' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdInputData' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdOutputData' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdSoftName' : 'VARCHAR(255) NOT NULL',
                                          'BdSoftVersion' : 'VARCHAR(32) NOT NULL',
                                          'BdSoftHighLevelLang' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'BdSoftHighLevelLangVersion' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                          'BdJobSoftwareID' : 'VARCHAR(255) NOT NULL DEFAULT ""'
                                          },
                                   'PrimaryKey' : 'BdJobID',
                                   'Indexes': { 'BdJobStatus': [ 'BdJobStatus' ] },
                                 }

  tablesDesc[ 'BD_History' ] = { 'Fields' : { 'His_ID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                          'His_BdJobID' : 'VARCHAR(32) NOT NULL',
                                          'His_JobName' : 'VARCHAR(255) NOT NULL',
                                          'His_JobStatus' : 'VARCHAR(32) NOT NULL',
                                          'His_Update' : 'DATETIME',
                                          'His_NameNode' : 'VARCHAR(255) NOT NULL',
                                          'His_SiteName' : 'VARCHAR(32) NOT NULL',
                                          'His_BdSoftName' : 'VARCHAR(255) NOT NULL',
                                          'His_BdSoftVersion' : 'VARCHAR(32) NOT NULL',
                                          'His_BdSoftHighLevelLang' : 'VARCHAR(255) NOT NULL DEFAULT ""',
                                          'His_BdSoftHighLevelLangVersion' : 'VARCHAR(32) NOT NULL DEFAULT ""',
                                          'His_BdJobSoftwareID' : 'VARCHAR(255) NOT NULL DEFAULT ""'
                                             },
                                   'PrimaryKey' : 'His_ID',
                                   'Indexes': { 'His_JobStatus': [ 'His_JobStatus' ] },
                                 }

  tablesDesc[ 'temp_BD_DataSetCatalog' ] = { 'Fields' : { 'BdLFNID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
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
      tableName = 'BD_Jobs'
      validStates = self.validJobStates
      idName = 'BdJobID'

    if element == 'dataset':
      tableName = 'temp_BD_DataSetCatalog'
      idName = 'BdLFNID'

    return ( tableName, validStates, idName )


  def getBigDataJobsByStatus( self, status ):
    """
    Get dictionary of Job Names with BdJobID in given status 
    """
    if status not in self.validJobStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    runningJobs = self._getFields( tableName, [ 'BdJobID' ],
                                        [ 'BdJobStatus' ],
                                        [ status ] )
    if not runningJobs[ 'OK' ]:
      return runningJobs

    jobsDict = {}

    jobsDict = runningJobs[ 'Value' ]

    return S_OK( jobsDict )

  def getBigDataJobsByStatusAndEndpoint( self, status, endpoint ):
    """
    Get dictionary of Job Names with BdJobID in given status 
    """
    if status not in self.validJobStates:
      return S_ERROR( 'Status %s is not known' % status )

    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    runningJobs = self._getFields( tableName, [ 'BdJobID' ],
                                        [ 'BdJobStatus', 'NameNode' ],
                                        [ status, endpoint ] )
    if not runningJobs[ 'OK' ]:
      return runningJobs

    jobsDict = {}

    jobsDict = runningJobs[ 'Value' ]

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

  def getDataSetBySitename( self, sitename ):
    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'dataset' )

    dataset = self._getFields( tableName, [ 'BdLFNID', 'LFN' ],
                                        [ 'SiteName' ], [ sitename ] )
    if not dataset[ 'OK' ]:
      return dataset
    datasetDict = {}
    datasetDict = dataset[ 'Value' ]
    return datasetDict

  def getSiteNameByDataSet( self, dataset ):
    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'dataset' )

    SiteName = self._getFields( tableName, [ 'BdLFNID', 'SiteName' ],
                                        [ 'LFN' ], [ dataset ] )
    if not SiteName[ 'OK' ]:
      return SiteName
    SiteNameDict = {}
    SiteNameDict = SiteName[ 'Value' ]
    return SiteNameDict

  def getSoftwareJobIDByJobID( self, jobID ):
    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    BdJobSoftwareID = self._getFields( tableName, [ 'BdJobSoftwareID', 'NameNode' ],
                                        [ 'BdJobID' ], [ jobID ] )
    if not BdJobSoftwareID[ 'OK' ]:
      return BdJobSoftwareID
    BdJobSoftwareIDDict = {}
    BdJobSoftwareIDDict = BdJobSoftwareID[ 'Value' ]
    return BdJobSoftwareIDDict


  def getJobIDInfo( self, jobID ):
    # InstanceTuple
    tableName, _validStates, _idName = self.__getTypeTuple( 'job' )

    BdJobSoftwareID = self._getFields( tableName, [ 'BdJobName', 'NameNode', 'SiteName',
                                                   'BdSoftName', 'BdSoftVersion', 'BdSoftHighLevelLang',
                                                    'BdSoftHighLevelLangVersion', 'BdJobSoftwareID'],
                                        [ 'BdJobID' ], [ jobID ] )
    if not BdJobSoftwareID[ 'OK' ]:
      return BdJobSoftwareID
    BdJobSoftwareIDDict = {}
    BdJobSoftwareIDDict = BdJobSoftwareID[ 'Value' ]
    return BdJobSoftwareIDDict

  def getRunningEnPointDict( self, runningEndPointName ):
    """
    Return from CS a Dictionary with getRunningEnPointDict definition
    """

    runningEndPointCSPath = '/Resources/BigDataEndPoints'

    definedRunningEndPoints = gConfig.getSections( runningEndPointCSPath )
    if not definedRunningEndPoints[ 'OK' ]:
      return S_ERROR( 'BigData section not defined' )

    runningBDCSPath = '%s/%s' % ( runningEndPointCSPath, runningEndPointName )

    runningEndPointBDDict = {}

    if not runningBDCSPath:
      return S_ERROR( 'Missing BigDataEndpoint "%s"' % runningEndPointName )

    for option, value in gConfig.getOptionsDict( runningBDCSPath )['Value'].items():
      runningEndPointBDDict[option] = value

    runningHighLevelLanguajeDict = gConfig.getOptionsDict( '%s/HighLevelLanguage' % runningBDCSPath )
    if not runningHighLevelLanguajeDict[ 'OK' ]:
      return S_ERROR( 'Missing HighLevelLang in "%s"' % runningBDCSPath )
    runningEndPointBDDict['HighLevelLanguage'] = runningHighLevelLanguajeDict['Value']

    runningReqDict = gConfig.getOptionsDict( '%s/Requirements' % runningBDCSPath )
    if not runningReqDict[ 'OK' ]:
      return S_ERROR( 'Missing Requirements in "%s"' % runningBDCSPath )
    runningEndPointBDDict['Requirements'] = runningReqDict['Value']

    return S_OK( runningEndPointBDDict )

  def setHadoopID( self, JobID, HadoopID ):
    """
    Insert HadoopID
    """
    tableName, _validStates, idName = self.__getTypeTuple( 'job' )
    sqlUpdate = 'UPDATE %s SET BdJobSoftwareID= "%s" WHERE %s = %s' % ( tableName, HadoopID, idName, JobID )
    return self._update( sqlUpdate )

  def updateHadoopIDAndJobStatus( self, JobID, HadoopID ):
    """
    Insert HadoopID
    """
    jobDB.setJobAttribute( JobID, "MinorStatus", "MapReducing Step, running with JobID: " + HadoopID )

    tableName, _validStates, idName = self.__getTypeTuple( 'job' )
    sqlUpdate = 'UPDATE %s SET BdJobSoftwareID= "%s" WHERE %s = %s' % ( tableName, HadoopID, idName, JobID )
    return self._update( sqlUpdate )

  def setJobStatus( self, JobID, status ):
    """
    Insert HadoopID
    """
    resultInfo = self.getJobIDInfo( JobID )

    tableName, _validStates, idName = self.__getTypeTuple( 'job' )
    sqlUpdate = 'UPDATE %s SET BdJobStatus= "%s" WHERE %s = %s' % ( tableName, status, idName, JobID )
    self._update( sqlUpdate )
    sqlUpdate = 'UPDATE %s SET BdJobLastUpdate= "%s" WHERE %s = %s' % ( tableName, Time.toString(), idName, JobID )
    self._update( sqlUpdate )

    job_his = self.insertHistoryJob( str( JobID ), resultInfo[0][0], status, resultInfo[0][1], Time.toString(),
                          resultInfo[0][2], resultInfo[0][3], resultInfo[0][4], resultInfo[0][5],
                          resultInfo[0][6] )
    if not job_his['OK']:
      return S_ERROR( 'Failed to insert Big Data Job in history table' )
    result = self.setIntoJobDBStatus( JobID, status, resultInfo[0][1], resultInfo[0][2], resultInfo[0][7].strip() )

    return S_OK( result )

  def setIntoJobDBStatus( self, jobID, status, JobGroup, Site, jobBDSoftwareName ):
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    if status == "Running":
      self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( jobID,
                                                        status,
                                                        "Running with JobID: " + jobBDSoftwareName,
                                                        'BigDataMonitoring@%s' % Site ) )
      jobStatus = jobReport.setJobStatus( int( jobID ),
                                          status,
                                          "MapReducing Step, running with JobID: " + jobBDSoftwareName,
                                          'BigDataMonitoring@%s' % Site )
      if not jobStatus['OK']:
        self.log.warn( jobStatus['Message'] )
      jobSite = jobReport.setJobSite( int( jobID ), Site )
      self.log.verbose( 'setJobSite(%s,%s)' % ( jobID, Site ) )
      if not jobSite['OK']:
        self.log.warn( jobSite['Message'] )
      jobDB.setJobAttribute( jobID, "ApplicationStatus", "Running MapReduce" )
    if status == "Submitted":
      if ( jobBDSoftwareName != "" ):
        jobStatus = jobReport.setJobStatus( int( jobID ),
                                            status,
                                            "Job in Queue with JobId:" + jobBDSoftwareName,
                                            'BigDataMonitoring@%s' % Site )
        self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( jobID,
                                                          status,
                                                          "Job in Queue with JobId:" + jobBDSoftwareName,
                                                          'BigDataMonitoring@%s' % Site ) )
        if not jobStatus['OK']:
          self.log.warn( jobStatus['Message'] )
        jobSite = jobReport.setJobSite( int( jobID ), Site )
        self.log.verbose( 'setJobSite(%s,%s)' % ( jobID, Site ) )
        if not jobSite['OK']:
          self.log.warn( jobSite['Message'] )
        jobDB.setJobAttribute( jobID, "ApplicationStatus", "Job in Queue" )
      else:
        jobStatus = jobReport.setJobStatus( int( jobID ),
                                            status = 'Rescheduled',
                                            application = "Unknown Status",
                                            sendFlag = True )
        self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( jobID,
                                                          'Rescheduled',
                                                          "Unknown Status",
                                                          'BigDataMonitoring@%s' % Site ) )
        if not jobStatus['OK']:
          self.log.warn( jobStatus['Message'] )
        jobSite = jobReport.setJobSite( int( jobID ), Site )
        self.log.verbose( 'setJobSite(%s,%s)' % ( jobID, Site ) )
        if not jobSite['OK']:
          self.log.warn( jobSite['Message'] )
    if status == "Done":
      self.log.verbose( 'setJobStatus(%s,%s,%s,%s)' % ( jobID,
                                                        status,
                                                        "MapReducing Process Finished",
                                                        'BigDataMonitoring@%s' % Site ) )
      jobStatus = jobReport.setJobStatus( int( jobID ),
                                          status,
                                          "MapReducing Process Finished",
                                          'BigDataMonitoring@%s' % Site )
      if not jobStatus['OK']:
        self.log.warn( jobStatus['Message'] )
      jobSite = jobReport.setJobSite( int( jobID ), Site )
      self.log.verbose( 'setJobSite(%s,%s)' % ( jobID, Site ) )
      if not jobSite['OK']:
        self.log.warn( jobSite['Message'] )
      jobDB.setJobAttribute( jobID, "ApplicationStatus", "MapReduce Complete" )
    return S_OK( 'OK' )

  def insertBigDataJob( self, JobID, JobName, LastUpdate, NameNode,
                        SiteName, PublicIP, ErrorMes, InputData,
                        OutputData, SoftName, SoftVersion, HighLevelLang,
                        HighLevelLangVersion, status ):
    """
    Insert tupple Job in JobBigData Table
    """
    self.log.info( 'BigDataDB:insertBigDataJob' )
    #Testing matching fields
    if SoftName not in self.validSoftware:
      self.log.error( 'Defined BigData Software %s not matching with:' % self.validSoftware, SoftName )
      return S_ERROR( "ERROR" )

    if SoftVersion not in self.validSoftwareVersion:
      self.log.error( 'Defined BigData Software ver. %s not matching with:' % self.validSoftwareVersion, SoftVersion )
      return S_ERROR( "ERROR" )

    if HighLevelLang not in self.validHighLevelLang:
      self.log.error( 'Defined BigData HLLang %s not matching with:' % self.validHighLevelLang, HighLevelLang )
      return S_ERROR( "ERROR" )

    tableName, validStates, _idName = self.__getTypeTuple( 'job' )

    fields = [ 'BdJobID', 'BdJobName', 'BdJobStatus', 'BdJobLastUpdate', 'NameNode',
              'SiteName', 'PublicIP', 'ErrorMessage', 'BdInputData',
              'BdOutputData', 'BdSoftName', 'BdSoftVersion', 'BdSoftHighLevelLang',
              'BdSoftHighLevelLangVersion']

    values = [JobID, JobName, status, LastUpdate, NameNode,
              SiteName, PublicIP, ErrorMes, InputData,
              OutputData, SoftName, SoftVersion, HighLevelLang,
              HighLevelLangVersion ]

    self.log.info( 'BigDataDB:insertBigDataJob:JobInsertion' )
    job = self._insert( tableName , fields, values )
    if not job['OK']:
      return S_ERROR( 'Failed to insert new Big Data Job' )

    self.log.info( 'BigDataDB:insertBigDataJob:JobHistoryInsertion' )
    job_his = self.insertHistoryJob( JobID, JobName, status, NameNode, LastUpdate,
                          SiteName, SoftName, SoftVersion, HighLevelLang,
                          HighLevelLangVersion )
    if not job_his['OK']:
      return S_ERROR( 'Failed to insert Big Data Job in history table' )

    return S_OK( job )

  def insertHistoryJob( self, JobID, JobName, status, NameNode, Update,
                          SiteName, SoftName, SoftVersion, HighLevelLang,
                          HighLevelLangVersion ):
      """
      Insert historic data
      """
      fields = [ 'His_BdJobID', 'His_JobName', 'His_JobStatus', 'His_Update', 'His_NameNode',
              'His_SiteName', 'His_BdSoftName', 'His_BdSoftVersion', 'His_BdSoftHighLevelLang',
              'His_BdSoftHighLevelLangVersion']

      values = [JobID, JobName, status, Update, NameNode,
                SiteName, SoftName, SoftVersion, HighLevelLang,
                HighLevelLangVersion ]

      job = self._insert( 'BD_History' , fields, values )
      if not job['OK']:
        return S_ERROR( 'Failed to insert new Big Data Job' )

      return S_OK( job )
