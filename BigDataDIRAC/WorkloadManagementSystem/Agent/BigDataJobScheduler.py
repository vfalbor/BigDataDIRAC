########################################################################
# $HeadURL$
# File :   BigDataScheduler.py
# Author : Victor Fernandez
########################################################################

"""  The Big Data Scheduler controls the submission of Jobs via the
     appropriated Directors. These are Backend-specific BigDataDirector derived classes.
     This is a simple wrapper that performs the instantiation and monitoring
     of the BigDataDirector instances and add workload to them via ThreadPool
     mechanism.

     From the base Agent class it uses the following configuration Parameters
       - WorkDir:
       - PollingTime:
       - ControlDirectory:
       - MaxCycles:

     The following parameters are searched for in WorkloadManagement/BigDataDirector:
       - ThreadStartDelay:
       - SubmitPools: All the Submit pools that are to be initialized
       - DefaultSubmitPools: If no specific pool is requested, use these

"""

import random, time, re
import DIRAC
from DIRAC  import gLogger, S_OK, S_ERROR

from numpy.random import poisson
from random       import shuffle

# DIRAC
from DIRAC                                                    import gConfig
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.Utilities.ThreadPool                          import ThreadPool
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB            import maxCPUSegments
from DIRAC.WorkloadManagementSystem.Client.ServerUtils        import taskQueueDB
from DIRAC.WorkloadManagementSystem.Client.ServerUtils        import jobDB
from DIRAC.Resources.Catalog.FileCatalog                      import FileCatalog
from BigDataDIRAC.Resources.BigData.BigDataDirector           import BigDataDirector
from BigDataDIRAC.WorkloadManagementSystem.Client.ServerUtils import BigDataDB
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                import ClassAd
from DIRAC.Interfaces.API.Dirac                               import Dirac

__RCSID__ = "$Id: $"

#FIXME: why do we need the random seed ?
random.seed()

class BigDataJobScheduler( AgentModule ):

  def initialize( self ):
    """ Standard constructor
    """
    import threading

    self.__tmpSandBoxDir = "/tmp/"
    self.jobDataset = ""
    self.am_setOption( "PollingTime", 60.0 )

    self.am_setOption( "ThreadStartDelay", 1 )
    self.am_setOption( "SubmitPools", [] )
    self.am_setOption( "DefaultSubmitPools", [] )

    self.am_setOption( "minThreadsInPool", 0 )
    self.am_setOption( "maxThreadsInPool", 2 )
    self.am_setOption( "totalThreadsInPool", 40 )

    self.directors = {}
    self.pools = {}

    self.directorDict = {}
    self.pendingTaskQueueJobs = {}

    self.callBackLock = threading.Lock()

    return DIRAC.S_OK()

  def execute( self ):
    """Main Agent code:
      1.- Query TaskQueueDB for existing TQs
      2.- Count Pending Jobs
      3.- Submit Jobs
    """
    self.__checkSubmitPools()

    bigDataJobsToSubmit = {}
    bigDataJobIdsToSubmit = {}

    for directorName, directorDict in self.directors.items():
      self.log.verbose( 'Checking Director:', directorName )
      self.log.verbose( 'RunningEndPoints:', directorDict['director'].runningEndPoints )
      for runningEndPointName in directorDict['director'].runningEndPoints:
        runningEndPointDict = directorDict['director'].runningEndPoints[runningEndPointName]
        NameNode = runningEndPointDict['NameNode']
        jobsByEndPoint = 0
        result = BigDataDB.getBigDataJobsByStatusAndEndpoint( 'Submitted', NameNode )
        if result['OK']:
          jobsByEndPoint += len( result['Value'] )
        result = BigDataDB.getBigDataJobsByStatusAndEndpoint( 'Running', NameNode )
        if result['OK']:
          jobsByEndPoint += len( result['Value'] )
        self.log.verbose( 'Checking Jobs By EndPoint %s:' % jobsByEndPoint )
        jobLimitsEndPoint = runningEndPointDict['LimitQueueJobsEndPoint']

        bigDataJobs = 0
        if jobsByEndPoint >= jobLimitsEndPoint:
          self.log.info( '%s >= %s Running jobs reach job limits: %s, skipping' % ( jobsByEndPoint, jobLimitsEndPoint, runningEndPointName ) )
          continue
        else:
          bigDataJobs = jobLimitsEndPoint - jobsByEndPoint
        requirementsDict = runningEndPointDict['Requirements']

        self.log.info( 'Requirements Dict: ', requirementsDict )
        result = taskQueueDB.getMatchingTaskQueues( requirementsDict )
        if not result['OK']:
          self.log.error( 'Could not retrieve TaskQueues from TaskQueueDB', result['Message'] )
          return result

        taskQueueDict = result['Value']
        self.log.info( 'Task Queues Dict: ', taskQueueDict )
        jobs = 0
        priority = 0
        cpu = 0
        jobsID = 0
        self.log.info( 'Pending Jobs from TaskQueue, which not matching before: ', self.pendingTaskQueueJobs )
        for tq in taskQueueDict:
          jobs += taskQueueDict[tq]['Jobs']
          priority += taskQueueDict[tq]['Priority']
          cpu += taskQueueDict[tq]['Jobs'] * taskQueueDict[tq]['CPUTime']

          #Matching of Jobs with BigData Softwares
          #This process is following the sequence:
          #Retrieve a job from taskqueueDict
          #Get job name and try to match with the resources        
          #If not match store the var pendingTaskQueueJobs for the
          #next iteration
          #
          #This matching is doing with the following JobName Pattern
          # NameSoftware _ SoftwareVersion _ HighLanguageName _ HighLanguageVersion _ DataSetName          
          #extract a job from the TaskQueue
          if tq not in self.pendingTaskQueueJobs.keys():
            self.pendingTaskQueueJobs[tq] = {}
          getJobFromTaskQueue = taskQueueDB.matchAndGetJob( taskQueueDict[tq] )
          if not getJobFromTaskQueue['OK']:
            self.log.error( 'Could not get Job and FromTaskQueue', getJobFromTaskQueue['Message'] )
            return getJobFromTaskQueue

          jobInfo = getJobFromTaskQueue['Value']
          jobID = jobInfo['jobId']
          jobAttrInfo = jobDB.getJobAttributes( jobID )

          if not jobAttrInfo['OK']:
            self.log.error( 'Could not get Job Attributes', jobAttrInfo['Message'] )
            return jobAttrInfo
          jobInfoUniq = jobAttrInfo['Value']
          jobName = jobInfoUniq['JobName']
          self.pendingTaskQueueJobs[tq][jobID] = jobName


          result = jobDB.getJobJDL( jobID, True )
          classAdJob = ClassAd( result['Value'] )
          arguments = 0
          if classAdJob.lookupAttribute( 'Arguments' ):
            arguments = classAdJob.getAttributeString( 'Arguments' )
          #if not classAdJob.lookupAttribute( 'Arguments' ):
          #  continue

          jobsToSubmit = self.matchingJobsForBDSubmission( arguments,
                                                       runningEndPointName,
                                                       runningEndPointDict['BigDataSoftware'],
                                                       runningEndPointDict['BigDataSoftwareVersion'],
                                                       runningEndPointDict['HighLevelLanguage']['HLLName'],
                                                       runningEndPointDict['HighLevelLanguage']['HLLVersion'],
                                                       jobID )
          if ( jobsToSubmit == "OK" ):
            if directorName not in bigDataJobsToSubmit:
              bigDataJobsToSubmit[directorName] = {}
            if runningEndPointName not in bigDataJobsToSubmit[directorName]:
              bigDataJobsToSubmit[directorName][runningEndPointName] = {}
            bigDataJobsToSubmit[directorName][runningEndPointName] = { 'JobId': jobID,
                                                        'JobName': jobName,
                                                        'TQPriority': priority,
                                                        'CPUTime': cpu,
                                                        'BigDataEndpoint': runningEndPointName,
                                                        'BigDataEndpointNameNode': runningEndPointDict['NameNode'],
                                                        'BdSoftware': runningEndPointDict['BigDataSoftware'],
                                                        'BdSoftwareVersion': runningEndPointDict['BigDataSoftwareVersion'],
                                                        'HLLName' : runningEndPointDict['HighLevelLanguage']['HLLName'],
                                                        'HLLVersion' : runningEndPointDict['HighLevelLanguage']['HLLVersion'],
                                                        'NumBigDataJobsAllowedToSubmit': bigDataJobs,
                                                        'SiteName': runningEndPointDict['SiteName'],
                                                        'PublicIP': runningEndPointDict['PublicIP'],
                                                        'User': runningEndPointDict['User'],
                                                        'Port': runningEndPointDict['Port'],
                                                        'UsePilot': runningEndPointDict['UsePilot'],
                                                        'IsInteractive': runningEndPointDict['IsInteractive'],
                                                        'Arguments': arguments }
            del self.pendingTaskQueueJobs[tq][jobID]
          else:
            self.log.error( jobsToSubmit )
        self.log.info( 'Pending Jobs from TaskQueue, which not matching after: ', self.pendingTaskQueueJobs )
        for tq in self.pendingTaskQueueJobs.keys():
          for jobid in self.pendingTaskQueueJobs[tq].keys():
            result = jobDB.getJobJDL( jobid, True )
            classAdJob = ClassAd( result['Value'] )
            arguments = 0
            if classAdJob.lookupAttribute( 'Arguments' ):
              arguments = classAdJob.getAttributeString( 'Arguments' )
            #if not classAdJob.lookupAttribute( 'Arguments' ):
            #  continue
            #do the match with the runningEndPoint
            jobsToSubmit = self.matchingJobsForBDSubmission( arguments,
                                                             runningEndPointName,
                                                             runningEndPointDict['BigDataSoftware'],
                                                             runningEndPointDict['BigDataSoftwareVersion'],
                                                             runningEndPointDict['HighLevelLanguage']['HLLName'],
                                                             runningEndPointDict['HighLevelLanguage']['HLLVersion'],
                                                             jobid )
            if ( jobsToSubmit == "OK" ):
              if directorName not in bigDataJobsToSubmit:
                bigDataJobsToSubmit[directorName] = {}
              if runningEndPointName not in bigDataJobsToSubmit[directorName]:
                bigDataJobsToSubmit[directorName][runningEndPointName] = {}
              bigDataJobsToSubmit[directorName][runningEndPointName] = { 'JobId': jobid,
                                                          'JobName': self.pendingTaskQueueJobs[tq][jobid],
                                                          'TQPriority': priority,
                                                          'CPUTime': cpu,
                                                          'BigDataEndpoint': runningEndPointName,
                                                          'BigDataEndpointNameNode': runningEndPointDict['NameNode'],
                                                          'BdSoftware': runningEndPointDict['BigDataSoftware'],
                                                          'BdSoftwareVersion': runningEndPointDict['BigDataSoftwareVersion'],
                                                          'HLLName' : runningEndPointDict['HighLevelLanguage']['HLLName'],
                                                          'HLLVersion' : runningEndPointDict['HighLevelLanguage']['HLLVersion'],
                                                          'NumBigDataJobsAllowedToSubmit': bigDataJobs,
                                                          'SiteName': runningEndPointDict['SiteName'],
                                                          'PublicIP': runningEndPointDict['PublicIP'],
                                                          'User': runningEndPointDict['User'],
                                                          'Port': runningEndPointDict['Port'],
                                                          'UsePilot': runningEndPointDict['UsePilot'],
                                                          'IsInteractive': runningEndPointDict['IsInteractive'],
                                                          'Arguments': arguments  }
              del self.pendingTaskQueueJobs[tq][jobid]
            else:
             self.log.error( jobsToSubmit )
        if not jobs and not self.pendingTaskQueueJobs:
          self.log.info( 'No matching jobs for %s found, skipping' % NameNode )
          continue

        self.log.info( '___BigDataJobsTo Submit:', bigDataJobsToSubmit )

    for directorName, JobsToSubmitDict in bigDataJobsToSubmit.items():
      for runningEndPointName, jobsToSubmitDict in JobsToSubmitDict.items():
        if self.directors[directorName]['isEnabled']:
          self.log.info( 'Requesting submission to %s of %s' % ( runningEndPointName, directorName ) )

          director = self.directors[directorName]['director']
          pool = self.pools[self.directors[directorName]['pool']]

          jobIDs = JobsToSubmitDict[runningEndPointName]['JobId']
          jobName = JobsToSubmitDict[runningEndPointName]['JobName']
          endpoint = JobsToSubmitDict[runningEndPointName]['BigDataEndpoint']
          runningSiteName = JobsToSubmitDict[runningEndPointName]['SiteName']
          NameNode = JobsToSubmitDict[runningEndPointName]['BigDataEndpointNameNode']
          BigDataSoftware = JobsToSubmitDict[runningEndPointName]['BdSoftware']
          BigDataSoftwareVersion = JobsToSubmitDict[runningEndPointName]['BdSoftwareVersion']
          HLLName = JobsToSubmitDict[runningEndPointName]['HLLName']
          HLLVersion = JobsToSubmitDict[runningEndPointName]['HLLVersion']
          PublicIP = JobsToSubmitDict[runningEndPointName]['PublicIP']
          User = JobsToSubmitDict[runningEndPointName]['User']
          Port = JobsToSubmitDict[runningEndPointName]['Port']
          UsePilot = JobsToSubmitDict[runningEndPointName]['UsePilot']
          IsInteractive = JobsToSubmitDict[runningEndPointName]['IsInteractive']
          Arguments = JobsToSubmitDict[runningEndPointName]['Arguments']
          numBigDataJobsAllowed = JobsToSubmitDict[runningEndPointName]['NumBigDataJobsAllowedToSubmit']

          ret = pool.generateJobAndQueueIt( director.submitBigDataJobs,
                                            args = ( endpoint, numBigDataJobsAllowed, runningSiteName, NameNode,
                                                     BigDataSoftware, BigDataSoftwareVersion, HLLName, HLLVersion,
                                                     PublicIP, Port, jobIDs, runningEndPointName, jobName, User, self.jobDataset, UsePilot, IsInteractive ),
                                            oCallback = self.callBack,
                                            oExceptionCallback = director.exceptionCallBack,
                                            blocking = False )
          if not ret['OK']:
            # Disable submission until next iteration
            self.directors[directorName]['isEnabled'] = False
          else:
            time.sleep( self.am_getOption( 'ThreadStartDelay' ) )

    if 'Default' in self.pools:
      # only for those in "Default' thread Pool
      # for pool in self.pools:
      self.pools['Default'].processAllResults()

    return DIRAC.S_OK()

  def matchingJobsForBDSubmission( self, arguments, bigdataendpoint, BigDataSoftware,
                                   BigDataSoftwareVersion, HLLName, HLLVersion, jobid ):
    """
     Jobs matching, first with the dataset and the SITE, find in the Database the matching with the Dataset key
     As the second step the endpoind is matched with the resulting SITES and in the case of 
     was matching, in the third step the job will be matched with the bigdatasoft of the SITE.
    """
    self.jobDataset = ""
    returned = jobDB.getInputData( jobid )
    if not returned['OK']:
      self.log.error( "There is not Input Data stored in the Job" )
      return "Error"

    if returned['Value'] != []:
      self.jobDataset = returned['Value'][0]

    if arguments == 0 and self.jobDataset == "":
      self.log.error( "Error reading the job arguments for BigData Submission:", arguments )
      return "Error"
    if arguments == 0 and self.jobDataset != "":
      self.fileCatalogue = FileCatalog()
      result = self.fileCatalogue.getReplicas( self.jobDataset )
      if not result['OK'] or result['Value']['Successful'] == {}:
        return S_ERROR( result )
      return_exit = False
      for SiteName in result['Value']['Successful'][self.jobDataset]:
        if bigdataendpoint in SiteName:
            return( "OK" )
        else:
            return_exit = True
      if return_exit:
        return "Dataset match with SiteName but Site doesn't have the software"

    self.log.info( "BigDataEndpoint", bigdataendpoint )
    self.log.info( "BigDataSoftware", BigDataSoftware )
    self.log.info( "BigDataSoftwareVersion", BigDataSoftwareVersion )
    self.log.info( "HLLName", HLLName )
    self.log.info( "HLLVersion", HLLVersion )

    jobNameSplitted = re.split( ' ', arguments )

    jobBigDataSoft = jobNameSplitted[0]
    if jobBigDataSoft not in BigDataDB.validSoftware:
      self.log.error( "Argument %s for valid B.D. software is not in the list of accepted:" % ( jobBigDataSoft ), BigDataDB.validSoftware )
      return "Error"

    jobBigDataVersion = jobNameSplitted[1]
    if jobBigDataVersion not in BigDataDB.validSoftwareVersion:
      self.log.error( "Argument %s for valid B.D. software version is not in the list of accepted:" % ( jobBigDataVersion ), BigDataDB.validSoftwareVersion )
      return "Error"

    jobHHLSoft = jobNameSplitted[2]
    if jobHHLSoft not in BigDataDB.validHighLevelLang:
      self.log.error( "Argument %s for valid B.D. H.L. software is not in the list of accepted:" % ( jobHHLSoft ), BigDataDB.validHighLevelLang )
      return "Error"

    jobHHLVersion = jobNameSplitted[3]
    #if jobHHLVersion not in BigDataDB.validHighLevelLangVersion:
    #  self.log.error( "Argument %s for valid B.D. H.L. software version is not in the list of accepted:" % ( jobHHLVersion ), BigDataDB.validHighLevelLangVersion )
    #  return "Error"

    #Old-one
    #JobSiteNames = BigDataDB.getSiteNameByDataSet( self.jobDataset );
    self.fileCatalogue = FileCatalog()
    result = self.fileCatalogue.getReplicas( self.jobDataset )
    if not result['OK'] or result['Value']['Successful'] == {}:
      return S_ERROR( result )
    for SiteName in result['Value']['Successful'][self.jobDataset]:
      if bigdataendpoint in SiteName:
        if ( jobBigDataSoft == BigDataSoftware ) and ( jobBigDataVersion == BigDataSoftwareVersion ) and ( HLLName == jobHHLSoft ) and ( HLLVersion == jobHHLVersion ):
          return( "OK" )
        else:
          return "Dataset match with SiteName but Site doesn't have the software"

    return "Dataset does not match with any Site"

  def submitPilotsForTaskQueue( self, taskQueueDict, waitingPilots ):

    taskQueueID = taskQueueDict['TaskQueueID']
    maxCPU = maxCPUSegments[-1]
    extraPilotFraction = self.am_getOption( 'extraPilotFraction' )
    extraPilots = self.am_getOption( 'extraPilots' )

    taskQueuePriority = taskQueueDict['Priority']
    self.log.verbose( 'Priority for TaskQueue %s:' % taskQueueID, taskQueuePriority )
    taskQueueCPU = max( taskQueueDict['CPUTime'], self.am_getOption( 'lowestCPUBoost' ) )
    self.log.verbose( 'CPUTime  for TaskQueue %s:' % taskQueueID, taskQueueCPU )
    taskQueueJobs = taskQueueDict['Jobs']
    self.log.verbose( 'Jobs in TaskQueue %s:' % taskQueueID, taskQueueJobs )

    # Determine number of pilots to submit, boosting TaskQueues with low CPU requirements
    #pilotsToSubmit = poisson( ( self.pilotsPerPriority * taskQueuePriority +
    #                            self.pilotsPerJob * taskQueueJobs ) * maxCPU / taskQueueCPU )
    pilotsToSubmit = poisson( ( taskQueuePriority +
                                taskQueueJobs ) * maxCPU / taskQueueCPU )
    # limit the number of pilots according to the number of waiting job in the TaskQueue
    # and the number of already submitted pilots for that TaskQueue
    pilotsToSubmit = min( pilotsToSubmit, int( ( 1 + extraPilotFraction ) * taskQueueJobs ) + extraPilots - waitingPilots )
    if pilotsToSubmit <= 0:
      return DIRAC.S_OK( 0 )
    self.log.verbose( 'Submitting %s pilots for TaskQueue %s' % ( pilotsToSubmit, taskQueueID ) )

    return self.__submitPilots( taskQueueDict, pilotsToSubmit )

  def __submitPilots( self, taskQueueDict, pilotsToSubmit ):
    """
      Try to insert the submission in the corresponding Thread Pool, disable the Thread Pool
      until next iteration once it becomes full
    """
    # Check if an specific MiddleWare is required
    if 'SubmitPools' in taskQueueDict:
      submitPools = taskQueueDict[ 'SubmitPools' ]
    else:
      submitPools = self.am_getOption( 'DefaultSubmitPools' )
    submitPools = DIRAC.List.randomize( submitPools )

    for submitPool in submitPools:
      self.log.verbose( 'Trying SubmitPool:', submitPool )

      if not submitPool in self.directors or not self.directors[submitPool]['isEnabled']:
        self.log.verbose( 'Not Enabled' )
        continue

      pool = self.pools[self.directors[submitPool]['pool']]
      director = self.directors[submitPool]['director']
      ret = pool.generateJobAndQueueIt( director.submitPilots,
                                        args = ( taskQueueDict, pilotsToSubmit, self.workDir ),
                                        oCallback = self.callBack,
                                        oExceptionCallback = director.exceptionCallBack,
                                        blocking = False )
      if not ret['OK']:
        # Disable submission until next iteration
        self.directors[submitPool]['isEnabled'] = False
      else:
        time.sleep( self.am_getOption( 'ThreadStartDelay' ) )
        break

    return DIRAC.S_OK( pilotsToSubmit )

  def __checkSubmitPools( self ):
    # this method is called at initialization and at the beginning of each execution cycle
    # in this way running parameters can be dynamically changed via the remote
    # configuration.

    # First update common Configuration for all Directors
    self.__configureDirector()

    # Now we need to initialize one thread for each Director in the List,
    # and check its configuration:
    for submitPool in self.am_getOption( 'SubmitPools' ):
      # check if the Director is initialized, then reconfigure
      if submitPool not in self.directors:
        # instantiate a new Director
        self.__createDirector( submitPool )

      self.__configureDirector( submitPool )

      # Now enable the director for this iteration, if some RB/WMS/CE is defined
      if submitPool in self.directors:
        if 'resourceBrokers' in dir( self.directors[submitPool]['director'] ) and self.directors[submitPool]['director'].resourceBrokers:
          self.directors[submitPool]['isEnabled'] = True
        if 'computingElements' in dir( self.directors[submitPool]['director'] ) and self.directors[submitPool]['director'].computingElements:
          self.directors[submitPool]['isEnabled'] = True

    # Now remove directors that are not Enable (they have been used but are no
    # longer required in the CS).
    pools = []
    for submitPool in self.directors.keys():
      if not self.directors[submitPool]['isEnabled']:
        self.log.info( 'Deleting Director for SubmitPool:', submitPool )
        director = self.directors[submitPool]['director']
        del self.directors[submitPool]
        del director
      else:
        pools.append( self.directors[submitPool]['pool'] )

    # Finally delete ThreadPools that are no longer in use
    for pool in self.pools:
      if pool != 'Default' and not pool in pools:
        pool = self.pools.pop( pool )
        # del self.pools[pool]
        del pool

  def __createDirector( self, submitPool ):
    """
     Instantiate a new VMDirector for the given SubmitPool
    """

    self.log.info( 'Creating Director for SubmitPool:', submitPool )
    # 1. get the BigDataDirector

    director = BigDataDirector( submitPool )
    directorName = '%sDirector' % submitPool

    self.log.info( 'Director Object instantiated:', directorName )

    # 2. check the requested ThreadPool (if not defined use the default one)
    directorPool = self.am_getOption( submitPool + '/Pool', 'Default' )
    if not directorPool in self.pools:
      self.log.info( 'Adding Thread Pool:', directorPool )
      poolName = self.__addPool( directorPool )
      if not poolName:
        self.log.error( 'Can not create Thread Pool:', directorPool )
        return

    # 3. add New director
    self.directors[ submitPool ] = { 'director': director,
                                     'pool': directorPool,
                                     'isEnabled': False,
                                   }

    self.log.verbose( 'Created Director for SubmitPool', submitPool )

    return

  def __configureDirector( self, submitPool = None ):
    # Update Configuration from CS
    # if submitPool == None then,
    #     disable all Directors
    # else
    #    Update Configuration for the BigDataDirector of that SubmitPool
    if submitPool == None:
      self.workDir = self.am_getOption( 'WorkDirectory' )
      # By default disable all directors
      for director in self.directors:
        self.directors[director]['isEnabled'] = False

    else:
      if submitPool not in self.directors:
        DIRAC.abort( -1, "Submit Pool not available", submitPool )
      director = self.directors[submitPool]['director']
      # Pass reference to our CS section so that defaults can be taken from there
      director.configure( self.am_getModuleParam( 'section' ), submitPool )

      # Enable director for jot submission
      self.directors[submitPool]['isEnabled'] = True

  def __addPool( self, poolName ):
    # create a new thread Pool, by default it has 2 executing threads and 40 requests
    # in the Queue

    if not poolName:
      return None
    if poolName in self.pools:
      return None
    pool = ThreadPool( self.am_getOption( 'minThreadsInPool' ),
                       self.am_getOption( 'maxThreadsInPool' ),
                       self.am_getOption( 'totalThreadsInPool' ) )
    # Daemonize except "Default" pool
    if poolName != 'Default':
      pool.daemonize()
    self.pools[poolName] = pool
    return poolName

  def callBack( self, threadedJob, submitResult ):
    if not submitResult['OK']:
      self.log.error( 'submitJobBigData Failed: ', submitResult['Message'] )
      if 'Value' in submitResult:
        self.callBackLock.acquire()
        self.callBackLock.release()
    else:
      self.log.info( 'New Job BigData Submitted' )
      self.callBackLock.acquire()
      self.callBackLock.release()


