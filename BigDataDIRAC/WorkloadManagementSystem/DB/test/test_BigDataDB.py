#!/usr/bin/env python
#
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )

from BigDataDIRAC.WorkloadManagementSystem.DB.BigDataDB               import BigDataDB

import DIRAC

db = BigDataDB()
validStates = db.validJobStates

ret = db.insertBigDataJob( 1, 'Montecarlo', 'CesgaHadoop', 'Cesga' , 'Mapping', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 2, 'Montecarlo2', 'CesgaHadoop', 'Cesga' , 'Mapping', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 3, 'Montecarlo3', 'CesgaHadoop', 'Cesga' , 'Reduction', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 4, 'Montecarlo4', 'CesgaHadoop', 'Cesga' , 'Submitted', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 5, 'Montecarlo5', 'CesgaHadoop', 'Cesga' , 'Done', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 6, 'Montecarlo6', 'CesgaHadoop', 'Cesga' , 'Stalled', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 7, 'Montecarlo7', 'CesgaHadoop', 'Cesga' , 'Mapping', 0, '', '', 'Hadoop', '', '' )
ret = db.insertBigDataJob( 8, 'Montecarlo8', 'CesgaHadoop', 'Cesga' , 'Mapping', 0, '', '', 'Hadoop', '', '' )

