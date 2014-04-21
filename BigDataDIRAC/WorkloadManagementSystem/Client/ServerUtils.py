########################################################################
# $HeadURL$
# File :   ServerUtils.py
# Author : Victor Fernandez
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

__RCSID__ = "$Id: $"

from DIRAC.WorkloadManagementSystem.Client.ServerUtils import getDBOrClient

def getBigDataDB():
  serverName = 'WorkloadManagement/BigDataJobManager'
  BigDataDB = None
  try:
    from BigDataDIRAC.WorkloadManagementSystem.DB.BigDataDB               import BigDataDB
  except:
    pass
  return getDBOrClient( BigDataDB, serverName )

BigDataDB = getBigDataDB()
