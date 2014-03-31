########################################################################
# $HeadURL$
# File :   BigDataDirector.py
# Author : Victor Fernandez
########################################################################

from DIRAC import S_OK, S_ERROR, gConfig

from BigDataDIRAC.Resources.BigData.SoftBigDataDirector           import SoftBigDataDirector
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV1        import HadoopV1
from BigDataDIRAC.WorkloadManagementSystem.Client.HadoopV2        import HadoopV2
from BigDataDIRAC.WorkloadManagementSystem.Client.Twister        import Twister

__RCSID__ = '$Id: $'

class BigDataDirector( SoftBigDataDirector ):
  def __init__( self, submitPool ):

    SoftBigDataDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes common configuration for BigData Director
    """

    BigDataDirector.configure( self, csSection, submitPool )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    BigDataDirector.configureFromSection( self, mySection )

  def _submitInstance( self, endpoint, CPUTime ):

    endpointsPath = "/Resources/BigData/BigDataEndpoints"

    driver = gConfig.getValue( "%s/%s/%s" % ( endpointsPath, endpoint, 'BigDataDriver' ), "" )

    if driver == 'HadoopV1':
      hdv1 = HadoopV1( imageName, endpoint )
      result = hdv1.submitNewBigJob()
      if not result[ 'OK' ]:
        return result
      bdjobID = result['Value'][0]
      return S_OK( bdjobID )

    if driver == 'HadoopV2':
      hdv2 = HadoopV2( imageName, endpoint )
      result = hdv2.submitNewBigJob()
      if not result[ 'OK' ]:
        return result
      bdjobID = result['Value'][0]
      return S_OK( bdjobID )

    if driver == 'Twister':
      twister = Twister( imageName, endpoint )
      result = twister.submitNewBigJob()
      if not result[ 'OK' ]:
        return result
      bdjobID = result['Value'][0]
      return S_OK( bdjobID )

    return S_ERROR( 'Unknown DIRAC BigData driver %s' % driver )

