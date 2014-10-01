# $HeadURL$

import os
import time

from DIRAC                                            import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities                             import List
from DIRAC.Core.Utilities.Subprocess                     import shellCall

__RCSID__ = '$Id: $'


class ConnectionUtils:

  def __init__( self, user, host, port = None, password = None ):
    self.log = gLogger.getSubLogger( "ConnectionUtils" )
    self.user = user
    self.host = host
    self.password = password
    self.port = port

  def __ssh_call( self, command, timeout ):
    try:
      import pexpect
      expectFlag = True
    except:
      from DIRAC import shellCall
      expectFlag = False

    if not timeout:
      timeout = 999

    if expectFlag:
      ssh_newkey = 'Are you sure you want to continue connecting'
      child = pexpect.spawn( command, timeout = timeout )

      i = child.expect( [pexpect.TIMEOUT, ssh_newkey, pexpect.EOF, 'password: '] )
      if i == 0: # Timeout        
          return S_OK( ( -1, child.before, 'SSH login failed' ) )
      elif i == 1: # SSH does not have the public key. Just accept it.
          child.sendline ( 'yes' )
          child.expect ( 'password: ' )
          i = child.expect( [pexpect.TIMEOUT, 'password: '] )
          if i == 0: # Timeout
            return S_OK( ( -1, child.before + child.after, 'SSH login failed' ) )
          elif i == 1:
            child.sendline( password )
            child.expect( pexpect.EOF )
            return S_OK( ( 0, child.before, '' ) )
      elif i == 2:
        # Passwordless login, get the output
        return S_OK( ( 0, child.before, '' ) )

      if self.password:
        child.sendline( self.password )
        child.expect( pexpect.EOF )
        return S_OK( ( 0, child.before, '' ) )
      else:
        return S_ERROR( ( -1, child.before, '' ) )
    else:
      # Try passwordless login
      result = shellCall( timeout, command )
      return result


  def sshCall( self, timeout, cmdSeq ):
    """ Execute remote command via a ssh remote call
    """
    command = cmdSeq
    if type( cmdSeq ) == type( [] ):
      command = ' '.join( cmdSeq )

    command = "ssh -l %s %s '%s'" % ( self.user, self.host, command )
    gLogger.info( 'Command Submitted: ', command )
    return self.__ssh_call( command, timeout )

  def sshCallByPort( self, timeout, cmdSeq ):
    """ Execute remote command via a ssh remote call
    """
    command = cmdSeq
    if type( cmdSeq ) == type( [] ):
      command = ' '.join( cmdSeq )

    command = "ssh -p %s -l %s %s '%s'" % ( self.port, self.user, self.host, command )
    gLogger.info( 'Command Submitted: ', command )
    return self.__ssh_call( command, timeout )


  def sshOnlyCall( self, timeout, cmdSeq ):
    """ Execute remote command via a ssh remote call
    """
    return self.__ssh_call( cmdSeq, timeout )

  def scpCall( self, timeout, localFile, destinationPath, upload = True ):
    """ Execute scp copy
    """
    if upload:
      command = "scp -r %s %s@%s:%s" % ( localFile, self.user, self.host, destinationPath )
    else:
      command = "scp -r %s@%s:%s %s" % ( self.user, self.host, destinationPath, localFile )
    return self.__ssh_call( command, timeout )


  def scpCallByPort( self, timeout, localFile, destinationPath, upload = True ):
    """ Execute scp copy
    """
    if upload:
      command = "scp -P %s -r %s %s@%s:%s" % ( self.port, localFile, self.user, self.host, destinationPath )
    else:
      command = "scp -P %s -r %s@%s:%s %s" % ( self.port, self.user, self.host, destinationPath, localFile )
    return self.__ssh_call( command, timeout )
