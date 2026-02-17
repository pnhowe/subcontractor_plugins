import logging
import time
import json
import base64
import ssl
import http
from urllib import request

from subcontractor.credentials import getCredentials


PROXY = None
PORT = None
DO_SSL = True

verify_ssl = False
timeout = 30

POWER_STATE_LOOKUP = { 'on': 'On', 'off': 'ForceOff', 'shutdown': 'GracefulShutdown', 'cycle': 'PowerCycle', 'reset': 'ForceRestart' }


class RedFishClient():
  def __init__( self, connection_paramaters ):
    super().__init__()
    self._chassis = None
    self._system = None

    self.ip_address = connection_paramaters[ 'ip_address' ]
    creds = connection_paramaters[ 'credentials' ]
    if isinstance( creds, str ):
      creds = getCredentials( creds )

    if PORT:
      port = ':{0}'.format( PORT )
    else:
      port = ''

    if DO_SSL:
      self.host = 'https://{0}{1}'.format( self.ip_address, port )
    else:
      self.host = 'http://{0}{1}'.format( self.ip_address, port )

    self.opener = request.OpenerDirector()

    if PROXY:   # not doing 'is not None', so empty strings don't try and proxy
      self.opener.add_handler( request.ProxyHandler( { 'http': PROXY, 'https': PROXY } ) )
    else:
      self.opener.add_handler( request.ProxyHandler( {} ) )

    self.opener.add_handler( request.HTTPHandler() )
    if hasattr( http.client, 'HTTPSConnection' ):
      if not verify_ssl:
        self.opener.add_handler( request.HTTPSHandler( context=ssl._create_unverified_context() ) )
      else:
        self.opener.add_handler( request.HTTPSHandler() )

    self.opener.add_handler( request.UnknownHandler() )

    basic_auth = 'Basic {0}'.format( base64.b64encode( '{0}:{1}'.format( creds[ 'username' ], creds[ 'password' ] ).encode() ).decode() )

    self.opener.addheaders = [
                                ( 'User-Agent', 'Contractor RedFish Client'),
                                ( 'Accepts', 'application/json' ),
                                ( 'Accept-Charset', 'utf-8' ),
                                ( 'Authorization', basic_auth ),
                              ]

  @property
  def chassis( self ):
    if not self._chassis:
      result = self._get( '/redfish/v1/Chassis' )
      self._chassis = result[ 'Members' ][0][ '@odata.id' ]
      logging.debug( 'RedFish: Set to Chassis "{0}"'.format( self._chassis ) )

    return self._chassis

  @property
  def system( self ):
    if not self._system:
      result = self._get( '/redfish/v1/Systems' )
      self._system = result[ 'Members' ][0][ '@odata.id' ]
      logging.debug( 'RedFish: Set to System "{0}"'.format( self._system ) )

    return self._system

  def _get( self, uri ):  # TODO: need retries for _get and _post
    url = '{0}{1}'.format( self.host, uri )
    logging.debug( 'RedFish: GETing "{0}"'.format( url ) )
    req = request.Request( url, method='GET' )
    try:
      resp = self.opener.open( req, timeout=timeout )
    except Exception:
      logging.error( 'Error Getting from RedFish Host' )
      raise

    http_code = resp.code
    if http_code not in ( 200, ):
      logging.error( 'RedFish: Unhandled HTTP Code {0}: "{1}"'.format( http_code, resp.read()[ :1024] ) )
      raise Exception( 'HTTP code "{0}" unhandled while Getting'.format( resp.code ) )

    result = json.load( resp )
    resp.close()
    return result

  def _post( self, uri, data ):
    url = '{0}{1}'.format( self.host, uri )
    logging.debug( 'RedFish: POSTINGing "{0}"'.format( uri ) )
    req = request.Request( url, data=json.dumps( data ).encode(), method='POST' )
    try:
      resp = self.opener.open( req, timeout=timeout )
    except Exception:
      logging.error( 'Error Posting to RedFish Host' )
      raise

    http_code = resp.code
    if http_code not in ( 200, ):
      logging.error( 'RedFish: Unhandled HTTP Code {0}: "{1}"'.format( http_code, resp.read()[ :1024] ) )
      raise Exception( 'HTTP code "{0}" unhandled while Posting'.format( resp.code ) )

    result = json.load( resp )
    resp.close()
    return result

  def getPower( self ):
    result = self._get( self.system )

    # Power State Options:
    # "On",
    # "Off"
    # "PoweringOn"
    # "PoweringOff"

    return ( 'on' if result[ 'PowerState' ] in ( 'On', 'PoweringOn' ) else 'off' )

  def setPower( self, state ):
    try:
      reset_type = POWER_STATE_LOOKUP[ state ]
    except KeyError:
      raise ValueError( 'Unknown power state "{0}"'.format( state ) )

    # ForceOff Turn off the unit immediately (non-graceful shutdown).
    # ForceOn Turn on the unit immediately.
    # ForceRestart Shut down immediately and non-gracefully and restart the system.
    # GracefulRestart Shut down gracefully and restart the system.
    # GracefulShutdown Shut down gracefully and power off.
    # Nmi Generate a diagnostic interrupt, which is usually an NMI on x86 systems, to stop normal
    # operations, complete diagnostic actions, and, typically, halt the system.
    # On Turn on the unit.
    # PowerCycle Power cycle the unit.
    # PushPowerButton Simulate the pressing of the physical power button on this unit.

    self._post( '{0}/Actions/ComputerSystem.Reset'.format( self.system ), { 'ResetType': reset_type } )


def link_test( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]

  logging.info( 'RedFish: link test on "{0}"...'.format( connection_paramaters[ 'ip_address' ] ) )
  client = RedFishClient( connection_paramaters )

  success = True
  try:
    client.getPower( 1 )
  except ConnectionError:
    success = False

  return { 'success': success }


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  desired_state = paramaters[ 'state' ]

  logging.info( 'RedFish: setting power state of "{0}" to "{1}"...'.format( connection_paramaters[ 'ip_address' ], desired_state ) )
  client = RedFishClient( connection_paramaters )

  curent_state = client.getPower()

  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  if desired_state == 'soft_off':
    desired_state = 'shutdown'

  client.setPower( desired_state )

  time.sleep( 1 )

  curent_state = client.getPower()
  logging.info( 'RedFish: setting power state of "{0}" to "{1}" complete'.format( connection_paramaters[ 'ip_address' ], desired_state ) )
  return { 'state': curent_state }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]

  logging.info( 'RedFish: getting power state of "{0}"...'.format( connection_paramaters[ 'ip_address' ] ) )

  client = RedFishClient( connection_paramaters )

  curent_state = client.getPower()
  return { 'state': curent_state }
