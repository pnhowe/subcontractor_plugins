import logging
import time
import json
from urllib import request

from subcontractor.credentials import getCredentials


class RedFishClient():
  def __init__( self, connection_paramaters ):
    super().__init__()
    self.ip_address = connection_paramaters[ 'ip_address' ]
    creds = connection_paramaters[ 'credentials' ]
    if isinstance( creds, str ):
      creds = getCredentials( creds )

    self.username = creds[ 'username' ]
    self.password = creds[ 'password' ]
    self.chassis = None

  def _get( self, url ):
    logging.debug( 'RedFish: GETing "{0}" from "{1}"'.format( url, self.ip_address ) )
    result = request.urlopen( 'http://{0}:5000{1}'.format( self.ip_address, url ) )
    return json.load( result )

  def _post( self, url, data ):
    logging.debug( 'RedFish: POSTINGing "{0}" to "{1}", data: "{2}"'.format( url, self.ip_address, data ) )
    result = request.urlopen( 'http://{0}:5000{1}'.format( self.ip_address, url ), data )
    return json.load( result )

  def _setChassis( self ):
    result = self._get( '/redfish/v1/Chassis' )
    self.chassis = result[ 'Members' ][0][ '@odata.id' ]

  def getPower( self ):
    if self.chassis is None:
      self._setChassis()

    result = self._get( '{0}/Power'.format( self.chassis ) )

    # Power State Options:
    # "On",
    # "Off"
    # "PoweringOn"
    # "PoweringOff"

    return ( 'on' if result == 'On' else 'off' )

  def setPower( self, state ):
    if self.chassis is None:
      self._setChassis()

    if state not in ( 'on', 'off', 'shutdown', 'cycle', 'reset' ):
      raise ValueError( 'Unknown power state "{0}"'.format( state ) )

      # ComputerSystem - for ACPI
      # Resources ( hard power off?)

    self._post( '{0}/Power'.format( self.chassis ), {} )


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
