import logging
import time
import ssl

import packet

from subcontractor.credentials import getCredentials

POLL_DELAY = 5


def _connect( connection_paramaters ):
  # work arround invalid SSL
  _create_unverified_https_context = ssl._create_unverified_context
  ssl._create_default_https_context = _create_unverified_https_context
  # TODO: flag for trusting SSL of connection, also there is a paramater to Connect for verified SSL

  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  # TODO: saninity check on creds

  return packet.Manager( auth_token=creds )


def create( paramaters ):
  device_paramaters = paramaters[ 'device' ]
  connection_paramaters = paramaters[ 'connection' ]
  device_description = device_paramaters[ 'description' ]

  logging.info( 'packet: creating device "{0}"'.format( device_description ) )
  manager = _connect( connection_paramaters )

  data = {
           'facility': device_paramaters[ 'facility' ],
           'plan': device_paramaters[ 'plan' ],
           'hostname': device_paramaters[ 'hostname' ],
           'operating_system': device_paramaters[ 'operating_system' ]
         }

  device = manager.create_device( **data )

  for i in range( 0, 20 ):
    time.sleep( POLL_DELAY )
    device = manager.get_device( device.id )
    logging.debug( 'packet: created device "{0}" is curently "{1}" waiting for "active", check {2} of 20...'.format( device_description, device.state, i ) )

  logging.info( 'packet: device "{0}" created, uuid: "{1}"'.format( device_description, device.id ) )

  return { 'done': True, 'uuid': device.id }


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]

  logging.info( 'packet: destroying vm "{0}"({1})'.format( device_description, device_uuid ) )
  manager = _connect( connection_paramaters )

  try:
      device = manager.get_device( device_uuid )
  except packet.ResponseError:
    return { 'done': True }  # it's gone, we are done

  device.delete()

  for i in range( 0, 20 ):
    time.sleep( POLL_DELAY )
    logging.debug( 'packet: checking to see if "{0}" is destroyed, check {1} of 20...'.format( device_description, i ) )
    try:
      manager.get_device( device_uuid )
    except packet.ResponseError:
      logging.info( 'packet: device "{0}" destroyed'.format( device_description ) )
      return { 'done': True }

  raise Exception( 'Timeout waiting for device "{0}" to delete'.format( device_description ) )


def _power_state_convert( state ):
  if state in ( 'inactive', 'queued', 'provisioning' ):
    return 'off'

  elif state in ( 'active', ):
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]
  desired_state = paramaters[ 'state' ]

  logging.info( 'packet: setting power state of "{0}"({1}) to "{2}"...'.format( device_description, device_uuid, desired_state ) )
  manager = _connect( connection_paramaters )
  device = manager.get_device( device_uuid )

  curent_state = _power_state_convert( device.state )
  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  if desired_state == 'on':
    device.action( 'power_on' )
  elif desired_state in ( 'off', 'soft_off' ):
    device.action( 'power_off' )

  time.sleep( POLL_DELAY )  # give the device the chance to do something

  device = manager.get_device( device_uuid )
  logging.info( 'packet: setting power state of "{0}"({1}) to "{2}" complete'.format( device_description, device_uuid, desired_state ) )
  return { 'state': _power_state_convert( device.state ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]

  logging.info( 'packat: getting "{0}"({1}) power state...'.format( device_description, device_uuid ) )
  manager = _connect( connection_paramaters )
  device = manager.get_device( device_uuid )
  return { 'state': _power_state_convert( device.state ) }
