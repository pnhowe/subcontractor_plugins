import logging
import time
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import packet

from subcontractor.credentials import getCredentials

POLL_DELAY = 10


def _connect( connection_paramaters ):
  # work arround invalid SSL
  _create_unverified_https_context = ssl._create_unverified_context
  ssl._create_default_https_context = _create_unverified_https_context
  # TODO: flag for trusting SSL of connection, also there is a paramater to Connect for verified SSL

  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  # TODO: saninity check on creds

  return packet.Manager( auth_token=creds[ 'token' ] )


def _get_vlan_map( manager, project_id ):
  result = {}

  uuid_list = manager.list_vlans( project_id )
  for uuid in uuid_list:
    data = manager.call_api( 'virtual-networks/{0}'.format( uuid ), type='GET' )
    result[ data[ 'description' ] ] = uuid.id

  return result


def create( paramaters ):
  device_paramaters = paramaters[ 'device' ]
  connection_paramaters = paramaters[ 'connection' ]
  device_description = device_paramaters[ 'description' ]

  logging.info( 'packet: creating device "{0}"'.format( device_description ) )
  manager = _connect( connection_paramaters )

  interface_map = device_paramaters[ 'interface_map' ]
  for interface in list( interface_map.values() ):
    interface_map[ interface[ 'physical_location' ] ] = interface

  part_list = []  # we are using the same general logic as cloud-init, if these old mime finctions get removed, go see what cloud-init is doing

  if 'eno2' in interface_map and interface_map[ 'eno2' ][ 'address_list' ]:
    content = '''#cloud-config

system_info:
  network:
    version: 2
    ethernets:
      eno1:
        dhcp4: no
        addresses: [{0}]
        gateway4: {1}
      eno2:
        dhcp4: no
        addresses: [{0}]
        gateway4: {1}
'''.format( interface_map[ 'eno2' ][ 'address_list' ][0][ 'address' ], interface_map[ 'eno2' ][ 'address_list' ][0][ 'gateway' ] )

    part = MIMEText( content, 'cloud-config', 'ascii' )  # ascii so it won't think the utf-8 needs to base64 encoded
    part.add_header( 'Content-Disposition', 'attachment; filename="network-config"' )
    part_list.append( part )

  data = {
           'project_id': device_paramaters[ 'project' ],
           'hostname': device_paramaters[ 'hostname' ],
           'plan': device_paramaters[ 'plan' ],
           'facility': device_paramaters[ 'facility' ],
           'operating_system': device_paramaters[ 'operating_system' ],
           # 'description': device_paramaters[ 'description' ]
         }

  if part_list:
    user_data = MIMEMultipart()
    for part in part_list:
      user_data.attach( part )

    data[ 'userdata' ] = user_data.as_string()

  device = manager.create_device( **data )
  device_uuid = device.id

  logging.info( 'packet: device "{0}" created, uuid: "{1}"'.format( device_description, device_uuid ) )

  # while True:
  #   device = manager.get_device( device_uuid )
  #   if device.state != 'queued':
  #     break
  #
  #   logging.debug( 'packet: waiting for device "{0}"({1}) to start provisioning...'.format( device_description, device_uuid ) )
  #   time.sleep( 1 )
  #
  # vlan_map = _get_vlan_map( manager, device_paramaters[ 'project' ] )
  # device = manager.get_device( device_uuid )
  # for network in device.network_ports:
  #   iface_name = network[ 'name' ]
  #
  #   if iface_name in interface_map:
  #     network_name = interface_map[ iface_name ][ 'network' ]
  #
  #     if network_name == 'public' or network_name not in vlan_map:
  #       continue
  #
  #     manager.disbond_ports( network[ 'id' ], False )
  #     manager.assign_port( network[ 'id' ], vlan_map[ network_name ] )

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
    device.power_on()
  elif desired_state in ( 'off', 'soft_off' ):
    device.power_off()

  time.sleep( POLL_DELAY )  # give the device the chance to do something

  device = manager.get_device( device_uuid )
  logging.info( 'packet: setting power state of "{0}"({1}) to "{2}" complete'.format( device_description, device_uuid, desired_state ) )
  return { 'state': _power_state_convert( device.state ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]

  logging.info( 'packat: getting "{0}"({1}) device power state...'.format( device_description, device_uuid ) )
  manager = _connect( connection_paramaters )
  device = manager.get_device( device_uuid )
  return { 'state': _power_state_convert( device.state ) }


def device_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]

  logging.info( 'packat: getting "{0}"({1}) device state...'.format( device_description, device_uuid ) )
  manager = _connect( connection_paramaters )
  device = manager.get_device( device_uuid )
  return { 'state': device.state }


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  device_uuid = paramaters[ 'uuid' ]
  device_description = paramaters[ 'description' ]

  logging.info( 'packat: getting "{0}"({1}) device ip adresses...'.format( device_description, device_uuid ) )
  manager = _connect( connection_paramaters )
  device = manager.get_device( device_uuid )

  result = dict( ( i[ 'id' ], { 'name': i[ 'name' ], 'mac': i[ 'data' ].get( 'mac', None ), 'ip_addresses': [] } ) for i in device.network_ports )

  for ip_address in device.ip_addresses:
    result[ ip_address[ 'interface' ][ 'href' ].split( '/' )[2] ][ 'ip_addresses' ].append( { 'address': ip_address[ 'address' ], 'gateway': ip_address[ 'gateway' ], 'public': ip_address[ 'public' ] } )

  return { 'interface_map': result }
