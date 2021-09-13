import logging
import time
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import packet

from subcontractor.credentials import getCredentials

POLL_DELAY = 10
TASK_WAIT_COUNT = 30


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


def _get_virtual_network_map( manager, project_id ):
  result = {}

  uuid_list = manager.list_vlans( project_id )
  for uuid in uuid_list:
    data = manager.call_api( 'virtual-networks/{0}'.format( uuid ), type='GET' )
    result[ uuid.id ] = { 'name': data[ 'description' ], 'vlan': data[ 'vxlan' ] }

  return result


def _ip_config( address ):
  if address is None:
    return '''        dhcp4: no
'''

  if address[ 'address' ] == 'dhcp':
    return '''        dhcp4: yes
'''

  result = '''        dhcp4: no
        addresses: [ {0}/{1} ]
'''.format( address[ 'address' ], address[ 'prefix' ] )

  if address[ 'gateway' ]:
    result += '''        gateway4: {0}
'''.format( address[ 'gateway' ] )

  return result


def create( paramaters ):
  device_paramaters = paramaters[ 'device' ]
  connection_paramaters = paramaters[ 'connection' ]
  device_description = device_paramaters[ 'description' ]

  logging.info( 'packet: creating device "{0}"'.format( device_description ) )
  manager = _connect( connection_paramaters )

  port_map = device_paramaters[ 'port_map' ]
  address_map = device_paramaters[ 'address_map' ]
  iface_name_physical_map = dict( [ ( j[ 'name' ], i ) for i, j in port_map.items() ] )
  bonded_interfaces = [ j for i in port_map.values() if i.get( 'interface_list', False ) for j in i[ 'interface_list' ] ]

  part_list = []  # we are using the same general logic as cloud-init, if these old mime finctions get removed, go see what cloud-init is doing

  cloud_config = '''#cloud-config

system_info:
  network:
    version: 2
    ethernets:'''

  for iface_name, address_list in address_map.items():
    address = None
    if iface_name in bonded_interfaces:
      address = None
    elif address_list and address_list[0].get( 'vlan', None ) is not None:
      address = address_list[0]
    elif port_map[ iface_name_physical_map[ iface_name ] ][ 'network' ] == 'public':
      address = { 'address': 'dhcp' }

    cloud_config += '''
      {0}:
{1}
'''.format( iface_name, _ip_config( address ) )

  cloud_config += '''
    bonds:'''
  for iface_name, iface in port_map.items():
    address = None
    if address_list and address_list[0].get( 'vlan', None ) is not None:
      address = address_list[0]
    elif port_map[ iface_name_physical_map[ iface_name ] ][ 'network' ] == 'public':
      address = { 'address': 'dhcp' }

    if 'interface_list' in iface:
      cloud_config += '''
      {0}:
        interfaces: [ {1} ]
{2}
        parameters:
          mode: 802.3ad
          down-delay: 200
          up-delay: 200
          lacp-rate: fast
          transmit-hash-policy: layer3+4
          mii-monitor-interval: 100
'''.format( iface_name, ', '.join( iface[ 'interface_list' ] ), _ip_config( address ) )

  cloud_config += '''
    vlans:'''
  for iface_name, address_list in address_map.items():
    for address in address_list:
      if address[ 'vlan' ]:
        cloud_config += '''
        {0}_{1}:
          link: {0}
          id: {1}
{2}
'''.format( iface_name, address[ 'vlan' ], _ip_config( address ) )

  part = MIMEText( cloud_config, 'cloud-config', 'ascii' )  # ascii so it won't think the utf-8 needs to base64 encoded
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

  for i in range( 0, TASK_WAIT_COUNT ):
    time.sleep( POLL_DELAY )
    device = manager.get_device( device_uuid )
    if device.state != 'queued':
      break

    logging.debug( 'packet: waiting for device "{0}"({1}) to start provisioning, {2} of {3}...'.format( device_description, device_uuid, i, TASK_WAIT_COUNT ) )

  else:
    Exception( 'Timeout waiting for device "{0}" to start provisioning'.format( device_description ) )

  virtual_network_map = _get_virtual_network_map( manager, device_paramaters[ 'project' ] )
  vlan_network_id_map = dict( [ ( v[ 'vlan' ], k ) for k, v in virtual_network_map.items() ] )
  name_network_id_map = dict( [ ( v[ 'name' ], k ) for k, v in virtual_network_map.items() ] )
  network_id_name_map = dict( [ ( k, v[ 'name' ] ) for k, v in virtual_network_map.items() ] )
  network_id_name_map[ None ] = 'public'
  device = manager.get_device( device_uuid )

  # manager.disbond_ports( network[ 'id' ], False )
  # manager.assign_port( network[ 'id' ], name_network_id_map[ network_name ] )
  # manager.assign_native_vlan( port_id, vnid )
  # manager.convert_layer_2( port_id, vlan_id )

  port_network_map = dict( [ ( i[ 'name' ], { "id": i[ 'id' ], 'native': i[ 'native_virtual_network' ], 'tagged': i[ 'virtual_networks' ] } ) for i in device.network_ports ] )

  for iface_name, port in port_map.items():
    if iface_name_physical_map[ port[ 'name' ] ] in bonded_interfaces:  # for now we are just going to ignore these
      continue

    if iface_name not in port_network_map:  # TODO: do we throw something for this?
      continue

    if network_id_name_map[ port_network_map[ iface_name ][ 'native' ] ] != port[ 'network' ]:
      manager.assign_native_vlan( port_network_map[ iface_name ][ 'id' ], name_network_id_map[ port[ 'network' ] ] )

    curent_tagged_vlans = set( [ virtual_network_map[ i ][ 'vlan' ] for i in port_network_map[ iface_name ][ 'tagged' ] ] )
    target_tagged_vlans = set( port[ 'tagged_vlans' ] )
    vlan_assignments = []
    for vlan in curent_tagged_vlans - target_tagged_vlans:
      vlan_assignments.append( { 'vlan': vlan_network_id_map[ vlan ], 'state': 'unassigned' } )

    for vlan in target_tagged_vlans - curent_tagged_vlans:
      vlan_assignments.append( { 'vlan': vlan_network_id_map[ vlan ], 'native': False, 'state': 'assigned' } )

    if vlan_assignments:
      manager.call_api( '/ports/{0}/vlan-assignments/batches'.format( port_network_map[ iface_name ][ 'id' ] ), type='POST', params={ 'vlan_assignments': vlan_assignments } )
      for i in range( 0, TASK_WAIT_COUNT ):
        time.sleep( POLL_DELAY )
        ready = True
        for assignment in manager.call_api( '/ports/{0}/vlan-assignments/batches'.format( port_network_map[ iface_name ][ 'id' ] ), type='GET' )[ 'batches' ]:
          ready = ready & ( assignment[ 'state' ] == 'completed' )
        if ready:
          break

        logging.debug( 'packet: waiting for vlan assignments "{0}"({1}) to start finish, {2} of {3}...'.format( device_description, device_uuid, i, TASK_WAIT_COUNT ) )

      else:
        Exception( 'Timeout waiting for vlan assignments "{0}" to finish'.format( device_description ) )

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

  for i in range( 0, TASK_WAIT_COUNT ):
    time.sleep( POLL_DELAY )
    logging.debug( 'packet: checking to see if "{0}" is destroyed, check {1} of {2}...'.format( device_description, i, TASK_WAIT_COUNT ) )
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
