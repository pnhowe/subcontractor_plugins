import logging
import time
import random
from datetime import datetime, UTC

from proxmoxer import ProxmoxAPI

from subcontractor.credentials import getCredentials

POLL_INTERVAL = 4


class MOBNotFound( Exception ):
  pass


def _connect( connection_paramaters ):
  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  logging.debug( 'proxmox: connecting to "{0}" with user "{1}"'.format( connection_paramaters[ 'host' ], creds[ 'username' ] ) )
  return ProxmoxAPI( connection_paramaters[ 'host' ], user=creds[ 'username' ], password=creds[ 'password' ], verify_ssl=False )


def _taskWait( node, task_id ):
  while True:
    task = node.tasks( task_id ).status.get()
    print( task )

    if task[ 'status' ] != 'running':
      return

    logging.debug( 'proxmox: Waiting pid: "{0}" age: "{1}"...'.format( task[ 'pid' ], int( datetime.now( UTC ).timestamp() ) - task[ 'starttime' ] ) )

    time.sleep( POLL_INTERVAL )


# for now just pick the first one
def _getNode( proxmox ):
  nodes = proxmox.nodes.get()
  return proxmox.nodes( nodes[0][ 'node' ] )


def _getVM( node, vm_id ):
  return node.qemu( vm_id )


def create( paramaters ):  # NOTE: the picking of the cluster/host and datastore should be done prior to calling this, that way rollback can know where it's at
  vm_paramaters = paramaters[ 'vm' ]
  connection_paramaters = paramaters[ 'connection' ]
  vm_name = vm_paramaters[ 'name' ]

  # Use Locally Administered Addresses (LAA): Use addresses starting with 02, 06, 0A, or 0E (e.g., 02:XX:XX:XX:XX:XX)
  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    mac = '02{0:010x}'.format( random.randint( 0, 268435455 ) )  # TODO: check to see if the mac is allready in use, also make them sequential
    vm_paramaters[ 'interface_list' ][ i ][ 'mac' ] = ':'.join( mac[ x:x + 2 ] for x in range( 0, 12, 2 ) )

  logging.info( 'proxmox: creating vm "{0}"'.format( vm_name ) )
  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )

  params = {
            'name': vm_name,
            'sockets': vm_paramaters[ 'cpu_count' ],
            'cores': 1,
            'cpu': 'x86-64-v2-AES',
            'memory': vm_paramaters[ 'memory_size' ],  # in MiB
            'ostype': 'l26',
            'scsihw': 'virtio-scsi-single'
            # 'smbios1': 'uuid=e8f68186-1742-41af-befe-0e32822b0f31',
          }

  params[ 'boot' ] = 'order={0}'.format( ';'.join( vm_paramaters[ 'boot_order' ] ) )

  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    interface = vm_paramaters[ 'interface_list' ][ i ]
    params[ 'net{0}'.format( i ) ] = 'virtio,bridge={0},macaddr={1}'.format(
                                      interface[ 'network' ],
                                      interface[ 'mac' ]
                                    )

  # as late as possible, just to help make sure our number is good
  vm_id = proxmox.cluster.nextid.get()

  for i in range( 0, len( vm_paramaters[ 'disk_list' ] ) ):
    disk = vm_paramaters[ 'disk_list' ][ i ]
    # params[ 'scsi{0}'.format( i ) ] = 'local-lvm:vm-{0}-{1}:size={2}G'.format( vm_id, disk[ 'name' ], disk[ 'size' ] )
    params[ 'scsi{0}'.format( i ) ] = 'local-lvm:{0}'.format( disk[ 'size' ] )  # in G

  params[ 'vmid' ] = vm_id

  logging.debug( 'proxmox: vm params {0}'.format( params ) )
  task_id = node.qemu().post( **params )
  _taskWait( node, task_id )

  logging.info( 'proxmox: vm "{0}" created, id: "{1}"'.format( vm_name, vm_id ) )

  return { 'done': True, 'id': vm_id }


def create_rollback( paramaters ):
  vm_paramaters = paramaters[ 'vm' ]
  connection_paramaters = paramaters[ 'connection' ]
  vm_name = vm_paramaters[ 'name' ]
  logging.info( 'proxmox: rolling back vm "{0}"'.format( vm_name ) )

  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )
  vm_id = 101

  vm = _getVM( node, vm_id )
  task_id = vm.delete( purge=1 )
  _taskWait( node, task_id )

  return { 'rollback_done': True }


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_id = paramaters[ 'id' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'proxmox: destroying vm "{0}"({1})'.format( vm_name, vm_id ) )
  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )

  vm = _getVM( node, vm_id )
  task_id = vm.delete( purge=1 )
  _taskWait( node, task_id )

  logging.info( 'proxmox: vm "{0}"({1}) destroyed'.format( vm_name, vm_id ) )
  return { 'done': True }


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_id = paramaters[ 'id' ]
  vm_name = paramaters[ 'name' ]
  interface_list = []

  logging.info( 'proxmox: getting interface map "{0}"({1})'.format( vm_name, vm_id ) )
  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )
  vm = _getVM( node, vm_id )

  config = vm.config.get()
  for k, v in config.items():
    if k.startswith( 'net' ):
      item_list = [ i for i in v.split( ',' ) if i.startswith( 'virtio=' ) ]  # the docs say this is suposed to 'macaddr='
      if len( item_list ) != 1:
        print( '"{0}": "{1}"'.format( k, v ) )
        raise Exception( 'proxmox: unable to find mac address for interface "{0}"'.format( k ) )

      interface_list.append( item_list[0].split( '=' )[1] )

  return { 'interface_list': interface_list }


def _power_state_convert( state ):
  if state == 'stopped':
    return 'off'

  elif state == 'running':
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_id = paramaters[ 'id' ]
  vm_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]

  logging.info( 'proxmox: setting power state of "{0}"({1}) to "{2}"...'.format( vm_name, vm_id, desired_state ) )
  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )
  vm = _getVM( node, vm_id )

  curent_state = _power_state_convert( vm.status.current.get()[ 'status' ] )

  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  task_id = None
  if desired_state == 'on':
    task_id = vm.status.start.post()
  elif desired_state == 'off':
    task_id = vm.status.stop.post()
  elif desired_state == 'soft_off':
    task_id = vm.status.shutdown.post()

  if task_id is not None:
    _taskWait( node, task_id )

  vm = _getVM( node, vm_id )
  logging.info( 'proxmox: setting power state of "{0}"({1}) to "{2}" complete'.format( vm_name, vm_id, desired_state ) )
  return { 'state': _power_state_convert( vm.status.current.get()[ 'status' ] ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_id = paramaters[ 'id' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'proxmox: getting "{0}"({1}) power state...'.format( vm_name, vm_id ) )
  proxmox = _connect( connection_paramaters )
  node = _getNode( proxmox )
  vm = _getVM( node, vm_id )

  return { 'state': _power_state_convert( vm.status.current.get()[ 'status' ] ) }
