import logging
import time
import re

from proxmoxer import ProxmoxAPI

from subcontractor.credentials import getCredentials

# https://pve.proxmox.com/pve-docs/api-viewer/

POLL_INTERVAL = 4
BOOT_ORDER_MAP = { 'hdd': 'c', 'net': 'n', 'cd': 'd' }

vlaned_network = re.compile( r'^[a-zA-Z0-9][a-zA-Z0-9_\-]*\.[0-9]{1,4}$' )


def _connect( connection_paramaters ):
  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  logging.debug( 'proxmox: connecting to "{0}" with user "{1}"'.format( connection_paramaters[ 'host' ], creds[ 'username' ] ) )

  return ProxmoxAPI( connection_paramaters[ 'host' ], user=creds[ 'username' ], password=creds[ 'password' ], verify_ssl=False )  # TODO: flag to toggle verify_ssl


def _disconnect( proxmox ):
  pass
  # proxmox.release_ticket()  # TODO: what do we do to logout?, have to add the try/finally everywhere _disconnect is called


def _taskWait( node, taskid ):
  while True:
    status = node.tasks( taskid ).status.get()
    if status[ 'status' ] != 'running':
      return status[ 'exitstatus' ]

    logging.debug( 'proxmox: Waiting ...' )

    time.sleep( POLL_INTERVAL )


def _get_vm( proxmox, vmid ):
  vmid = str( vmid )
  for node in proxmox.nodes.get():
    node = proxmox.nodes( node[ 'node' ] )
    for vm in node.qemu.get():
      if vm[ 'vmid' ] == vmid:
        return node, node.qemu( vmid )

  return None, None


def create( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_paramaters = paramaters[ 'vm' ]
  vm_vmid = vm_paramaters[ 'vmid' ]
  vm_name = vm_paramaters[ 'name' ]

  logging.info( 'proxmox: creating vm "{0}"'.format( vm_name ) )
  proxmox = _connect( connection_paramaters )

  node = proxmox.nodes( vm_paramaters[ 'node' ] )
  network_list = []
  for network in node.network.get():
    if 'bridge' in network[ 'type' ].lower():
      network_list.append( network[ 'iface' ] )

  # TODO: let harddrive creation be raw or qcow2
  # TODO: need network boot order, or just the provisioning interface
  for interface in vm_paramaters[ 'interface_list' ]:
    network_name = interface[ 'network' ]
    if vlaned_network.match( network_name ):
      network_name, _ = network_name.split( '.' )

    if network_name not in network_list:
      raise ValueError( 'Network "{0}" not aviable on node "{1}"'.format( network_name, vm_paramaters[ 'node' ] ) )

  spec = {
            'vmid': vm_vmid,
            'name': vm_name,
            'ostype': vm_paramaters.get( 'ostype', 'l26' ),
            'memory': vm_paramaters.get( 'memory_size', 512 ),  # in MB
            'sockets': vm_paramaters.get( 'sockets', 1 ),
            'numa': vm_paramaters.get( 'numa', 0 ),
            'cores': vm_paramaters.get( 'core_count', 1 ),
            'boot': ''.join( BOOT_ORDER_MAP[i] for i in vm_paramaters.get( 'boot_order', 'nc' ) ),
            'scsihw': 'virtio-scsi-pci',
            'bootdisk': 'scsi0'
         }

  interface_list = vm_paramaters[ 'interface_list' ]
  interface_list.sort( key=lambda a: a[ 'physical_location' ] )

  for index in range( 0, len( interface_list ) ):
    interface = interface_list[ index ]
    network_name = interface[ 'network' ]
    vlan = ''
    if vlaned_network.match( network_name ):
      network_name, vlan = network_name.split( '.' )
      vlan = ',tag={0}'.format( vlan )

    spec[ 'net{0}'.format( index ) ] = '{0},bridge={1},firewall=0{2}'.format( interface.get( 'type', 'virtio' ), network_name, vlan )

  disk_list = vm_paramaters[ 'disk_list' ]
  disk_list.sort( key=lambda a: a[ 'name' ] )
  for index in range( 0, len( disk_list ) ):
    disk = disk_list[ index ]
    location = 'local'
    # location = 'local-lvm'
    # if disk[ 'type' ] == 'thin':
    #   location = 'local-lvmthin'
    # spec[ 'scsi{0}'.format( index ) ] = '{0}:vm-{1}-{2},size={3}G'.format( location, vm_vmid, disk[ 'name' ], disk.get( 'size', 10 ) )  # if we pre-created the file, make sure delete removes disks if they are manually created
    # file name for generating our selves: Block: vm-<vm id>-<disk name>  Ffilesystem: <vmid>/vm-<vm id>-<disk name>
    spec[ 'scsi{0}'.format( index ) ] = '{0}:{1}'.format( location, disk.get( 'size', 10 ) )  # in GiB

  # have yet to find the log file for the "{data:null}" results, I have found that using `qm create` on the command line helps expose the error, https://pve.proxmox.com/pve-docs/qm.1.html
  taskid = node.qemu.create( **spec )

  if _taskWait( node, taskid ) != 'OK':
    raise Exception( 'Create task failed' )

  return { 'complete': True }


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_vmid = paramaters[ 'vmid' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'proxmox: destroying vm "{0}"({1})'.format( vm_name, vm_vmid ) )
  proxmox = _connect( connection_paramaters )

  node, vm = _get_vm( proxmox, vm_vmid )

  if vm is None:
    return { 'done': True }  # it's gone, we are donne

  taskid = vm.delete()

  if _taskWait( node, taskid ) != 'OK':
    raise Exception( 'Delete task failed' )

  logging.info( 'proxmox: vm "{0}" destroyed'.format( vm_name ) )
  return { 'done': True }


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_vmid = paramaters[ 'vmid' ]
  vm_name = paramaters[ 'name' ]
  interface_list = []
  logging.info( 'proxmox: getting interface map "{0}"({1})'.format( vm_name, vm_vmid ) )
  proxmox = _connect( connection_paramaters )

  _, vm = _get_vm( proxmox, vm_vmid )
  if vm is None:
    raise Exception( 'VM Not Found' )

  config_map = vm.config.get()
  interface_map = {}
  for name, value in config_map.items():
    if name.startswith( 'net' ):
      lines = value.split( ',' )
      ( _, mac ) = lines[0].split( '=' )  # 'net0': 'virtio=A6:EF:6D:0F:F3:7F,bridge=vmbr0,firewall=1',
      interface_map[ name ] = mac

  for name in sorted( interface_map.keys() ):
    interface_list.append( interface_map[ name ] )

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
  vm_vmid = paramaters[ 'vmid' ]
  vm_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]

  logging.info( 'proxmox: setting power state of "{0}"({1}) to "{2}"...'.format( vm_name, vm_vmid, desired_state ) )
  proxmox = _connect( connection_paramaters )

  node, vm = _get_vm( proxmox, vm_vmid )

  if vm is None:
    raise Exception( 'VM Not Found' )

  status = vm.status.current.get()
  curent_state = _power_state_convert( status[ 'status' ] )

  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  taskid = None
  if desired_state == 'on':
    taskid = vm.status.start.post()

  elif desired_state == 'off':
    taskid = vm.status.stop.post()

  elif desired_state == 'soft_off':
    taskid = vm.status.shutdown.post()

  else:
    raise Exception( 'proxmox desired state "{0}"'.format( desired_state ) )

  rc = _taskWait( node, taskid )
  if rc not in ( 'OK', 'VM quit/powerdown failed - got timeout' ):
    logging.debug( 'proxmox: unexpected power control return "{0}"'.format( rc ) )
    raise Exception( 'Power task failed' )

  status = vm.status.current.get()
  logging.info( 'proxmox: setting power state of "{0}"({1}) to "{2}" complete'.format( vm_name, vm_vmid, desired_state ) )
  return { 'state': _power_state_convert( ( status[ 'status' ] ) ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_vmid = paramaters[ 'vmid' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'proxmox: getting "{0}"({1}) power state...'.format( vm_name, vm_vmid ) )
  proxmox = _connect( connection_paramaters )

  _, vm = _get_vm( proxmox, vm_vmid )

  if vm is None:
    raise Exception( 'VM Not Found' )

  status = vm.status.current.get()

  return { 'state': _power_state_convert( status[ 'status' ] ) }


def node_list( paramaters ):
  # returns a list of hosts in a resource
  # host must have paramater[ 'min_memory' ] aviable in MB and at least paramaters[ 'min_cores' ] cpu cores, then
  # each metric is converted to a value 0 -> 1, where 1 is most desired, the * the scaler then summed up, then we
  #  sort by the score and return the list
  connection_paramaters = paramaters[ 'connection' ]
  logging.info( 'proxmox: getting Node List' )
  proxmox = _connect( connection_paramaters )

  node_map = {}
  for node in proxmox.nodes.get():
    if node[ 'status' ] != 'online':
      logging.debug( 'proxmox: node "{0}", not online, status: "{1}"'.format( node[ 'node' ], node[ 'status' ] ) )
      continue

    status = proxmox.nodes( node[ 'node' ] ).status.get()

    vm_count = len( proxmox.nodes( node[ 'node' ] ).qemu.get() )

    total_memory = status[ 'memory' ][ 'total' ] / 1024.0 / 1024.0
    memory_aviable = status[ 'memory' ][ 'free' ] / 1024.0 / 1024.0
    if memory_aviable < paramaters[ 'min_memory' ]:
      logging.debug( 'proxmox: host "{0}", low aviable ram: "{1}"'.format( node[ 'node' ], memory_aviable ) )
      continue

    cpu_utilization_aviable = 1.0 - min( 1.0, status[ 'cpu' ] )
    cpu_aviable = cpu_utilization_aviable * status[ 'cpuinfo' ][ 'cpus' ]
    if cpu_aviable < paramaters[ 'min_cores' ]:
      logging.debug( 'proxmox: host "{0}", low aviable cores: "{1}"'.format( node[ 'node' ], cpu_aviable ) )
      continue

    node_map[ node[ 'node' ] ] = paramaters[ 'scalers' ][ 'memory' ] * ( memory_aviable / total_memory )
    node_map[ node[ 'node' ] ] += paramaters[ 'scalers' ][ 'cpu' ] * cpu_utilization_aviable
    node_map[ node[ 'node' ] ] += paramaters[ 'scalers' ][ 'io' ] * ( 1.0 - min( 1.0, status[ 'wait' ] ) )
    node_map[ node[ 'node' ] ] += paramaters[ 'scalers' ][ 'vm' ] * ( 1.0 / vm_count )

  logging.debug( 'proxmox: node_map {0}'.format( node_map ) )

  result = list( node_map.keys() )
  result.sort( key=lambda a: node_map[ a ], reverse=True )

  return { 'node_list': result }
