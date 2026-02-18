import logging
import asyncio
import os
import random

from subcontractor.credentials import getCredentials
from subcontractor_plugins.virtualbox import constants
from subcontractor_plugins.virtualbox.client import VirtualBox, VirtualBoxNotFound

CREATE_GROUP = ''
CREATE_GROUPS = []
CREATE_FLAGS = ''

BOOT_ORDER_MAP = { 'hdd': constants.DeviceType.HardDisk, 'net': constants.DeviceType.Network, 'cd': constants.DeviceType.DVD, 'usb': constants.DeviceType.USB }


def _connect( connection_paramaters ):
  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  logging.debug( 'virtualbox: connecting to "{0}" with user "{1}"'.format( connection_paramaters[ 'host' ], creds[ 'username' ] ) )

  return VirtualBox( 'http://{0}:18083/'.format( connection_paramaters[ 'host' ] ), creds[ 'username' ], creds[ 'password' ] )


def create( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_paramaters = paramaters[ 'vm' ]
  vm_name = vm_paramaters[ 'name' ]

  # virtualbox static mac are 08:00:27:00:00:00 -> 08:00:27:FF:FF:FF
  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    mac = '080027{0:06x}'.format( random.randint( 0, 16777215 ) )  # TODO: check to see if the mac is allready in use, also make them sequential
    vm_paramaters[ 'interface_list' ][ i ][ 'mac' ] = ':'.join( mac[ x:x + 2 ] for x in range( 0, 12, 2 ) )

  logging.info( 'virtualbox: creating vm "{0}"'.format( vm_name ) )
  vbox = _connect( connection_paramaters )

  settings_file = vbox.compose_machine_filename( vm_name, CREATE_GROUP, CREATE_FLAGS, vbox.system_properties[ 'default_machine_folder' ] )
  vm = vbox.create_machine( settings_file, vm_name, CREATE_GROUPS, vm_paramaters[ 'guest_type' ], CREATE_FLAGS )
  vm.RTC_use_UTC = True
  vm.memory_size = vm_paramaters[ 'memory_size' ]  # in MiB

  disk_controller_name = 'SCSI'
  vm.add_storage_controller( disk_controller_name, constants.StorageBus.SCSI )
  cd_controller_name = 'SATA'
  vm.add_storage_controller( cd_controller_name, constants.StorageBus.SATA )

  vm.save_settings()
  logging.debug( 'virtualbox: regestering vm "{0}"'.format( vm_name ) )
  vbox.register_machine( vm )

  vm.lock( vbox.session, constants.LockType.Write )
  try:
    vm2 = vbox.session.machine

    for i in range( 0, vbox.system_properties[ 'max_boot_position' ] ):
      vm2.set_boot_order( i + 1, constants.DeviceType.Null )

    for i in range( 0, 4 ):
      adapter = vm2.get_network_adapter( i )
      adapter.enabled = False

    disk_port = 0
    cd_port = 0
    for disk in vm_paramaters[ 'disk_list' ]:
      disk_name = disk[ 'name' ]
      logging.debug( 'vritualbox: creating disk "{0}" on "{1}"'.format( disk_name, vm_name ) )
      if 'file' in disk:
        disk_file = disk[ 'file' ]

        if disk_file.endswith( '.iso' ):
          medium = vbox.open_medium( disk_file, constants.DeviceType.DVD, constants.AccessMode.ReadOnly, True )
          vm2.attach_device( cd_controller_name, cd_port, 0, constants.DeviceType.DVD, medium )
          cd_port += 1

        else:
          medium = vbox.open_medium( disk_file, constants.DeviceType.HardDisk, constants.AccessMode.ReadWrite, True )
          vm2.attach_device( disk_controller_name, disk_port, 0, constants.DeviceType.HardDisk, medium )
          disk_port += 1

      else:
        disk_size = disk.get( 'size', 10 ) * 1024 * 1024 * 1024  # disk_size is in bytes, we were passed in GiB
        disk_format = 'vdi'
        location = '{0}/{1}.vdi'.format( os.path.dirname( vm.settings_file_path ), disk_name )
        medium = vbox.create_medium( disk_format, location, constants.AccessMode.ReadWrite, constants.DeviceType.HardDisk )
        progress = medium.create_base_storage( disk_size, [ constants.MediumVariant.Standard ] )
        while not progress.completed:
          logging.debug( 'virtualbox: creating storage for "{0}" disk "{1}" at {2}%, {3} seconds left'.format( vm_name, disk_name, progress.percent, progress.time_remaining ) )
          asyncio.sleep( 1 )

        if medium.state != constants.MediumState.Created:
          raise Exception( 'disk "{0}" for vm "{1}" faild to create: "{2}"'.format( disk_name, vm_name, progress.error_info[ 'text' ] ) )

        vm2.attach_device( disk_controller_name, disk_port, 0, constants.DeviceType.HardDisk, medium )
        disk_port += 1

    for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
      interface = vm_paramaters[ 'interface_list' ][ i ]
      adapter = vm2.get_network_adapter( interface[ 'index' ] )

      try:
        adapterType = constants.NetworkAdapterType.__dict__[ interface.get( 'adapter_type', 'I82540EM' ) ]
      except KeyError:
        raise ValueError( 'Unknown adapter type "{0}"'.format( interface[ 'adapter_type' ] ) )

      adapter.enabled = True
      adapter.adapter_type = adapterType
      adapter.mac_address = interface[ 'mac' ].replace( ':', '' )

      if interface[ 'type' ] == 'host':
        adapter.attachment_type = constants.NetworkAttachmentType.HostOnly
        adapter.host_only_interface = interface[ 'network' ]

      elif interface[ 'type' ] == 'bridge':
        adapter.attachment_type = constants.NetworkAttachmentType.Bridged
        adapter.bridged_interface = interface[ 'network' ]

      elif interface[ 'type' ] == 'nat':
        adapter.attachment_type = constants.NetworkAttachmentType.NATNetwork
        adapter.nat_network = interface[ 'network' ]

      elif interface[ 'type' ] == 'internal':
        adapter.attachment_type = constants.NetworkAttachmentType.Internal
        adapter.internal_network = interface[ 'network' ]

      else:
        raise Exception( 'Unknown interface type "{0}"'.format( interface[ 'type' ] ) )

    for i in range( 0, vbox.system_properties[ 'max_boot_position' ] ):
      if i < len( vm_paramaters[ 'boot_order' ] ):
        try:
          vm2.set_boot_order( i + 1, BOOT_ORDER_MAP[ vm_paramaters[ 'boot_order' ][ i ] ] )
        except KeyError:
          raise Exception( 'Unknown boot item "{0}"'.format( vm_paramaters[ 'boot_order' ][ i ] ) )

    vm2.save_settings()

  finally:
    vbox.session.unlock_machine()

  logging.info( 'virtualbox: vm "{0}" created'.format( vm_name ) )

  return { 'done': True, 'uuid': vm.hardware_uuid }


def create_rollback( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_paramaters = paramaters[ 'vm' ]
  vm_name = vm_paramaters[ 'name' ]
  logging.info( 'virtualbox: rolling back vm "{0}"'.format( vm_name ) )
  vbox = _connect( connection_paramaters )

  try:
    vm = vbox.find_machine( vm_name )
  except VirtualBoxNotFound:
    vm = None

  if vm is not None:
    media = vm.unregister( constants.CleanupMode.DetachAllReturnHardDisksOnly )
    progress = vm.delete_config( media )
    while not progress.completed:
      logging.debug( 'virtualbox: deleting config "{0}" power off at {1}%, {2} seconds left'.format( vm_name, progress.percent, progress.time_remaining ) )
      asyncio.sleep( 1 )

  # make a list of files that needs to be cleaned up, just incase they are created an not attached, or vm wasn't registerd
  file_list = [ vbox.compose_machine_filename( vm_name, CREATE_GROUP, CREATE_FLAGS, vbox.system_properties[ 'default_machine_folder' ] ) ]

  for disk in vm_paramaters[ 'disk_list' ]:
    disk_name = disk[ 'name' ]
    if 'file' not in disk:
      file_list.append( '{0}/{1}.vdi'.format( os.path.dirname( file_list[0] ), disk_name ) )

  logging.debug( 'virtualbox: rollback cleanup file list "{0}"'.format( file_list ) )
  for file_name in file_list:
    try:
      os.unlink( file_name )
    except OSError as e:
      if e.errno != 2:  # no such file or directory
        raise e

  # would be nice to clean up temp files and dirs, but really don't know what is safe,
  # this is rollback anyway, hopfully it get's created right  the next time and everything
  # get's cleaned up anyway.

  logging.info( 'virtualbox: vm "{0}" rolledback'.format( vm_name ) )
  return { 'rollback_done': True }


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  logging.info( 'virtualbox: destroying vm "{0}"({1})'.format( vm_name, vm_uuid ) )
  vbox = _connect( connection_paramaters )

  try:
    vm = vbox.find_machine( vm_uuid )
  except VirtualBoxNotFound:
    return { 'done': True }  # it's gone, we are donne

  media = vm.unregister( constants.CleanupMode.DetachAllReturnHardDisksOnly )
  progress = vm.delete_config( media )
  while not progress.completed:
    logging.debug( 'virtualbox: deleting config "{0}"({1}) at {2}%, {3} seconds left'.format( vm_name, vm_uuid, progress.percent, progress.time_remaining ) )
    asyncio.sleep( 1 )

  logging.info( 'virtualbox: vm "{0}" destroyed'.format( vm_name ) )
  return { 'done': True }


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  interface_list = []
  logging.info( 'virtualbox: getting interface map "{0}"({1})'.format( vm_name, vm_uuid ) )
  vbox = _connect( connection_paramaters )

  vm = vbox.find_machine( vm_name )

  for i in range( 0, 4 ):
    adapter = vm.get_network_adapter( i )
    if not adapter.enabled:  # stop after the first disabled one
      break

    interface_list.append( adapter.mac_address )

  return { 'interface_list': interface_list }


def _power_state_convert( state ):
  if state in ( constants.MachineState.PoweredOff, constants.MachineState.Saved ):
    return 'off'

  elif state in ( constants.MachineState.Running, constants.MachineState.Starting, constants.MachineState.Stopping ):
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]
  logging.info( 'virtualbox: setting power state of "{0}"({1}) to "{2}"...'.format( vm_name, vm_uuid, desired_state ) )
  vbox = _connect( connection_paramaters )

  vm = vbox.find_machine( vm_name )

  curent_state = _power_state_convert( vm.state )
  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  progress = None
  if desired_state == 'on':
    progress = vm.launch_vm_process( vbox.session )

  elif desired_state == 'off':
    vm.lock( vbox.session, constants.LockType.Shared )
    try:
      vbox.session.machine.power_down( vbox.session )
    finally:
      vbox.session.unlock_machine()

  elif desired_state == 'soft_off':
    vm.lock( vbox.session, constants.LockType.Shared )
    try:
      vbox.session.machine.power_button( vbox.session )
    finally:
      vbox.session.unlock_machine()

  else:
    raise Exception( 'Unknown desired state "{0}"'.format( desired_state ) )

  if progress is not None:
    while not progress.completed:
      logging.debug( 'virtualbox: vm "{0}"({1}) power "{2}" at {3}%, {4} seconds left'.format( vm_name, vm_uuid, desired_state, progress.percent, progress.time_remaining ) )
      asyncio.sleep( 1 )

  logging.info( 'virtualbox: setting power state of "{0}"({1}) to "{2}" complete'.format( vm_name, vm_uuid, desired_state ) )
  return { 'state': _power_state_convert( vm.state ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  logging.info( 'virtualbox: getting "{0}"({1}) power state...'.format( vm_name, vm_uuid ) )
  vbox = _connect( connection_paramaters )

  vm = vbox.find_machine( vm_uuid )

  return { 'state': _power_state_convert( vm.state ) }
