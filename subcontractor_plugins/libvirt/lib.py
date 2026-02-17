import logging
import os
import random
import subprocess
import shutil
from xml.etree import ElementTree


import libvirt


DISK_IMAGE_DIR = '/var/lib/libvirt/t3kton'

ENABLE_VNC_CONSOLE = False


def _connect( connection_paramaters ):
  host = connection_paramaters[ 'host' ]
  logging.debug( 'libvirt: connecting to "{0}"'.format( host ) )

  return libvirt.open()  # 'qemu://{0}/system'.format( host ) )


def create( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_paramaters = paramaters[ 'vm' ]
  vm_name = vm_paramaters[ 'name' ]

  # generic static mac are 52:54:00:00:00:00 -> 52:54:00:FF:FF:FF
  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    mac = '525400{0:06x}'.format( random.randint( 0, 16777215 ) )  # TODO: check to see if the mac is allready in use, also make them sequential
    vm_paramaters[ 'interface_list' ][ i ][ 'mac' ] = ':'.join( mac[ x:x + 2 ] for x in range( 0, 12, 2 ) )

  logging.info( 'libvirt: creating vm "{0}"'.format( vm_name ) )
  lvirt = _connect( connection_paramaters )

  vm_disk_dir = os.path.join( DISK_IMAGE_DIR, vm_name )
  os.mkdir( vm_disk_dir )
  disk_list = []
  for disk in vm_paramaters[ 'disk_list' ]:
    disk_name = disk[ 'name' ]
    logging.debug( 'libvirt: creating disk "{0}" on "{1}"'.format( disk_name, vm_name ) )
    if 'file' in disk:
      disk_file = disk[ 'file' ]
    else:
      disk_file = os.path.join( vm_disk_dir, f'{ disk_name }.qcow2' )
      logging.debug( f'libvirt: creating storage for "{ vm_name }" disk "{ disk_name }"...' )
      rc = subprocess.run( ['qemu-img', 'create', '-f', 'qcow2', disk_file, '{0}G'.format( disk.get( 'size', 10 ) ) ], capture_output=True )
      if rc.returncode != 0:
        raise Exception( 'Unable to create disk image for {0}: rc: {1} out: {2} error: {3}'.format( disk_name, rc.returncode, rc.stdout, rc.stderr ) )

    if disk_file.endswith( '.iso' ):
      disk_list.append( f'''
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw' />
      <source file='{ disk_file }' />
    </disk>''' )

    else:
      disk_list.append( f'''
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2' />
      <target dev='sda' bus='scsi' />
      <source file='{ disk_file }' />
    </disk>''' )

  interface_list = []
  for interface in vm_paramaters[ 'interface_list' ]:
    # for a list of supported models: qemu-system-x86_64 -net nic,model=?
    interface_list.append( f'''
      <interface type='bridge'>
        <!-- name: { interface[ 'name' ] } -->
        <source bridge='{ interface[ 'network' ] }' />
        <mac address='{ interface[ 'mac' ] }' />
        <model type='e1000' />  <!-- or virtio -->
      </interface>''')

  boot_list = []
  for dev in vm_paramaters[ 'boot_order' ]:
    boot_list.append( f'    <boot dev=\'{ dev }\'/>' )

  console = '''<console type='pty'/>
'''
  if ENABLE_VNC_CONSOLE:
    console += '''    <graphics type='spice' port='5900' autoport='yes' listen='127.0.0.1'>
      <listen type='address' address='127.0.0.1'/>
      <image compression='off'/>
    </graphics>
    <video>
      <model type='qxl' ram='65536' vram='65536' vgamem='16384' heads='1' primary='yes'/>
      <alias name='video0'/>
    </video>
'''

  vmxml = f'''<domain type='kvm'>
  <name>{ vm_name }</name>
  <title>{ vm_name }</title>
  <memory unit='Mib'>{ vm_paramaters[ 'memory_size' ] }</memory>
  <vcpu>{ vm_paramaters[ 'cpu_count' ] }</vcpu>
  <cpu mode="host-passthrough" check="none" migratable="on"/>
  <clock offset='utc'>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>
  <pm>
    <suspend-to-mem enabled='no'/>
    <suspend-to-disk enabled='no'/>
  </pm>
  <os>
    <type arch='x86_64' machine='pc-i440fx-noble'>hvm</type><!-- pc-q35-8.2 -->
{ '\n'.join( boot_list ) }
    <bootmenu enable='yes' timeout='1000'/>
    <smbios mode='emulate'/>
    <bios useserial='yes' rebootTimeout='0'/>
  </os>
  <features>
    <acpi/>
  </features>
  <devices>
    { console }
{ '\n'.join( disk_list ) }
{ '\n'.join( interface_list ) }
    <rng model='virtio'>
      <backend model='random'>/dev/urandom</backend>
      <alias name='rng0'/>
    </rng>
  </devices>
</domain>
'''

  logging.debug( 'libvirt: vm XML {0}'.format( vmxml ) )
  domain = lvirt.defineXML( vmxml )

  return { 'done': True, 'uuid': domain.UUIDString() }


def create_rollback( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_paramaters = paramaters[ 'vm' ]
  vm_name = vm_paramaters[ 'name' ]
  logging.info( 'libvirt: rolling back vm "{0}"'.format( vm_name ) )
  lvirt = _connect( connection_paramaters )

  try:
    domain = lvirt.lookupByName( vm_name )
  except libvirt.libvirtError:
    domain = None

  if domain is not None:
    try:
      domain.undefine()
    except libvirt.libvirtError:
      pass

  vm_disk_dir = os.path.join( DISK_IMAGE_DIR, vm_name )
  for disk in vm_paramaters[ 'disk_list' ]:
    disk_name = disk[ 'name' ]
    logging.debug( 'libvirt: rollback remove disk "{0}"'.format( disk_name ) )
    try:
      os.unlink( os.path.join( vm_disk_dir, f'{ disk_name }.qcow2' ) )
    except OSError as e:
      if e.errno != 2:  # no such file or directory
        raise e

  os.rmdir( vm_disk_dir )

  logging.info( 'libvirt: vm "{0}" rolledback'.format( vm_name ) )
  return { 'rollback_done': True }


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  logging.info( 'libvirt: destroying vm "{0}"({1})'.format( vm_name, vm_uuid ) )
  lvirt = _connect( connection_paramaters )

  try:
    domain = lvirt.lookupByUUIDString( vm_uuid )
  except libvirt.libvirtError:
    domain = None

  if domain is not None:
    try:
      domain.undefine()
    except libvirt.libvirtError:
      pass

  vm_disk_dir = os.path.join( DISK_IMAGE_DIR, vm_name )
  shutil.rmtree( vm_disk_dir )

  logging.info( 'libvirt: vm "{0}" destroyed'.format( vm_name ) )
  return { 'done': True }


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  interface_list = []
  logging.info( 'libvirt: getting interface map "{0}"({1})'.format( vm_name, vm_uuid ) )
  lvirt = _connect( connection_paramaters )

  domain = lvirt.lookupByUUIDString( vm_uuid )
  root = ElementTree.fromstring( domain.XMLDesc( libvirt.VIR_DOMAIN_XML_SECURE ) )

  for interface in root.findall( './devices/interface' ):
    interface_list.append( interface.find( 'mac' ).attrib[ 'address' ] )

  return { 'interface_list': interface_list }


def _power_state_convert( state ):
  state, _ = state  # the second value is reason for the state
  if state in ( libvirt.VIR_DOMAIN_SHUTDOWN, libvirt.VIR_DOMAIN_SHUTOFF, libvirt.VIR_DOMAIN_CRASHED ):
    return 'off'

  elif state in ( libvirt.VIR_DOMAIN_RUNNING, ):
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]
  logging.info( 'libvirt: setting power state of "{0}"({1}) to "{2}"...'.format( vm_name, vm_uuid, desired_state ) )
  lvirt = _connect( connection_paramaters )

  domain = lvirt.lookupByUUIDString( vm_uuid )

  curent_state = _power_state_convert( domain.state() )
  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  if desired_state == 'on':
    domain.create()

  elif desired_state == 'off':
    domain.destroy()

  elif desired_state == 'soft_off':
    domain.shutdownFlags( libvirt.VIR_DOMAIN_SHUTDOWN_ACPI_POWER_BTN )

  else:
    raise Exception( 'Unknown desired state "{0}"'.format( desired_state ) )

  logging.info( 'libvirt: setting power state of "{0}"({1}) to "{2}" complete'.format( vm_name, vm_uuid, desired_state ) )
  return { 'state': _power_state_convert( domain.state() ) }


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  logging.info( 'libvirt: getting "{0}"({1}) power state...'.format( vm_name, vm_uuid ) )
  lvirt = _connect( connection_paramaters )

  domain = lvirt.lookupByUUIDString( vm_uuid )

  return { 'state': _power_state_convert( domain.state() ) }
