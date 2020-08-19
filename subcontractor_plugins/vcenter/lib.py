import logging
import time
import re
import random
import ssl
from datetime import datetime, timedelta

from pyVim import connect
from pyVmomi import vim

from subcontractor.credentials import getCredentials
from subcontractor_plugins.vcenter.images import OVAImportHandler, OVAExportHandler

POLL_INTERVAL = 4
BOOT_ORDER_MAP = {
                    'hdd': vim.vm.BootOptions.BootableDiskDevice( deviceKey=2000 ),  # TODO: figure out which is the boot drive and put it here
                    'net': vim.vm.BootOptions.BootableEthernetDevice( deviceKey=4000 ),  # TODO: figure out which is the provisinioning interface and set it here
                    'cd': 'CD',
                    'usb': 'USB'
                 }

NET_CLASS_MAP = { 'E1000': vim.vm.device.VirtualE1000,
                  'E1000e': vim.vm.device.VirtualE1000e,
                  'PCNet32': vim.vm.device.VirtualPCNet32,
                  'VMXNet': vim.vm.device.VirtualVmxnet,
                  'VMXNet2': vim.vm.device.VirtualVmxnet2,
                  'VMXNet3': vim.vm.device.VirtualVmxnet3
                  }


class MOBNotFound( Exception ):
  pass


def _connect( connection_paramaters ):
  # work arround invalid SSL
  _create_unverified_https_context = ssl._create_unverified_context
  ssl._create_default_https_context = _create_unverified_https_context
  # TODO: flag for trusting SSL of connection, also there is a paramater to Connect for verified SSL

  creds = connection_paramaters[ 'credentials' ]
  if isinstance( creds, str ):
    creds = getCredentials( creds )

  # TODO: saninity check on creds

  if 'username' in creds:
    logging.debug( 'vcenter: connecting to "{0}" with user "{1}"'.format( connection_paramaters[ 'host' ], creds[ 'username' ] ) )
    return connect.SmartConnect( host=connection_paramaters[ 'host' ], user=creds[ 'username' ], pwd=creds[ 'password' ], mechanism='userpass' )

  else:
    logging.debug( 'vcenter: connecting to "{0}" with token "{1}"'.format( connection_paramaters[ 'host' ], creds[ 'token' ] ) )
    return connect.SmartConnect( host=connection_paramaters[ 'host' ], b64token=creds[ 'token' ], mechanism='sspi' )


def _disconnect( si ):
  connect.Disconnect( si )


def _taskWait( task ):
  while True:
    if task.info.state not in ( 'running', 'queued' ):
      return

    try:
      progress = task.info.progress
    except AttributeError:
      progress = None

    if progress is not None:
      logging.debug( 'vmware: Waiting, {0}% Complete ...'.format( task.info.progress ) )
    else:
      logging.debug( 'vmware: Waiting ...' )

    time.sleep( POLL_INTERVAL )


def _getDatacenter( si, name ):
  for item in si.content.rootFolder.childEntity:
    if item.__class__.__name__ == 'vim.Datacenter' and item.name == name:
      return item

  raise MOBNotFound( 'Datacenter "{0}" not found'.format( name ) )


def _getResourcePool( dc, name ):  # TODO: recursive folder search
  for item in dc.hostFolder.childEntity:
    if item.__class__.__name__ in ( 'vim.ComputeResource', 'vim.ClusterComputeResource' ) and item.name == name:
      return item.resourcePool

    if item.__class__.__name__ in ( 'vim.ResourcePool', ) and item.name == name:
      return item

  raise MOBNotFound( 'Cluster/ResourcePool "{0}" not found'.format( name ) )


def _getHost( rp, name ):
  for host in rp.owner.host:
    if host.name == name:
      return host

  raise MOBNotFound( 'Host "{0}" in "{1}" not found'.format( name, rp.name ) )


def _getDatastore( dc, name ):
  for ds in dc.datastore:
    if ds.name == name:
      return ds

  raise MOBNotFound( 'Datastore "{0}" in "{1}" not found'.format( name, dc.name ) )


def _getNetwork( host, name ):
  for network in host.network:
    if network.name == name:
      return network

  raise MOBNotFound( 'Network "{0}" in "{1}" not found'.format( name, host.name ) )


def _getVM( si, vm_uuid ):
  cont = si.RetrieveContent()
  vm = cont.searchIndex.FindByUuid( None, vm_uuid, True, True )

  if vm is None:
    raise MOBNotFound( 'vcenter: unable to find vm "{0}"'.format( vm_uuid ) )

  return vm


def _genPaths( vm_name, disk_list, datastore ):
  vmx_file_path = '[{0}] {1}/{1}.vmx'.format( datastore.name, vm_name )

  disk_filepath_list = []
  for disk in disk_list:
    disk_filepath_list.append( '[{0}] {1}/{2}.vmdk'.format( datastore.name, vm_name, disk[ 'name' ] ) )

  logging.debug( 'vcenter: vm path: "{0}", disk Paths {1}'.format( vmx_file_path, disk_filepath_list ) )

  return vmx_file_path, disk_filepath_list


def _genNetworkBacking( network ):
  if network.__class__.__name__ == 'vim.dvs.DistributedVirtualPortgroup':
    result = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
    result.port = vim.dvs.PortConnection()
    result.port.portgroupKey = network.key
    result.port.switchUuid = network.config.distributedVirtualSwitch.uuid

  else:
    result = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
    result.deviceName = network.name

  return result


def host_list( paramaters ):
  # returns a list of hosts in a resource
  # host must have paramater[ 'min_memory' ] aviable in MB
  # orderd by paramater[ 'cpu_scaler' ] * %cpu remaning + paramater[ 'memory_scaler' ] * %mem remaning
  connection_paramaters = paramaters[ 'connection' ]
  logging.info( 'vcenter: getting Host List for dc: "{0}"  rp: "{1}"'.format( paramaters[ 'datacenter' ], paramaters[ 'cluster' ] ) )
  si = _connect( connection_paramaters )
  try:
    dataCenter = _getDatacenter( si, paramaters[ 'datacenter' ] )
    resourcePool = _getResourcePool( dataCenter, paramaters[ 'cluster' ] )

    host_map = {}
    for host in resourcePool.owner.host:
      if host.summary.quickStats.overallMemoryUsage is None:  # sometimes the quickstats don't get updated, for now skip that host
        continue

      total_memory = host.summary.hardware.memorySize / 1024.0 / 1024.0  # we want MiB
      memory_aviable = total_memory - host.summary.quickStats.overallMemoryUsage
      if memory_aviable < paramaters[ 'min_memory' ]:
        logging.debug( 'vcenter: host "{0}", low aviable ram: "{1}"'.format( host.name, memory_aviable ) )
        continue

      total_cpu = host.summary.hardware.numCpuCores * host.summary.hardware.cpuMhz
      cpu_aviable = total_cpu - host.summary.quickStats.overallCpuUsage

      host_map[ host.name ] = ( paramaters[ 'memory_scaler' ] * ( memory_aviable / total_memory ) ) + ( paramaters[ 'cpu_scaler' ] * ( cpu_aviable / total_cpu ) )

    logging.debug( 'vcenter: host_map {0}'.format( host_map ) )

    result = list( host_map.keys() )
    result.sort( key=lambda a: host_map[ a ], reverse=True )

    return { 'host_list': result }

  finally:
    _disconnect( si )


def create_datastore( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  logging.info( 'vcenter: creating datastores: "{0}"'.format( paramaters[ 'name' ] ) )
  si = _connect( connection_paramaters )
  try:
    dataCenter = _getDatacenter( si, paramaters[ 'datacenter' ] )
    resourcePool = _getResourcePool( dataCenter, paramaters[ 'host' ] )
    host = _getHost( resourcePool, paramaters[ 'host' ] )

    dss = host.configManager.datastoreSystem
    ss = host.configManager.storageSystem

    disk_list = []
    for lun in ss.storageDeviceInfo.scsiLun:
      disk_list.append( { 'model': lun.model, 'path': lun.devicePath } )

    spec = None
    for i in range( 0, len( disk_list ) ):
      disk = disk_list[ i ]
      if disk[ 'model' ] == paramaters[ 'model' ]:
        spec = dss.QueryVmfsDatastoreCreateOptions( disk[ 'path' ] )[0].spec
        del disk_list[ i ]
        break

    if spec is None:
      raise ValueError( 'Unable to find an aviable disk with model "{0}"'.format( paramaters[ 'model' ] ) )

    spec.vmfs.volumeName = paramaters[ 'name' ]

    dss.CreateVmfsDatastore( spec )

    return { 'done': True }

  finally:
    _disconnect( si )


def datastore_list( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  logging.info( 'vcenter: getting Datastore List for dc: "{0}" rp: "{1}" host: "{2}"'.format( paramaters[ 'datacenter' ], paramaters[ 'cluster' ], paramaters[ 'host' ] ) )
  paramaters[ 'min_free_space' ] = paramaters[ 'min_free_space' ]
  if paramaters.get( 'name_regex', None ):  # could also be ''
    try:
      paramaters[ 'name_regex' ] = re.compile( paramaters[ 'name_regex' ] )
    except TypeError:
      paramaters[ 'name_regex' ] = None
  else:
    paramaters[ 'name_regex' ] = None

  si = _connect( connection_paramaters )
  try:
    dataCenter = _getDatacenter( si, paramaters[ 'datacenter' ] )
    resourcePool = _getResourcePool( dataCenter, paramaters[ 'cluster' ] )
    host = _getHost( resourcePool, paramaters[ 'host' ] )

    result = []
    for datastore in host.datastore:
      if datastore.summary.freeSpace / 1024.0 / 1024.0 / 1024.0 < paramaters[ 'min_free_space' ]:
        continue

      if paramaters[ 'name_regex' ] is not None and not paramaters[ 'name_regex' ].match( datastore.name ):
        continue

      result.append( datastore.name )

    return { 'datastore_list': result }

  finally:
    _disconnect( si )


def network_list( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  logging.info( 'vcenter: getting Network List for dc: "{0}" rp: "{1}" host: "{2}"'.format( paramaters[ 'datacenter' ], paramaters[ 'cluster' ], paramaters[ 'host' ] ) )

  try:
    paramaters[ 'name_regex' ] = re.compile( paramaters[ 'name_regex' ] )
  except TypeError:
    pass

  si = _connect( connection_paramaters )
  try:
    dataCenter = _getDatacenter( si, paramaters[ 'datacenter' ] )
    resourcePool = _getResourcePool( dataCenter, paramaters[ 'cluster' ] )
    host = _getHost( resourcePool, paramaters[ 'host' ] )

    result = []
    for network in host.network:
      if paramaters[ 'name_regex' ] is not None and not paramaters[ 'name_regex' ].match( network.name ):
        continue

      result.append( network.name )

    return { 'network_list': result }

  finally:
    _disconnect( si )


def _createDisk( si, dc, disk, datastore, file_path ):
  ( dir_name, _ ) = file_path.rsplit( '/', 1 )

  spec = vim.host.DatastoreBrowser.SearchSpec()
  spec.query.append( vim.host.DatastoreBrowser.FolderQuery() )
  task = datastore.browser.SearchDatastore_Task( datastorePath=dir_name, searchSpec=spec )
  _taskWait( task )

  if task.info.state == 'error':
    if task.info.error.__class__.__name__ == 'vim.fault.FileNotFound':
      logging.debug( 'vcenter: making dir "{0}"'.format( dir_name ) )
      si.content.fileManager.MakeDirectory( name=dir_name, datacenter=dc, createParentDirectories=True )
    else:
      raise Exception( 'Unknown Task Error when checking directory: "{0}"'.format( task.info.error ) )

  elif task.info.state != 'success':
    raise Exception( 'Unexpected Task State when checking directory: "{0}"'.format( task.info.state ) )

  spec = vim.VirtualDiskManager.FileBackedVirtualDiskSpec()
  spec.diskType = disk.get( 'type', 'thin' )  # 'thin', 'eagerZeroedThick', 'preallocate'
  spec.adapterType = disk.get( 'adapter', 'busLogic' )  # 'busLogic', 'ide', 'lsiLogic'
  spec.capacityKb = disk.get( 'size', 10 ) * 1024 * 1024  # convert to kb we got GiB

  logging.debug( 'vcenter: creating disk "{0}"'.format( file_path ) )

  task = si.content.virtualDiskManager.CreateVirtualDisk( name=file_path, datacenter=dc, spec=spec )
  _taskWait( task )

  if task.info.state == 'error':
    raise Exception( 'Unknown Task Error when Creating Disk: "{0}"'.format( task.info.error ) )

  if task.info.state != 'success':
    raise Exception( 'Unexpected Task State when Creating Disk: "{0}"'.format( task.info.state ) )


def _inject_ovf_env( si, vm, vm_paramaters ):
  logging.info( 'vcenter: injecting ovf enviornment' )
  try:
    property_map = vm_paramaters[ 'property_map' ]
  except KeyError:
    return

  values = {}
  values[ 'moid' ] = 'vm-{0}'.format( vm._moId )
  values[ 'kind' ] = si.content.about.name
  values[ 'version' ] = si.content.about.version
  values[ 'vendor' ] = si.content.about.vendor

  property_list = []
  for key, value in property_map.items():
    property_list.append( '         <Property oe:key="{0}" oe:value="{1}"/>'.format( key, value ) )

  values[ 'properties' ] = '\n'.join( property_list )

  spec = """<?xml version="1.0" encoding="UTF-8"?>
<Environment
     xmlns="http://schemas.dmtf.org/ovf/environment/1"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xmlns:oe="http://schemas.dmtf.org/ovf/environment/1"
     xmlns:ve="http://www.vmware.com/schema/ovfenv"
     oe:id=""
     ve:esxId="vm-{moid}">
   <PlatformSection>
      <Kind>{kind}/Kind>
      <Version>{version}</Version>
      <Vendor>{vendor}</Vendor>
      <Locale>en</Locale>
   </PlatformSection>
   <PropertySection>
{properties}
   </PropertySection>
</Environment>
""".format( **values )

  opt = vim.option.OptionValue()
  opt.key = 'guestinfo.ovfEnv'
  opt.value = spec

  configSpec = vim.vm.ConfigSpec()
  configSpec.extraConfig = [ opt ]

  task = vm.ReconfigVM_Task( configSpec )
  _taskWait( task )

  if task.info.state == 'error':
    raise Exception( 'Error With OVF Environment Injection: "{0}"'.format( task.info.error ) )

  if task.info.state != 'success':
    raise Exception( 'Unexpected Task State With OVF Environment Injection: "{0}"'.format( task.info.state ) )


def _create_from_template( si, vm_name, data_center, resource_pool, folder, host, datastore, vm_paramaters ):
  logging.info( 'vcenter: creating from Template("{0}") "{1}"'.format( vm_paramaters[ 'template' ], vm_name ) )

  template = None
  cont = si.RetrieveContent()
  for item in cont.viewManager.CreateContainerView( data_center, [ vim.VirtualMachine ], True ).view:
    if item.name == vm_paramaters[ 'template' ]:
      template = item
      break

  if template is None:
    raise MOBNotFound( 'vcenter: unable to find template "{0}"'.format( vm_paramaters[ 'template' ] ) )

  network_device_list = []
  for device in template.config.hardware.device:
    if isinstance( device, vim.vm.device.VirtualEthernetCard ):
      network_device_list.append( device )

  if len( network_device_list ) != len( vm_paramaters[ 'interface_list' ] ):
    raise ValueError( 'network in template and config missmatch' )

  configSpec = vim.vm.ConfigSpec()
  customSpec = vim.vm.customization.Specification()

  customSpec.identity = vim.vm.customization.LinuxPrep()
  customSpec.identity.domain = vm_paramaters[ 'domain' ]
  customSpec.identity.hostName = vim.vm.customization.FixedName( name=vm_paramaters[ 'hostname' ] )
  customSpec.globalIPSettings = vim.vm.customization.GlobalIPSettings()
  customSpec.globalIPSettings.dnsServerList = vm_paramaters[ 'dnsserver_list' ]
  customSpec.globalIPSettings.dnsSuffixList = vm_paramaters[ 'dnssuffix_list' ]

  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    interface = vm_paramaters[ 'interface_list' ][ i ]
    network = _getNetwork( host, interface[ 'network' ] )

    devSpec = vim.vm.device.VirtualDeviceSpec()
    devSpec.operation = 'edit'
    devSpec.device = network_device_list[ i ]
    devSpec.device.addressType = 'Manual'
    devSpec.device.macAddress = interface[ 'mac' ]
    devSpec.device.backing = _genNetworkBacking( network )
    configSpec.deviceChange.append( devSpec )

    ipSettings = vim.vm.customization.IPSettings()
    ipSettings.ip = vim.vm.customization.FixedIp( ipAddress=interface[ 'address' ] )
    ipSettings.subnetMask = interface[ 'netmask' ]
    try:
      ipSettings.gateway = interface[ 'gateway' ]
    except KeyError:
      pass

    adapter = vim.vm.customization.AdapterMapping()
    adapter.adapter = ipSettings
    adapter.macAddress = interface[ 'mac' ]
    customSpec.nicSettingMap.append( adapter )

  property_map = vm_paramaters.get( 'property_map', None )
  if property_map is not None:
    configSpec.vAppConfig = vim.vApp.VmConfigSpec()
    configSpec.vAppConfig.ovfEnvironmentTransport = [ 'com.vmware.guestInfo' ]
    counter = 0
    for key, value in vm_paramaters[ 'property_map' ].items():
      counter += 1
      property = vim.vApp.PropertySpec()
      property.operation = 'add'
      property.info = vim.vApp.PropertyInfo()
      property.info.key = counter
      property.info.id = key
      property.info.value = value
      property.info.type = 'string'
      configSpec.vAppConfig.property.append( property )

  reloSpec = vim.vm.RelocateSpec()
  reloSpec.datastore = datastore
  reloSpec.host = host
  reloSpec.pool = resource_pool

  cloneSpec = vim.vm.CloneSpec()
  cloneSpec.config = configSpec
  cloneSpec.location = reloSpec
  cloneSpec.customization = customSpec
  cloneSpec.powerOn = False
  cloneSpec.template = False

  task = template.Clone( folder=folder, name=vm_name, spec=cloneSpec )
  _taskWait( task )

  if task.info.state == 'error':
    raise Exception( 'Error With VM Clone Task: "{0}"'.format( task.info.error ) )

  if task.info.state != 'success':
    raise Exception( 'Unexpected Task State With VM Clone: "{0}"'.format( task.info.state ) )

  return task.info.result.config.instanceUuid


def _create_from_ova( si, vm_name, connection_host, data_center, resource_pool, folder, host, datastore, vm_paramaters ):
  logging.info( 'vcenter: creating from OVA("{0}") "{1}"'.format( vm_paramaters[ 'ova' ], vm_name ) )
  if hasattr( ssl, '_create_unverified_context' ):
    sslContext = ssl._create_unverified_context()
  else:
    sslContext = None

  handler = OVAImportHandler( vm_paramaters[ 'ova' ], sslContext )

  ovf_manager = si.content.ovfManager

  network_mapping = []
  for interface in vm_paramaters[ 'interface_list' ]:
    network_mapping.append( vim.OvfManager.NetworkMapping( name=interface[ 'physical_location' ], network=_getNetwork( host, interface[ 'network' ] ) ) )

  property_map = []
  try:
    for key, value in vm_paramaters[ 'property_map' ].items():
      property_map.append( vim.KeyValue( key=key, value=value ) )
  except KeyError:
    pass

  cisp = vim.OvfManager.CreateImportSpecParams( entityName=vm_name, hostSystem=host, propertyMapping=property_map, networkMapping=network_mapping )

  try:
    cisp.diskProvisioning = vm_paramaters[ 'disk_provisioning' ]
  except KeyError:
    pass

  try:
    cisp.deploymentOption = vm_paramaters[ 'deployment_option' ]
  except KeyError:
    pass

  try:
    cisp.ipProtocol = vm_paramaters[ 'ip_protocol' ]
  except KeyError:
    pass

  logging.debug( 'vcenter: Import Spec Params: "{0}"'.format( cisp ) )

  result = ovf_manager.CreateImportSpec( handler.descriptor, resource_pool, datastore, cisp )

  if result.importSpec is not None and result.importSpec.configSpec.vAppConfig is not None:
    for property in result.importSpec.configSpec.vAppConfig.property:
      info = property.info
      if info.id in vm_paramaters[ 'property_map' ] and not info.userConfigurable:
        logging.warning( 'Setting non user configurable "{0}" to configurable'.format( info.id ) )
        info.userConfigurable = True

  if len( result.warning ):
    logging.warning( 'vcenter: Warning with OVA Import Spec: "{0}"'.format( result.warning ) )

  if len( result.error ):
    raise Exception( 'OVA Import Errors: "{0}"'.format( '","'.join( [ str( i ) for i in result.error ] ) ) )

  uuid = handler.upload( connection_host, resource_pool, result, data_center )

  if si.content.about.productLineId == 'embeddedEsx':
    _inject_ovf_env( si, _getVM( si, uuid ), vm_paramaters )

  return uuid


def _create_from_scratch( si, vm_name, data_center, resource_pool, folder, host, datastore, vm_paramaters ):
  logging.info( 'vcenter: creating from scratch "{0}"'.format( vm_name ) )

  vmx_file_path, disk_filepath_list = _genPaths( vm_paramaters[ 'name' ], vm_paramaters[ 'disk_list' ], datastore )

  for i in range( 0, len( vm_paramaters[ 'disk_list' ] ) ):
    _createDisk( si, data_center, vm_paramaters[ 'disk_list' ][ i ], datastore, disk_filepath_list[ i ] )

  configSpec = vim.vm.ConfigSpec()
  configSpec.name = vm_name
  configSpec.memoryMB = vm_paramaters[ 'memory_size' ]  # in MiB
  configSpec.numCPUs = vm_paramaters[ 'cpu_count' ]
  configSpec.guestId = vm_paramaters[ 'guest_id' ]

  configSpec.flags = vim.vm.FlagInfo()

  configSpec.files = vim.vm.FileInfo()
  configSpec.files.vmPathName = vmx_file_path

  configSpec.bootOptions = vim.vm.BootOptions()
  configSpec.bootOptions.bootDelay = 5000
  configSpec.bootOptions.bootRetryEnabled = True
  configSpec.bootOptions.bootRetryDelay = 50000

  devSpec = vim.vm.device.VirtualDeviceSpec()
  devSpec.operation = 'add'
  devSpec.device = vim.vm.device.VirtualLsiLogicController()
  devSpec.device.key = 1000
  devSpec.device.sharedBus = 'noSharing'
  devSpec.device.busNumber = 0
  devSpec.device.controllerKey = 100
  devSpec.device.unitNumber = 0
  configSpec.deviceChange.append( devSpec )

  for i in range( 0, len( vm_paramaters[ 'disk_list' ] ) ):
    disk = vm_paramaters[ 'disk_list' ][ i ]
    devSpec = vim.vm.device.VirtualDeviceSpec()
    devSpec.operation = 'add'
    devSpec.device = vim.vm.device.VirtualDisk()
    devSpec.device.key = 2000 + i
    devSpec.device.controllerKey = 1000
    devSpec.device.capacityInKB = disk[ 'size' ] * 1024 * 1024  # want KB were passed in GiB
    devSpec.device.unitNumber = i + 1
    devSpec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
    devSpec.device.backing.fileName = disk_filepath_list[ i ]
    devSpec.device.backing.datastore = datastore
    devSpec.device.backing.diskMode = 'persistent'
    configSpec.deviceChange.append( devSpec )

  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    interface = vm_paramaters[ 'interface_list' ][ i ]
    network = _getNetwork( host, interface[ 'network' ] )

    try:
      devClass = NET_CLASS_MAP[ interface.get( 'type', 'E1000' ) ]
    except KeyError:
      raise ValueError( 'Unknown interface type "{0}"'.format( interface[ 'type' ] ) )

    devSpec = vim.vm.device.VirtualDeviceSpec()
    devSpec.operation = 'add'
    devSpec.device = devClass()
    devSpec.device.key = 4000 + i
    devSpec.device.controllerKey = 100
    devSpec.device.addressType = 'Manual'
    devSpec.device.macAddress = interface[ 'mac' ]
    devSpec.device.unitNumber = i + 7
    devSpec.device.backing = _genNetworkBacking( network )
    configSpec.deviceChange.append( devSpec )

  for item in vm_paramaters[ 'boot_order' ]:
    configSpec.bootOptions.bootOrder.append( BOOT_ORDER_MAP[ item ] )

  veu = vm_paramaters.get( 'virtual_exec_usage', None )
  if veu == 'on':
    configSpec.flags.virtualExecUsage = 'hvOn'
  elif veu == 'off':
    configSpec.flags.virtualExecUsage = 'hvOff'
  elif veu == 'auto':
    configSpec.flags.virtualExecUsage = 'hvAuto'

  vmu = vm_paramaters.get( 'virtual_mmu_usage', None )
  if vmu == 'on':
    configSpec.flags.virtualMmuUsage = 'on'
  elif vmu == 'off':
    configSpec.flags.virtualMmuUsage = 'off'
  elif vmu == 'auto':
    configSpec.flags.virtualMmuUsage = 'automatic'

  if vm_paramaters.get( 'virtual_vhv', False ):
    configSpec.nestedHVEnabled = True

  property_map = vm_paramaters.get( 'property_map', None )
  if property_map is not None:
    configSpec.vAppConfig = vim.vApp.VmConfigSpec()
    configSpec.vAppConfig.ovfEnvironmentTransport = [ 'com.vmware.guestInfo' ]
    counter = 0
    for key, value in vm_paramaters[ 'property_map' ].items():
      counter += 1
      property = vim.vApp.PropertySpec()
      property.operation = 'add'
      property.info = vim.vApp.PropertyInfo()
      property.info.key = counter
      property.info.id = key
      property.info.value = value
      property.info.type = 'string'
      configSpec.vAppConfig.property.append( property )

  task = folder.CreateVm( config=configSpec, pool=resource_pool, host=host )

  _taskWait( task )

  if task.info.state == 'error':
    raise Exception( 'Error With VM Create Task: "{0}"'.format( task.info.error ) )

  if task.info.state != 'success':
    raise Exception( 'Unexpected Task State With VM Create: "{0}"'.format( task.info.state ) )

  return task.info.result.config.instanceUuid


def create( paramaters ):  # NOTE: the picking of the cluster/host and datastore should be done prior to calling this, that way rollback can know where it's at
  vm_paramaters = paramaters[ 'vm' ]
  connection_paramaters = paramaters[ 'connection' ]
  vm_name = vm_paramaters[ 'name' ]

  # vcenter static mac are 00:50:56:00:00:00 -> 00:50:56:3F:FF:FF
  # not used by OVA deploy, mabey it should?
  for i in range( 0, len( vm_paramaters[ 'interface_list' ] ) ):
    mac = '005056{0:06x}'.format( random.randint( 0, 4194303 ) )  # TODO: check to see if the mac is allready in use, also make them sequential
    vm_paramaters[ 'interface_list' ][ i ][ 'mac' ] = ':'.join( mac[ x:x + 2 ] for x in range( 0, 12, 2 ) )

  logging.info( 'vcenter: creating vm "{0}"'.format( vm_name ) )
  si = _connect( connection_paramaters )
  try:
    data_center = _getDatacenter( si, vm_paramaters[ 'datacenter' ] )
    resource_pool = _getResourcePool( data_center, vm_paramaters[ 'cluster' ] )
    folder = data_center.vmFolder
    host = _getHost( resource_pool, vm_paramaters[ 'host' ] )
    datastore = _getDatastore( data_center, vm_paramaters[ 'datastore' ] )

    if 'ova' in vm_paramaters:
      vm_uuid = _create_from_ova( si, vm_name, paramaters[ 'connection' ][ 'host' ], data_center, resource_pool, folder, host, datastore, vm_paramaters )
    elif 'template' in vm_paramaters:
      vm_uuid = _create_from_template( si, vm_name, data_center, resource_pool, folder, host, datastore, vm_paramaters )
    else:
      vm_uuid = _create_from_scratch( si, vm_name, data_center, resource_pool, folder, host, datastore, vm_paramaters )

    logging.info( 'vcenter: vm "{0}" created, uuid: "{1}"'.format( vm_name, vm_uuid ) )

    return { 'done': True, 'uuid': vm_uuid }

  finally:
    _disconnect( si )


def create_rollback( paramaters ):
  vm_paramaters = paramaters[ 'vm' ]
  connection_paramaters = paramaters[ 'connection' ]
  vm_name = vm_paramaters[ 'name' ]
  logging.info( 'vcenter: rolling back vm "{0}"'.format( vm_name ) )

  si = _connect( connection_paramaters )
  try:
    dataCenter = _getDatacenter( si, vm_paramaters[ 'datacenter' ] )
    datastore = _getDatastore( dataCenter, vm_paramaters[ 'datastore' ] )

    vmx_file_path, disk_filepath_list = _genPaths( vm_paramaters[ 'name' ], vm_paramaters[ 'disk_list' ], datastore )

    file_list = disk_filepath_list + [ i.replace( '.vmdk', '-flat.vmdk' ) for i in disk_filepath_list ] + [ vmx_file_path ]

    for item in file_list:
      logging.debug( 'vcenter: deleting "{0}"'.format( item ) )
      task = si.content.fileManager.DeleteFile( name=item, datacenter=dataCenter )
      _taskWait( task )
      if task.info.state == 'error':
        if task.info.error.__class__.__name__ == 'vim.fault.FileNotFound':
          continue
        else:
          raise Exception( 'Unknown Task Error when Deleting "{0}": "{1}"'.format( item, task.info.error ) )

      if task.info.state != 'success':
        raise Exception( 'Unexpected Task State when Deleting "{0}": "{1}"'.format( task.info.state ) )

    # remove all the folders if empty

    return { 'rollback_done': True }

  finally:
    _disconnect( si )


def destroy( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'vcenter: destroying vm "{0}"({1})'.format( vm_name, vm_uuid ) )
  si = _connect( connection_paramaters )
  try:
    try:
      vm = _getVM( si, vm_uuid )
    except MOBNotFound:
      return { 'done': True }  # it's gone, we are donne

    task = vm.Destroy()

    _taskWait( task )

    if task.info.state == 'error':
      raise Exception( 'Error With VM Destroy Task: "{0}"'.format( task.info.error ) )

    if task.info.state != 'success':
      raise Exception( 'Unexpected Task State With VM Destroy: "{0}"'.format( task.info.state ) )

    logging.info( 'vcenter: vm "{0}" destroyed'.format( vm_name ) )
    return { 'done': True }

  finally:
    _disconnect( si )


def get_interface_map( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  interface_list = []

  logging.info( 'vcenter: getting interface map "{0}"({1})'.format( vm_name, vm_uuid ) )
  si = _connect( connection_paramaters )
  try:
    vm = _getVM( si, vm_uuid )

    for device in vm.config.hardware.device:
      if device.__class__ in NET_CLASS_MAP.values():
        i = device.key - 4000
        if i < 0 or i > 64:
          raise ValueError( 'Invalid device key "{0}"'.format( device.key ) )

        interface_list.append( device.macAddress )

    return { 'interface_list': interface_list }

  finally:
    _disconnect( si )


def _power_state_convert( state ):
  if state in ( vim.VirtualMachinePowerState.poweredOff, vim.VirtualMachinePowerState.suspended ):
    return 'off'

  elif state in ( vim.VirtualMachinePowerState.poweredOn, ):
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]

  logging.info( 'vcenter: setting power state of "{0}"({1}) to "{2}"...'.format( vm_name, vm_uuid, desired_state ) )
  si = _connect( connection_paramaters )
  try:
    vm = _getVM( si, vm_uuid )

    curent_state = _power_state_convert( vm.runtime.powerState )
    if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
      return { 'state': curent_state }

    task = None
    if desired_state == 'on':
      task = vm.PowerOn()
    elif desired_state == 'off':
      task = vm.PowerOff()
    elif desired_state == 'soft_off':
      try:
        vm.ShutdownGuest()  # no Task
      except vim.fault.ToolsUnavailable:
        task = vm.PowerOff()

    # if it won't power off
    # vm.terminateVM()  # no Task

    if task is not None:
      while task.info.state not in ( vim.TaskInfo.State.success, vim.TaskInfo.State.error ):
        logging.debug( 'vcenter: vm "{0}"({1}) power "{2}" at {3}%'.format( vm_name, vm_uuid, desired_state, task.info.progress ) )
        time.sleep( POLL_INTERVAL )

      if task.info.state == vim.TaskInfo.State.error:
        raise Exception( 'vcenter: Unable to set power state of "{0}"({1}) to "{2}"'.format( vm_name, vm_uuid, desired_state ) )

    else:
      time.sleep( POLL_INTERVAL * 2 )  # give the vm the chance to do something

    logging.info( 'vcenter: setting power state of "{0}"({1}) to "{2}" complete'.format( vm_name, vm_uuid, desired_state ) )
    return { 'state': _power_state_convert( vm.runtime.powerState ) }

  finally:
    _disconnect( si )


def power_state( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]

  logging.info( 'vcenter: getting "{0}"({1}) power state...'.format( vm_name, vm_uuid ) )
  si = _connect( connection_paramaters )
  try:
    vm = _getVM( si, vm_uuid )

    return { 'state': _power_state_convert( vm.runtime.powerState ) }

  finally:
    _disconnect( si )


def execute( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  program = paramaters[ 'program' ]
  args = paramaters[ 'args' ]
  dir = paramaters[ 'dir' ]

  logging.info( 'vcenter: executing "{0}" "{1}" on "{2}"({3})'.format( program, args, vm_name, vm_uuid ) )
  si = _connect( connection_paramaters )
  try:
    vm = _getVM( si, vm_uuid )

    if vm.guest.toolsStatus in ( 'toolsNotInstalled', 'toolsNotRunning' ):
      return { 'error': 'VMwareTools is not installed or not Running' }

    pManager = si.content.guestOperationsManager.processManager

    passwordAuth = vim.vm.guest.NamePasswordAuthentication( username=paramaters[ 'username' ], password=paramaters[ 'password' ] )

    programSpec = vim.vm.guest.ProcessManager.ProgramSpec()
    programSpec.programPath = program
    programSpec.arguments = args
    programSpec.workingDirectory = dir

    pid = pManager.StartProgramInGuest( vm=vm, auth=passwordAuth, spec=programSpec )

    logging.debug( 'vcenter: executing "{0}" "{1}" on "{2}"({3}) is pid "{4}"'.format( program, args, vm_name, vm_uuid, pid ) )

    finish_by = timedelta( seconds=paramaters[ 'timeout' ] ) + datetime.utcnow()
    pList = pManager.ListProcessesInGuest( vm=vm, auth=passwordAuth, pids=[ pid ] )
    while pList[0].exitCode is None:
      logging.debug( 'vcenter: executing "{0}" "{1}" on "{2}"({3}) waiting for "{4}"...'.format( program, args, vm_name, vm_uuid, pid ) )
      time.sleep( POLL_INTERVAL )
      pList = pManager.ListProcessesInGuest( vm=vm, auth=passwordAuth, pids=[ pid ] )

      if datetime.utcnow() > finish_by:
        raise Exception( 'timeout waiting for command to finish' )

    return { 'rc': pList[0].exitCode }

  finally:
    _disconnect( si )


def mark_as_template( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  as_template = paramaters[ 'as_template' ]

  logging.info( 'vcenter: mark_as_template "{0}"({1}) to "{2}"...'.format( vm_name, vm_uuid, as_template ) )
  si = _connect( connection_paramaters )
  try:
    if si.content.about.productLineId == 'embeddedEsx':  # mark as template not supported on ESX
      return {}

    vm = _getVM( si, vm_uuid )

    if as_template:
      vm.MarkAsTemplate()
    else:
      vm.MarkAsVirtualMachine()

    return {}

  finally:
    _disconnect( si )


def export( paramaters ):
  connection_paramaters = paramaters[ 'connection' ]
  vm_uuid = paramaters[ 'uuid' ]
  vm_name = paramaters[ 'name' ]
  url = paramaters[ 'url' ]

  if hasattr( ssl, '_create_unverified_context' ):
    sslContext = ssl._create_unverified_context()
  else:
    sslContext = None

  logging.info( 'vcenter: exporting "{0}"({1}) to "{2}"...'.format( vm_name, vm_uuid, url ) )

  si = _connect( connection_paramaters )
  try:
    handler = OVAExportHandler( si.content.ovfManager, url, sslContext )
    vm = _getVM( si, vm_uuid )
    location = handler.export( paramaters[ 'connection' ][ 'host' ], vm, vm_name )

    return { 'location': location }

  finally:
    _disconnect( si )
