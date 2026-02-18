import logging
import boto3
import asyncio
from botocore.exceptions import WaiterError

from subcontractor.credentials import getCredentials


POLL_INTERVAL = 3
POWER_SET_TIMEOUT = int( 30 / POLL_INTERVAL )


#  TODO: pass credentals in from contractor, for now put them in your env or ~/.aws/credentials
def _connect():
  logging.debug( 'aws: connecting to EC2' )
  token = None
  token = getCredentials( token )
  return boto3.resource( service_name='ec2',
                         # region_name='us-west-2',
                         # aws_access_key_id=ACCESS_KEY,
                         # aws_secret_access_key=SECRET_KEY,
                         aws_session_token=token,
                         use_ssl=True,
                         verify=True )


#  https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
def create( paramaters ):
  instance_name = paramaters[ 'name' ]
  logging.info( 'aws: creating instance "{0}"'.format( instance_name ) )
  ec2 = _connect()

  instance_paramaters = {
                          'ImageId': paramaters[ 'image_id' ],
                          'InstanceType': paramaters[ 'instance_type' ],
                          # 'TagSpecifications': [ { 'ResourceType': 'instance', 'Tags': [ { 'Name': instance_name } ] } ],
                          'KeyName': 'xps13',
                          'MinCount': 1,
                          'MaxCount': 1,
                          'InstanceInitiatedShutdownBehavior': 'stop'
                        }

  instance_list = ec2.create_instances( **instance_paramaters )
  if len( instance_list ) != 1:
    raise Exception( 'Tried to make 1 instance, got {0}'.format( len( instance_list ) ) )

  instance = instance_list[0]

  ec2.Tag( instance.id, 'Name', instance_name )  # TODO: remove this and use TagSpecifications when using newer version of boto3

  logging.info( 'aws: waiting for creation of "{0}"'.format( instance_name ) )
  try:
    instance.wait_until_running()
  except WaiterError:
    raise Exception( 'Timeout waiting for AWS EC2 instance "{0}" to be created'.format( instance_name ) )

  instance = ec2.Instance( instance.id )  # reloadinstance, get the public ip

  interface_list = []
  ip_address_map = {}
  for iface in instance.network_interfaces_attribute:
    name = 'eth{0}'.format( iface[ 'Attachment' ][ 'DeviceIndex' ] )
    interface_list.append( { 'name': name, 'mac': iface[ 'MacAddress' ] } )
    ip_address_map[ name ] = iface[ 'Association' ][ 'PublicIp' ]

  logging.info( 'aws: instance "{0}" created'.format( instance_name ) )
  return { 'done': True, 'id': instance.id, 'interface_list': interface_list, 'ip_address_map': ip_address_map }


def create_rollback( paramaters ):
  instance_name = paramaters[ 'name' ]
  logging.info( 'aws: rolling back instance "{0}"'.format( instance_name ) )

  raise Exception( 'aws rollback not implemented, yet' )

  logging.info( 'aws: instance "{0}" rolledback'.format( instance_name ) )
  return { 'rollback_done': True }


def destroy( paramaters ):
  instance_id = paramaters[ 'instance_id' ]
  instance_name = paramaters[ 'name' ]
  logging.info( 'aws: destroying instance "{0}"({1})'.format( instance_name, instance_id ) )
  ec2 = _connect()
  instance = ec2.Instance( instance_id )

  instance.terminate()

  try:
    instance.wait_for_termated()
  except WaiterError:
    raise Exception( 'Timeout waiting for AWS EC2 instance "{0}" to be terminated'.format( instance_name ) )

  logging.info( 'aws: instance "{0}" destroyed'.format( instance_name ) )
  return { 'done': True }


def _power_state_convert( state ):
  if state[ 'name' ] in ( 'pending', 'terminated', 'stopped' ):
    return 'off'

  elif state[ 'name' ] in ( 'running', 'shutting-down', 'stopping' ):
    return 'on'

  else:
    return 'unknown "{0}"'.format( state )


def set_power( paramaters ):
  instance_id = paramaters[ 'instance_id' ]
  instance_name = paramaters[ 'name' ]
  desired_state = paramaters[ 'state' ]
  logging.info( 'aws: setting power state of "{0}"({1}) to "{2}"...'.format( instance_name, instance_id, desired_state ) )
  ec2 = _connect()
  instance = ec2.Instance( instance_id )

  curent_state = _power_state_convert( instance.state )
  if curent_state == desired_state or ( curent_state == 'off' and desired_state == 'soft_off' ):
    return { 'state': curent_state }

  if desired_state == 'on':
    instance.start()
  elif desired_state == 'off':
    instance.stop( Force=True )
  elif desired_state == 'soft_off':
    instance.stop( Force=False )
  else:
    raise Exception( 'Unknown desired state "{0}"'.format( desired_state ) )

  counter = 0
  while True:
    asyncio.sleep( POLL_INTERVAL )
    instance = ec2.Instance( instance_id )
    if instance.state[ 'Name' ] == 'running':
      break

    counter += 1
    if counter >= POWER_SET_TIMEOUT:
      raise Exception( 'Timeout waiting for AWS EC2 instance "{0}" to power "{1}", curent state: "{2}"'.format( instance_name, desired_state, instance.state[ 'Name' ] ) )

  logging.info( 'aws: setting power state of "{0}"({1}) to "{2}" complete'.format( instance_name, instance_id, desired_state ) )
  return { 'state': _power_state_convert( instance.state ) }


def power_state( paramaters ):
  instance_id = paramaters[ 'instance_id' ]
  instance_name = paramaters[ 'name' ]
  logging.info( 'aws: getting "{0}"({1}) power state...'.format( instance_name, instance_id ) )
  ec2 = _connect()
  instance = ec2.Instance( instance_id )

  return { 'state': _power_state_convert( instance.state ) }
