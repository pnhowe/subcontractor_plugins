import logging
import ssl
import http
import json
from urllib import request

# from subcontractor.credentials import getCredentials


def _get_opener( proxy=None, verify_ssl=False ):
  result = request.OpenerDirector()

  if proxy:  # not doing 'is not None', so empty strings don't try and proxy   # have a proxy option to take it from the envrionment vars
    result.add_handler( request.ProxyHandler( { 'http': proxy, 'https': proxy } ) )
  else:
    result.add_handler( request.ProxyHandler( {} ) )

  result.add_handler( request.HTTPHandler() )
  if hasattr( http.client, 'HTTPSConnection' ):
    if not verify_ssl:
      result.add_handler( request.HTTPSHandler( context=ssl._create_unverified_context() ) )
    else:
      result.add_handler( request.HTTPSHandler() )

  result.add_handler( request.UnknownHandler() )

  return result


def _send( host, auth_key, data ):
  logging.debug( 'vyos: POSTING "{0}" to "{1}"'.format( data, host ) )
  opener = _get_opener()
  result = opener.open( 'https://{0}/configure'.format( host ), data=json.dumps( { 'key': auth_key, 'data': data } ).encode( 'utf-8' ) )
  result = json.loads( result.read() )
  logging.debug( 'vyos: result "{0}" from "{1}"'.format( result, host ) )
  return result


def apply( paramaters ):
  command_list = paramaters[ 'command_list' ]
  logging.info( 'vyos: issuing "{0}" to "{1}"...'.format( command_list, paramaters[ 'host' ] ) )
  for command in command_list:
    rc = _send( paramaters[ 'host' ], paramaters[ 'auth_key' ], { 'op': command[0], 'path': command[1] } )
    if not rc[ 'success' ]:
      return { 'error': rc[ 'error' ] }

  return { 'error': None }

# curl -k -X POST -F data='{"op": "set", "path": ["interfaces", "dummy", "dum1", "address"], "value": "203.0.113.76/32"}' -F key=MY-HTTP-API-PLAINTEXT-KEY https://192.168.122.127/configure
