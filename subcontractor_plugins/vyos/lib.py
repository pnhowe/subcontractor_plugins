import logging
import ssl
import http
import json
from urllib import request, parse

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
  req = request.Request( 'https://{0}/configure'.format( host ), data=parse.urlencode( { 'key': auth_key, 'data': json.dumps( data ) } ).encode( 'utf-8' ), method='POST' )
  result = opener.open( req )
  result = json.loads( result.read() )
  logging.debug( 'vyos: result "{0}" from "{1}"'.format( result, host ) )
  return result


def apply( paramaters ):
  command_list = json.loads( paramaters[ 'command_list' ] )
  logging.info( 'vyos: issuing "{0}" to "{1}"...'.format( command_list, paramaters[ 'host' ] ) )
  for command in command_list:
    rc = _send( paramaters[ 'host' ], paramaters[ 'auth_key' ], { 'op': command[0], 'path': command[1] } )
    if not rc[ 'success' ]:
      return { 'error': rc[ 'error' ] }

  return { 'error': None }
