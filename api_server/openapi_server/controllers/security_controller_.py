import config
import logging

from jwkaas import JWKaas
from flask import request, g

my_jwkaas = None

if hasattr(config, 'OAUTH_JWKS_URL'):
    my_jwkaas = JWKaas(config.OAUTH_EXPECTED_AUDIENCE,
                       config.OAUTH_EXPECTED_ISSUER,
                       jwks_url=config.OAUTH_JWKS_URL)

if hasattr(config, 'OAUTH_E2E_JWKS_URL'):
    my_e2e_jwkaas = JWKaas(config.OAUTH_E2E_EXPECTED_AUDIENCE,
                           config.OAUTH_E2E_EXPECTED_ISSUER,
                           jwks_url=config.OAUTH_E2E_JWKS_URL)


def info_from_oAuth2(token):
    """
    Validate and decode token.
    Returned value will be passed in 'token_info' parameter of your operation function, if there is one.
    'sub' or 'uid' will be set in 'user' parameter of your operation function, if there is one.
    'scope' or 'scopes' will be passed to scope validation function.

    :param token Token provided by Authorization header
    :type token: str
    :return: Decoded token information or None if token is invalid
    :rtype: dict | None
    """
    result = my_jwkaas.get_connexion_token_info(token)
    g.ip = request.remote_addr

    # Check if e2e test token is configured
    if result is None and my_e2e_jwkaas is not None:
        token_info = my_e2e_jwkaas.get_connexion_token_info(token)
        if token_info is not None and 'appid' in token_info and token_info['appid'] == config.OAUTH_E2E_APPID:
            logging.warning('Approved e2e access token for appid [%s]', token_info['appid'])
            result = {'scopes': config.OAUTH_E2E_SCOPES, 'sub': 'e2e', 'upn': 'e2e-technical-user'}

    if result is not None:
        g.user = result.get('upn')
        g.token = result

    return result
