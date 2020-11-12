import config
import os
import re
import base64
import logging

from openapi_server.controllers.content_controller import create_content_response
from flask import request, current_app, g, jsonify, make_response
from google.cloud import kms

logging.basicConfig(level=logging.INFO)


def check_database_configuration(request_method):
    existing_config = {
        'db_client': False if current_app.db_client is None else True,
        'db_table_name': False if g.db_table_name is None else True,
        'db_table_id': False if g.db_table_id is None else True,
        'response_keys': False if (request_method in ['get'] and g.response_keys is None) else True,
        'db_keys': False if (request_method in ['post', 'put'] and g.db_keys is None) else True
    }

    for key in existing_config:
        if not existing_config[key]:
            logging.debug(existing_config)
            return make_response(jsonify("Database information insufficient"), 500)


def check_identifier(kwargs):
    if g.request_id is None or g.request_id not in kwargs:
        return make_response(jsonify("Identifier name not found"), 500)


def kms_encrypt_decrypt_cursor(cursor, kms_type):
    if cursor and hasattr(config, 'KMS_KEY_INFO') and \
            'keyring' in config.KMS_KEY_INFO and \
            'key' in config.KMS_KEY_INFO and \
            'location' in config.KMS_KEY_INFO:
        project_id = os.environ['GOOGLE_CLOUD_PROJECT']
        location_id = config.KMS_KEY_INFO['location']
        key_ring_id = config.KMS_KEY_INFO['keyring']
        crypto_key_id = config.KMS_KEY_INFO['key']

        try:
            client = kms.KeyManagementServiceClient()
            name = client.crypto_key_path_path(project_id, location_id, key_ring_id, crypto_key_id)

            if kms_type == 'encrypt':
                encrypt_response = client.encrypt(name, cursor.encode() if isinstance(cursor, str) else cursor)
                response = base64.urlsafe_b64encode(encrypt_response.ciphertext).decode()
            else:
                encrypt_response = client.decrypt(name, base64.urlsafe_b64decode(cursor))
                response = encrypt_response.plaintext
        except Exception as e:
            logging.error(f"An exception occurred when {kms_type}-ing a cursor: {str(e)}")
            return None
    else:
        response = cursor

    return response


def generic_get_multiple():  # noqa: E501
    """Returns a array of entities

    :param kwargs: Keyword argument list
    :type kwargs: dict

    :rtype: array
    """

    # Check for Database configuration
    db_existence = check_database_configuration('get')
    if db_existence:
        return db_existence

    try:
        db_response = current_app.db_client.get_multiple(
            kind=g.db_table_name, db_keys=g.db_keys, res_keys=g.response_keys, filters=g.request_queries)
    except ValueError as e:
        return make_response(jsonify(str(e)), 400)

    if db_response:
        return create_content_response(db_response, request.content_type)

    return make_response(jsonify([]), 204)


def generic_get_multiple_page(**kwargs):  # noqa: E501
    """Returns a dict containing entities and pagination information

    :param kwargs: Keyword argument list
    :type kwargs: dict

    :rtype: array
    """

    # Check for Database configuration
    db_existence = check_database_configuration('get')
    if db_existence:
        return db_existence

    page_cursor = kms_encrypt_decrypt_cursor(kwargs.get('page_cursor', None), 'decrypt')
    page_size = kwargs.get('page_size', 50)
    page_action = kwargs.get('page_action', 'next')

    try:
        db_response = current_app.db_client.get_multiple_page(
            kind=g.db_table_name, db_keys=g.db_keys, res_keys=g.response_keys, filters=g.request_queries,
            page_cursor=page_cursor, page_size=page_size, page_action=page_action)
    except ValueError as e:
        return make_response(jsonify(str(e)), 400)

    if db_response:
        url_rule = re.sub(r'<.*?>', '', str(request.url_rule)).strip('/')
        host_url = config.BASE_URL.rstrip('/') if hasattr(config, 'BASE_URL') else \
            request.host_url.replace('http://', 'https://')

        if db_response.get('next_page'):
            next_cursor = kms_encrypt_decrypt_cursor(db_response.get('next_page'), 'encrypt')

            if not url_rule.endswith("/pages"):
                url_rule = f"{url_rule}/pages"

            db_response['next_page'] = f"{host_url}/{url_rule}/{next_cursor}?page_size={page_size}&page_action=next"
        else:
            db_response['next_page'] = None

        if page_cursor:
            prev_cursor = kms_encrypt_decrypt_cursor(page_cursor, 'encrypt')
            db_response['prev_page'] = f"{host_url}/{url_rule}/{prev_cursor}?page_size={page_size}&page_action=prev"
        else:
            db_response['prev_page'] = None

        return create_content_response(db_response, request.content_type)

    return make_response(jsonify([]), 204)
