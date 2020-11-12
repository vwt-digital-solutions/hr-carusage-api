# flake8: noqa

import yaml
import re
import operator
import logging

from functools import reduce
from flask import request

OPENAPI_PATH = "openapi_server/openapi/openapi.yaml"


def get_from_dict(data_dict, map_list):
    """Returns a dictionary based on a mapping"""
    return reduce(operator.getitem, map_list, data_dict)


def get_specification():
    """Returns the OpenAPI specification as JSON"""
    with open(OPENAPI_PATH, 'r') as openapi:
        spec = yaml.safe_load(openapi.read())

    return spec


def transform_url_rule(url_rule):
    """Returns the path from the current request"""
    new_url_rule = str(url_rule).replace('int:', '')
    new_url_rule = str(new_url_rule).translate(str.maketrans('<>', '{}'))

    return new_url_rule


def get_path_schema_reference(path_object, object_type):
    """Returns the schema-reference from the current path item object"""
    if object_type == 'responses' and object_type in path_object:
        for code in ['200', '201', '202', '203', '204']:
            if code in path_object['responses']:
                content_type = request.content_type if request.content_type else 'application/json'
                try:
                    route_scheme_ref = get_from_dict(
                        path_object['responses'][code], ['content', content_type, 'schema', '$ref'])
                except KeyError:
                    pass
                    raise ValueError(f"The content-type '{content_type}' is not found within the specification")
                else:
                    return route_scheme_ref
    elif object_type == 'requestBody' and object_type in path_object:
        try:
            route_scheme_ref = get_from_dict(
                path_object['requestBody'], ['content', 'application/json', 'schema', '$ref'])
        except KeyError:
            pass
        else:
            return route_scheme_ref
    return None


def get_schema(spec, reference):
    """Returns the schema object based on a reference"""
    if reference and re.search(r"(?:#/)(.+)", reference):
        reference_list = reference.replace('#/', '').split('/')

        try:
            route_schema = get_from_dict(spec, reference_list)
        except KeyError:
            pass
        else:
            return route_schema

    return None


def get_schema_properties(spec, schema, request_method):
    """Returns all properties as {name: type} from a schema"""
    if not schema:
        return None

    schema_properties = schema.get('properties', {})
    properties = {}
    required_properties = schema.get('required', [])

    for field in schema_properties:
        if 'x-target-field' in schema_properties[field]:
            target_field = schema_properties[field]['x-target-field']
        elif '_target' not in schema_properties[field]:
            target_field = field
        else:
            target_field = None

        if schema_properties[field].get('type') in ['array', 'dict'] and '$ref' not in schema_properties[field]:
            for key in schema_properties[field]:
                if type(schema_properties[field][key]) == dict and '$ref' in schema_properties[field][key]:
                    nested_schema = get_schema(spec, schema_properties[field][key]['$ref'])
                    properties[field] = get_schema_properties(spec, nested_schema, request_method)
                    break

            if field not in properties:
                properties[field] = schema_properties[field]
        else:
            if '$ref' in schema_properties[field]:
                nested_schema_properties = get_schema(spec, schema_properties[field]['$ref'])

                if 'properties' in nested_schema_properties:
                    properties[field] = {
                        '_target': nested_schema_properties.get('x-target-field', field),
                        '_properties': get_schema_properties(spec, nested_schema_properties, request_method)
                    }
            else:
                properties[field] = schema_properties[field]

        if 'properties' in schema_properties[field]:
            properties[field] = {
                '_properties': schema_properties[field]['properties']
            }

        if target_field:
            properties[field]['_target'] = target_field.split('.')

        if 'x-target-field' in properties[field]:
            del properties[field]['x-target-field']

        if field in required_properties:
            properties[field]['required'] = True

    return properties if len(properties) > 0 else None


def get_schema_id(spec, schema):
    """Returns the x-db-table-id from a schema"""
    schema_id = None

    if 'x-db-table-id' in schema:
        schema_id = schema['x-db-table-id']
    else:
        schema_properties = schema.get('properties')
        for field in schema_properties:
            if schema_properties[field]['type'] in ['array', 'dict']:
                for key in schema_properties[field]:
                    if type(schema_properties[field][key]) == dict and '$ref' in schema_properties[field][key]:
                        nested_schema = get_schema(spec, schema_properties[field][key]['$ref'])
                        schema_id = get_schema_id(spec, nested_schema)

    return schema_id


def get_request_id(path_item_object):
    """Returns the first request parameter name"""
    if 'parameters' in path_item_object and 'name' in path_item_object['parameters'][0] and \
            path_item_object['parameters'][0]['name'] not in ['page_cursor', 'page_size', 'page_action'] and \
            path_item_object['parameters'][0]['in'] == 'path':
        return path_item_object['parameters'][0]['name']

    return None


def get_request_query_filters(spec, path_item_object):
    """Returns all query filters from the request"""
    query_filters = []
    comparisons = {
        'equal_to': '==', 'not_equal_to': '!=', 'less_than': '<', 'less_than_or_equal_to': '<=', 'greater_than': '>',
        'greater_than_or_equal_to': '>='}

    for filter in path_item_object.get('parameters', []):
        filter = get_schema(spec, filter['$ref']) if '$ref' in filter else filter

        if filter['in'] == 'query' and filter['name'] not in ['page_cursor', 'page_size', 'page_action']:
            for key in ['schema', 'x-query-filter-comparison', 'x-query-filter-field']:
                if key not in filter:
                    logging.info(f"Error: query param '{filter['name']}' is missing the required '{key}'")
                    continue

            if filter['x-query-filter-comparison'] not in comparisons:
                logging.info(
                    f"Error: query param '{filter['name']}' has a not supported comparison: "
                    f"'{filter['x-query-filter-comparison']}'")
                continue

            if filter['schema'].get('type') not in ['string', 'number', 'integer', 'boolean']:
                logging.info(
                    f"Error: query param '{filter['name']}' has a not supported type: "
                    f"'{filter['schema'].get('type')}'")
                continue

            query_filters.append({
                'comparison': comparisons[filter['x-query-filter-comparison']],
                'field': filter['x-query-filter-field'],
                'name': filter['name'],
                'schema': filter['schema'],
                'required': bool(filter.get('required', False))
            })

    return query_filters


def get_database_info(request):
    """Returns the all database info"""
    db_table_name = None
    db_table_id = None
    db_keys = None
    response_keys = None
    request_id = None
    request_queries = None

    spec = get_specification()
    request_method = str(request.method).lower()
    path_object = spec.get('paths', {}).get(transform_url_rule(request.url_rule), None)

    if path_object and request_method in path_object:
        path_item_object = path_object[request_method]
        db_table_name = path_object.get('x-db-table-name', None)

        request_id = get_request_id(path_item_object)
        request_queries = get_request_query_filters(spec, path_item_object)

        db_path_schema = get_schema(spec, get_path_schema_reference(path_item_object, 'requestBody'))
        db_keys = get_schema_properties(spec, db_path_schema, request_method)

        response_path_schema = get_schema(spec, get_path_schema_reference(path_item_object, 'responses'))
        response_keys = get_schema_properties(spec, response_path_schema, request_method)

        db_table_id = get_schema_id(spec, response_path_schema if request_method == 'get' else db_path_schema)

    return db_table_name, db_table_id, db_keys, response_keys, request_id, request_queries
