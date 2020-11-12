# flake8: noqa

import operator

from abc import ABC, abstractmethod
from flask import g
from functools import reduce


class DatabaseInterface(ABC):

    @abstractmethod
    def process_audit_logging(self, old_data, new_data):
        pass

    @abstractmethod
    def get_single(self, id, kind, db_keys, res_keys):
        pass

    @abstractmethod
    def put_single(self, id, body, kind, db_keys, res_keys):
        pass

    @abstractmethod
    def post_single(self, body, kind, db_keys, res_keys):
        pass

    @abstractmethod
    def get_multiple(self, kind, db_keys, res_keys, filters):
        pass

    @abstractmethod
    def get_multiple_page(self, kind, db_keys, res_keys, filters, page_cursor, page_size, page_action):
        pass


class EntityParser:

    def __init__(self):
        pass

    def parse(self, keys, entity, method, entity_id):
        if method == 'get':
            return self.create_object(keys, entity, entity_id, False)
        else:
            new_object = self.create_object(keys, entity, entity_id, True)
            return self.create_update_object(keys, new_object)

    def create_object(self, keys, entity, entity_id, value_bound):
        entity_to_return = {}
        for key in keys:
            if key == '_target':
                continue

            if key == g.db_table_id:
                entity_to_return[key] = entity_id
                continue

            if isinstance(keys[key], dict) and '_properties' in keys[key]:
                try:
                    new_entity = entity.get(key)
                except (KeyError, AttributeError):
                    new_entity = {}

                nested_object = self.create_object(keys[key]['_properties'], new_entity, entity_id, value_bound)
                if nested_object:
                    entity_to_return[key] = nested_object
            else:
                try:
                    entity_to_return[key] = entity.get(key)
                except (KeyError, AttributeError):
                    if value_bound and not keys[key].get('required', False):
                        continue

                    entity_to_return[key] = None

            if value_bound and keys[key].get('required', False) and not entity.get(key, None):
                raise ValueError(f"Property '{key}' is required")

        return entity_to_return

    def create_update_object(self, keys, entity):
        target_keys = self.flatten(d=keys, sep='.')

        entity_to_return = {}
        for key in target_keys:
            try:
                value = get_from_dict(entity, key.split('.'))
            except KeyError:
                continue
            else:
                entity_to_return = self.create_nested_object(target_keys[key], value, entity_to_return)

        return entity_to_return

    def create_nested_object(self, key_list, value, object):
        key = key_list[0]
        if len(key_list) > 1:
            if key not in object:
                object[key] = {}

            object[key] = self.create_nested_object(key_list[1:], value, object[key])
        else:
            object[key] = value

        return object

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict) and '_properties' in v:
                items.extend(self.flatten(v['_properties'], new_key, sep=sep).items())
            else:
                items.append((new_key, v['_target']))
        return dict(items)


def get_from_dict(data_dict, map_list):
    """Returns a dictionary based on a mapping"""
    return reduce(operator.getitem, map_list, data_dict)
