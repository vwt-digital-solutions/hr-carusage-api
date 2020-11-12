import config
import logging
import types

from datetime import datetime
from flask import g, request
from google.cloud import firestore
from openapi_server.abstractdatabase import DatabaseInterface, EntityParser


class FirestoreDatabase(DatabaseInterface):

    def __init__(self):
        self.db_client = firestore.Client()

    def process_audit_logging(self, old_data, new_data, entity_id):
        if hasattr(config, 'AUDIT_LOGS_NAME') and config.AUDIT_LOGS_NAME != "":
            try:
                old_data = old_data.to_dict() if type(old_data) != dict else old_data
                new_data = new_data.to_dict() if type(new_data) != dict else new_data

                changed = {}
                for attribute in list(set(old_data) | set(new_data)):
                    if attribute not in old_data:
                        changed[attribute] = {"new": new_data[attribute]}
                    elif attribute not in new_data:
                        changed[attribute] = {"old": old_data[attribute], "new": None}
                    elif old_data[attribute] != new_data[attribute]:
                        changed[attribute] = {"old": old_data[attribute], "new": new_data[attribute]}

                if changed:
                    doc_ref = self.db_client.collection(config.AUDIT_LOGS_NAME).document()
                    doc_ref.set({
                        "attributes_changed": changed,
                        "table_id": entity_id,
                        "table_name": g.db_table_name,
                        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + 'Z',
                        "user": g.user if g.user is not None else request.remote_addr
                    })
            except Exception as e:
                logging.error(f"An exception occurred when audit logging changes for entity '{entity_id}': {str(e)}")
                pass

    def get_single(self, id, kind, db_keys, res_keys):
        """Returns an entity as a dict

        :param id: A unique identifier
        :type id: str | int
        :param kind: Database kind of entity
        :type kind: str
        :param db_keys: List of keys for database entity
        :type kind: list
        :param res_keys: List of keys for response entity
        :type kind: list

        :rtype: dict
        """

        doc_ref = self.db_client.collection(kind).document(id)
        doc = doc_ref.get()

        if doc.exists:
            return create_response(res_keys, doc)

        return None

    def put_single(self, id, body, kind, db_keys, res_keys):
        """Updates an entity

        :param id: A unique identifier
        :type id: str | int
        :param body:
        :type body: dict
        :param kind: Database kind of entity
        :type kind: str
        :param db_keys: List of keys for database entity
        :type kind: list
        :param res_keys: List of keys for response entity
        :type kind: list

        :rtype: str
        """

        doc_ref = self.db_client.collection(kind).document(id)
        doc = doc_ref.get()

        if doc.exists:
            new_doc = EntityParser().parse(db_keys, body, 'put', id)
            doc_ref.update(new_doc)

            updated_doc = doc_ref.get()

            self.process_audit_logging(old_data=doc, new_data=updated_doc, entity_id=doc_ref.id)
            return create_response(res_keys, updated_doc)

        return None

    def post_single(self, body, kind, db_keys, res_keys):
        """Creates an entity

        :param body:
        :type body: dict
        :param kind: Database kind of entity
        :type kind: str
        :param db_keys: List of keys for database entity
        :type kind: list
        :param res_keys: List of keys for response entity
        :type kind: list

        :rtype: str
        """

        doc_ref = self.db_client.collection(kind).document()
        doc_ref.set(EntityParser().parse(db_keys, body, 'post', doc_ref.id))

        updated_doc = doc_ref.get()

        self.process_audit_logging(old_data={}, new_data=updated_doc, entity_id=doc_ref.id)

        return create_response(res_keys, updated_doc)

    def get_multiple(self, kind, db_keys, res_keys, filters):
        """Returns all entities as a list of dicts

        :param kind: Database kind of entity
        :type kind: str
        :param db_keys: List of keys for database entity
        :type kind: list
        :param res_keys: List of keys for response entity
        :type kind: list
        :param filters: List of query filters
        :type kind: list

        :rtype: array
        """

        docs_ref = self.create_db_query(kind, filters)
        docs = docs_ref.stream()

        if docs:
            return create_response(res_keys, docs)

        return None

    def get_multiple_page(self, kind, db_keys, res_keys, filters, page_cursor, page_size, page_action):
        """Returns all entities as a list of dicts

        :param kind: Database kind of entity
        :type kind: str
        :param db_keys: List of keys for database entity
        :type kind: list
        :param res_keys: List of keys for response entity
        :type kind: list
        :param filters: List of query filters
        :type kind: list
        :param page_cursor: The cursor for retrieve a specific page
        :type page_cursor: str
        :param page_size: The numbers of items within a page
        :type page_size: int
        :param page_action: Selector to get next or previous page based on the cursor
        :type page_action: str

        :rtype: dict
        """

        docs_ref = self.create_db_query(kind, filters)
        docs_ref = docs_ref.limit(page_size)

        if page_cursor:
            snapshot = self.db_client.collection(kind).document(page_cursor).get()
            if snapshot:
                docs_ref = docs_ref.end_at(snapshot) if page_action == 'prev' else docs_ref.start_after(snapshot)
            else:
                raise ValueError("Cursor is not valid")

        docs = [{'id': doc.id, **doc.to_dict()} for doc in docs_ref.stream()]

        response = {
            'results': res_keys['results']
        }

        # Return results
        if docs:
            response = create_response(response, docs)

            if page_action == 'prev':
                if g.db_table_id:
                    response['results'] = sorted(
                        response['results'], key=lambda i: i[g.db_table_id], reverse=True)
                next_cursor = page_cursor  # Grab current cursor for next page
            else:
                next_cursor = docs[-1]['id'] if self.check_for_next_document(docs[-1]['id'], kind) else None
        else:
            response['results'] = []
            next_cursor = None

        # Create response object
        response['status'] = 'success'
        response['page_size'] = page_size
        response['next_page'] = next_cursor

        return response

    def check_for_next_document(self, document_id, kind):
        snapshot = self.db_client.collection(kind).document(document_id).get()
        docs = self.db_client.collection(kind).limit(1).start_after(snapshot).stream()

        return True if len(list(docs)) > 0 else False

    def create_db_query(self, kind, filters):
        query = self.db_client.collection(kind)

        if filters:
            args = request.args.to_dict()

            for filter in filters:
                if filter['name'] in args:
                    filter_datatype = filter['schema']['format'] if \
                        filter['schema'].get('format') else filter['schema']['type']
                    filter_value = data_type_validator(args[filter['name']], filter_datatype)

                    if not filter_value:
                        raise ValueError(f"Value '{args[filter['name']]}' for query param "
                                         f"'{filter['name']}' is not of type '{filter_datatype}'")

                    query = query.where(filter['field'], filter['comparison'], filter_value)

        return query


def data_type_validator(value, type):
    try:
        if not type:
            return value
        if type == 'integer' or type == 'number':
            value = int(value)
        if type == 'boolean':
            value = bool(value)
        if type == 'date-time':
            value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        if type == 'date':
            value = datetime.strptime(value, "%Y-%m-%d")
    except Exception:
        pass
        return None
    else:
        return value


def create_response(keys, data):
    if isinstance(data, types.GeneratorType) or isinstance(data, list):
        return_object = {}
        for key in keys:
            if type(keys[key]) == dict:
                return_object[key] = [EntityParser().parse(keys[key], entity, 'get', entity.id) for entity in data]

        return return_object

    return EntityParser().parse(keys, data, 'get', data.id)
