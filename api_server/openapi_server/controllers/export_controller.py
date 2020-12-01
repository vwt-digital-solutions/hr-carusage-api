import logging
import config
import json
import operator

from flask import g
from functools import reduce
from datetime import datetime, timedelta
from gobits import Gobits
from hashlib import sha256

from google.cloud import firestore, pubsub_v1


class ExportProcessor(object):
    def __init__(self, ended_after, ended_before):
        self.db_client = firestore.Client()

        self.ended_after = datetime.strptime(ended_after, "%Y-%m-%dT%H:%M:%SZ")
        self.ended_before = datetime.strptime(ended_before, "%Y-%m-%dT%H:%M:%SZ")

        self.collection_trips = config.COLLECTION_NAME
        self.collection_fo = config.COLLECTION_NAME_FREQ_OFF
        self.collection_audit = config.AUDIT_LOGS_NAME

    def get_trips_to_export(self):
        trips_to_export = []

        batch_limit = 50
        batch_has_new_entities = True
        batch_last_reference = None

        all_trips_marked = True

        while batch_has_new_entities:
            query = self.db_client.collection(self.collection_trips)

            query = query.where('ended_at', '>=', self.ended_after)
            query = query.where('ended_at', '<=', self.ended_before)
            query = query.where('outside_time_window', '==', True)
            query = query.where('department.manager_mail', '==', g.user)
            query = query.order_by("ended_at", "ASCENDING")
            query = query.limit(batch_limit)

            if batch_last_reference:
                query = query.start_after(batch_last_reference)

            docs = query.stream()

            if docs:
                docs_list = list(docs)

                if len(docs_list) < batch_limit:
                    batch_has_new_entities = False
                else:
                    batch_last_reference = docs_list[-1]

                for doc in docs_list:
                    doc_dict = doc.to_dict()

                    is_exported = get_from_dict(doc_dict, ['exported', 'exported_at'])  # Check if trip is exported
                    is_marked = get_from_dict(
                        doc_dict, ['checking_info', 'trip_kind']) in ['work', 'personal']  # Check if trip is marked

                    if is_exported:  # Skip if trip is already exported
                        continue
                    elif not is_marked:  # Break if trip is not marked yet
                        all_trips_marked = False
                        batch_has_new_entities = False
                        break
                    else:  # Append trip to list for export
                        for loc in doc_dict['locations']:
                            loc['when'] = loc['when'].strftime('%Y-%m-%dT%H:%M:%SZ')

                        doc_dict['started_at'] = doc_dict['started_at'].strftime('%Y-%m-%dT%H:%M:%SZ')
                        doc_dict['ended_at'] = doc_dict['ended_at'].strftime('%Y-%m-%dT%H:%M:%SZ')
                        doc_dict['doc_id'] = doc.id
                        doc_dict['doc_reference'] = doc.reference
                        trips_to_export.append(doc_dict)
            else:
                batch_has_new_entities = False

        return trips_to_export, all_trips_marked

    @staticmethod
    def get_new_frequent_offenders(trips_active):
        current_fo = {}

        for trip in trips_active:
            trip_kind = get_from_dict(trip, ['checking_info', 'trip_kind'])
            driver_id = get_from_dict(trip, ['driver_info', 'driver_employee_number'])

            if trip_kind != 'personal' or not driver_id:
                continue

            fo_id = sha256(str(driver_id).encode('utf-8')).hexdigest()

            if fo_id in current_fo:
                current_fo[fo_id]['trips'].append(get_fo_trip(trip))
            else:
                new_fo = {
                    'department': {
                        'department_id': get_from_dict(trip, ['department', 'department_id']),
                        'department_name': get_from_dict(trip, ['department', 'department_name']),
                        'manager_mail': get_from_dict(trip, ['department', 'manager_mail']),
                    },
                    'driver_info': {
                        'driver_employee_number': driver_id,
                        'driver_first_name': get_from_dict(trip, ['driver_info', 'driver_first_name']),
                        'driver_last_name': get_from_dict(trip, ['driver_info', 'driver_last_name']),
                        'driver_mail': get_from_dict(trip, ['driver_info', 'driver_mail']),
                    },
                    'trips': [get_fo_trip(trip)]
                }
                current_fo[fo_id] = new_fo

        return current_fo

    def get_existing_frequent_offenders(self):
        existing_fo = {}
        query = self.db_client.collection(self.collection_fo)
        query = query.where('department.manager_mail', '==', g.user)
        docs = query.stream()

        if docs:
            fo_time_window = self.ended_after - timedelta(weeks=8)

            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict['doc_id'] = doc.id
                doc_dict['doc_reference'] = doc.reference

                active_trips = []
                for trip in doc_dict['trips']:
                    if datetime.strptime(trip['ended_at'], '%Y-%m-%dT%H:%M:%SZ') >= fo_time_window:
                        active_trips.append(trip)

                doc_dict['trips'] = active_trips

                existing_fo[doc.id] = doc_dict

        return existing_fo

    def process_frequent_offenders(self, trips_active):
        fo_existing = self.get_existing_frequent_offenders()  # Current frequent offenders
        fo_new = self.get_new_frequent_offenders(trips_active)  # New frequent offenders

        fo_to_update = {}
        fo_active = {}

        for fo_id in fo_new:
            if fo_id in fo_active:  # If FO has already more than 3 trips
                fo_active[fo_id]['trips'] = fo_active[fo_id]['trips'] + fo_new[fo_id]['trips']
                continue

            if fo_id in fo_existing:  # If FO already exists in DB
                fo_existing[fo_id]['trips'] = fo_existing[fo_id]['trips'] + fo_new[fo_id]['trips']

                if len(fo_existing[fo_id]['trips']) >= 3:  # If FO in DB has more than 3 trips
                    fo_active[fo_id] = fo_existing[fo_id]
                    fo_to_update[fo_id] = 'delete'
                else:
                    fo_to_update[fo_id] = 'update'
            else:
                if len(fo_new[fo_id]['trips']) >= 3:  # If new FO has more than 3 trips
                    fo_active[fo_id] = fo_new[fo_id]
                else:
                    fo_existing[fo_id] = fo_new[fo_id]
                    fo_to_update[fo_id] = 'add'

        return fo_active, fo_existing, fo_to_update

    def update_entities(self, fo_existing, fo_to_update, trips_to_export):
        transaction = self.db_client.transaction()
        update_in_transaction(transaction, self.db_client, self.collection_fo, self.collection_trips,
                              self.collection_audit, fo_existing, fo_to_update, trips_to_export)

    def exported_trips_to_topic(self, response):
        trip_batches = chunks(response, 50)
        for trip_batch in trip_batches:
            topic_response = self.to_topic(trip_batch)
            if topic_response is False:
                return False
        return True

    @staticmethod
    def to_topic(batch):
        batch_to_publish = []
        for item in batch:
            batch_item = {}
            for key in item:
                if key not in ['doc_id', 'doc_reference']:
                    batch_item[key] = item[key]

            batch_to_publish.append(batch_item)

        try:
            gobits = Gobits()  # Get gobits
            publisher = pubsub_v1.PublisherClient()  # Publish to topic

            topic_path = f"projects/{config.PROJECT_ID_TOPIC}/topics/{config.TOPIC_NAME}"
            msg = {
                "gobits": [gobits.to_json()],
                "trips": batch_to_publish
            }

            future = publisher.publish(topic_path, bytes(json.dumps(msg).encode('utf-8')))
            future.add_done_callback(lambda x: logging.debug(f"Published {len(batch_to_publish)} exported trips"))
        except Exception as e:
            logging.exception(f"Unable to publish exported trips to topic because of {str(e)}")
            return False
        else:
            return True


@firestore.transactional
def update_in_transaction(transaction, db_client, collection_fo, collection_trips, collection_audit, fo_existing,
                          fo_to_update, trips_to_export):
    for fo_id in fo_to_update:  # Create, update or delete FO from table
        if fo_to_update[fo_id] == 'delete':
            transaction.delete(fo_id)
        elif fo_to_update[fo_id] == 'update':
            transaction.update(fo_id, {'trips': fo_existing[fo_id]['trips']})
        else:
            doc_ref = db_client.collection(collection_fo).document(fo_id)
            transaction.create(doc_ref, fo_existing[fo_id])

    time_now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    exported_field = {
        "exported": {
            "exported_at": time_now,
            "exported_by": g.user
        }
    }

    for trip in trips_to_export:
        transaction.update(trip['doc_reference'], exported_field)  # Update exported status on trips

        # Create audit logging for trip
        audit_log = {
            "attributes_changed": {
                "exported": {
                    "new": exported_field["exported"]
                }
            },
            "table_id": trip['doc_id'],
            "table_name": collection_trips,
            "timestamp": time_now,
            "user": g.user
        }
        doc_ref = db_client.collection(collection_audit).document()
        transaction.create(doc_ref, audit_log)


def get_fo_trip(trip):
    return {
        "ended_at": get_from_dict(trip, ['ended_at']),
        "license": get_from_dict(trip, ['license']),
        "started_at": get_from_dict(trip, ['started_at']),
        "trip_kind": get_from_dict(trip, ['checking_info', 'trip_kind']),
        "trip_description": get_from_dict(trip, ['checking_info', 'description'])
    }


def get_from_dict(data_dict, map_list):
    """Returns a dictionary based on a mapping"""
    try:
        return reduce(operator.getitem, map_list, data_dict)
    except (KeyError, AttributeError):
        return None


def chunks(the_list, n):
    n = max(1, n)
    return (the_list[i:i + n] for i in range(0, len(the_list), n))
