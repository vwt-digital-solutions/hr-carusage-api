from google.cloud import firestore
from google.cloud import storage
import datetime
import logging
from pytz import timezone
import config
import json

logging.basicConfig(level=logging.INFO)


class AddFieldsToFirestoreEntities(object):
    def __init__(self):
        self.db = firestore.Client()
        self.collection = self.db.collection(config.collection)
        self.entities = self.get_entities()
        self.storage_client = storage.Client()
        self.storage_bucket = self.storage_client.get_bucket(config.GCP_BUCKET_CAR_INFORMATION)
        self.trip_information = self.init_trip_information()

    def init_trip_information(self):
        trip_info = {'drivers': None, 'business_units': None}

        if hasattr(config, 'DRIVERS_INFORMATION_PATH'):  # Retrieve drivers from storage
            if storage.Blob(bucket=self.storage_bucket, name=config.DRIVERS_INFORMATION_PATH).exists(self.storage_client):
                driver_information_blob_json_string = self.storage_bucket.get_blob(config.DRIVERS_INFORMATION_PATH).download_as_string()
                trip_info['drivers'] = json.loads(driver_information_blob_json_string)

        if hasattr(config, 'BUSINESSUNITS_INFORMATION_PATH'):  # Retrieve business units from storage
            if storage.Blob(bucket=self.storage_bucket, name=config.BUSINESSUNITS_INFORMATION_PATH).exists(self.storage_client):
                bu_information_blob_json_string = self.storage_bucket.get_blob(config.BUSINESSUNITS_INFORMATION_PATH).download_as_string()
                trip_info['business_units'] = json.loads(bu_information_blob_json_string)

        return trip_info

    def get_entities(self):
        entities = []
        # Query for documents
        for doc in self.collection.stream():
            # dict of entity:
            doc_dict = doc.to_dict()
            doc_dict['id'] = doc.id
            entities.append(doc_dict)
        return entities

    def update_firestore_entity(self, entity_id, field, value):
        doc_ref = self.collection.document(entity_id)
        doc_ref.update({field: value})

    def add_fields_to_collection(self):
        for entity in self.entities:
            # Mark entity if its start date are before a certain time window
            # The mark is "outside_time_window"
            self.mark_entity_outside_time_window(entity, "outside_time_window")

            if self.trip_information['drivers']:  # Add driver information to trip
                self.add_driver_info(entity)

        logging.info("Added fields to trips")

    def mark_entity_outside_time_window(self, entity, field):
        hour_before = config.time_window['start_time_hour']
        minutes_before = config.time_window['start_time_minutes']
        hour_after = config.time_window['end_time_hour']
        minutes_after = config.time_window['end_time_minutes']

        # time window is in Europe/Amsterdam time
        time_before = datetime.time(hour_before, minutes_before, tzinfo=timezone('Europe/Amsterdam'))
        time_after = datetime.time(hour_after, minutes_after, tzinfo=timezone('Europe/Amsterdam'))

        # Convert started_at and ended_at of trip to Europe/Amsterdam timezone
        started_at = entity['started_at'].astimezone(timezone('Europe/Amsterdam')).time()
        # ended_at = entity['ended_at'].astimezone(timezone('Europe/Amsterdam')).time()

        if started_at < time_before or started_at > time_after:
            # Entity started before begin time of time window or after end time of time window
            self.update_firestore_entity(entity['id'], field, True)

    def add_driver_info(self, entity):
        # Check if driver exists in driver information
        driver = self.trip_information['drivers'].get(entity['license'])
        if driver:
            driver["department"] = self.process_department(driver.get("department"))
            self.update_firestore_entity(entity['id'], "driver_info", driver)

    def process_department(self, department):
        if department:
            department_id = int(department['id']) if isinstance(department, dict) else int(department)
            if self.trip_information['business_units'] and \
                    self.trip_information['business_units'].get(str(department_id)):
                return self.trip_information['business_units'].get(str(department_id))
            else:
                return {'id': int(department_id)}
        else:
            return None
