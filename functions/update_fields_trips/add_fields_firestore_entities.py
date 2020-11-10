import logging
import config
import json
import random
import math

from google.cloud import firestore
from google.cloud import storage
from datetime import datetime, timedelta, timezone, time as dt_time
from pytz import timezone as py_timezone

logging.basicConfig(level=logging.INFO)


class AddFieldsToFirestoreEntities(object):
    def __init__(self):
        self.db_client = firestore.Client()
        self.storage_client = storage.Client()

        self.trip_information = self.init_trip_information()  # Retrieve all driver and business unit information

    def init_trip_information(self):
        trip_info = {'drivers': None, 'business_units': None}
        storage_bucket = self.storage_client.get_bucket(config.GCP_BUCKET_CAR_INFORMATION)

        if hasattr(config, 'DRIVERS_INFORMATION_PATH'):  # Retrieve drivers from storage
            if storage.Blob(bucket=storage_bucket, name=config.DRIVERS_INFORMATION_PATH).exists(
                    self.storage_client):
                driver_information_blob_json_string = storage_bucket.get_blob(
                    config.DRIVERS_INFORMATION_PATH).download_as_string()
                trip_info['drivers'] = json.loads(driver_information_blob_json_string)

        if hasattr(config, 'BUSINESSUNITS_INFORMATION_PATH'):  # Retrieve business units from storage
            if storage.Blob(bucket=storage_bucket, name=config.BUSINESSUNITS_INFORMATION_PATH).exists(
                    self.storage_client):
                bu_information_blob_json_string = storage_bucket.get_blob(
                    config.BUSINESSUNITS_INFORMATION_PATH).download_as_string()
                trip_info['business_units'] = json.loads(bu_information_blob_json_string)

        return trip_info

    def add_fields_to_collection(self):
        if not self.trip_information['drivers']:
            raise FileNotFoundError('No driver information found')

        today = datetime.now(timezone.utc)
        yesterday = today - timedelta(1)
        start_date = datetime(yesterday.year, yesterday.month, yesterday.day)
        end_date = datetime(today.year, today.month, today.day)

        batch_limit = 500
        batch_has_new_entities = True
        batch_last_reference = None

        count_out_time_window = 0
        count_in_time_window = 0
        count_driver = 0

        while batch_has_new_entities:
            query = self.db_client.collection(config.collection)

            query = query.where("ended_at", ">=", start_date)
            query = query.where("ended_at", "<", end_date)
            query = query.where("outside_time_window", "==", None)
            query = query.limit(batch_limit)

            if batch_last_reference:
                query = query.order_by("ended_at")
                query = query.start_after(batch_last_reference)

            docs = query.stream()

            if docs:
                batch = self.db_client.batch()  # Creating new batch
                docs_list = list(docs)

                # Get a percentage of trips as a sample
                sample_percentage = config.sample_percentage if hasattr(config, 'sample_percentage') else 0
                sample_amount = math.floor((sample_percentage * len(docs_list)) / 100.0)
                sampled_list = [doc.id for doc in random.sample(docs_list, sample_amount)] if sample_amount > 0 else []

                if len(docs_list) < batch_limit:
                    batch_has_new_entities = False

                for doc in docs_list:
                    new_fields = {}
                    doc_dict = doc.to_dict()

                    batch_last_reference = doc

                    if entity_outside_time_window(doc_dict['started_at']):
                        new_fields["outside_time_window"] = True
                        count_out_time_window += 1
                    elif doc.id in sampled_list:  # Mark trip from sample as "outside-time-window"
                        new_fields["outside_time_window"] = True
                        count_out_time_window += 1
                    else:
                        new_fields["outside_time_window"] = False
                        count_in_time_window += 1

                    driver = self.trip_information['drivers'].get(doc_dict['license'])
                    if driver:  # Add driver information to trip
                        new_fields["department"] = self.process_department(driver.get("department"))
                        new_fields["driver_info"] = driver
                        count_driver += 1

                    batch.update(doc.reference, new_fields)  # Add new fields to batch

                batch.commit()  # Committing changes within batch
            else:
                batch_has_new_entities = False

        logging.info(
            f"Marked {count_out_time_window} as 'outside time window', {count_in_time_window} as 'inside time window' "
            f"and updated {count_driver} with driver information")

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


def entity_outside_time_window(value):
    hour_before = config.time_window['start_time_hour']
    minutes_before = config.time_window['start_time_minutes']
    hour_after = config.time_window['end_time_hour']
    minutes_after = config.time_window['end_time_minutes']

    # time window is in Europe/Amsterdam time
    time_before = dt_time(hour_before, minutes_before, tzinfo=py_timezone('Europe/Amsterdam'))
    time_after = dt_time(hour_after, minutes_after, tzinfo=py_timezone('Europe/Amsterdam'))

    # Convert started_at and ended_at of trip to Europe/Amsterdam timezone
    started_at = value.astimezone(py_timezone('Europe/Amsterdam')).time()

    if started_at < time_before or started_at > time_after:
        return True

    return False
