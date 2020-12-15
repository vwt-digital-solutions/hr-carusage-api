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

        today = datetime.now(timezone.utc)
        yesterday = today - timedelta(1)
        self.start_date = datetime(yesterday.year, yesterday.month, yesterday.day)
        self.end_date = datetime(today.year, today.month, today.day)

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

    def mark_trips_time_window(self):
        batch_limit = 500
        batch_has_new_entities = True
        batch_last_reference = None

        count_out_time_window = 0
        count_in_time_window = 0
        count_driver = 0

        trips_in_time_window = []

        while batch_has_new_entities:
            query = self.db_client.collection(config.collection)

            query = query.where("ended_at", ">=", self.start_date)
            query = query.where("ended_at", "<", self.end_date)
            query = query.order_by("ended_at", direction="ASCENDING")

            if batch_last_reference:
                query = query.start_after(batch_last_reference)

            query = query.limit(batch_limit)
            docs = query.stream()

            if docs:
                batch = self.db_client.batch()  # Creating new batch
                docs_list = list(docs)

                if len(docs_list) < batch_limit:
                    batch_has_new_entities = False

                for doc in docs_list:
                    batch_last_reference = doc
                    doc_dict = doc.to_dict()

                    if doc_dict.get('outside_time_window') is None:
                        new_fields = {}

                        if entity_outside_time_window(doc_dict['started_at']):
                            new_fields["outside_time_window"] = True
                            count_out_time_window += 1
                        else:
                            new_fields["outside_time_window"] = False
                            trips_in_time_window.append(doc.reference)
                            count_in_time_window += 1

                        drivers_list = self.trip_information['drivers'].get(doc_dict['license'])
                        driver = self.process_driver(drivers_list, doc_dict['started_at'], doc_dict['ended_at'])

                        if driver:  # Add driver information to trip
                            new_fields["driver_info"] = driver
                            new_fields["department"] = self.process_department(driver.get("department_id"))
                            count_driver += 1
                        else:
                            new_fields["driver_info"] = None
                            new_fields["department"] = None

                        batch.update(doc.reference, new_fields)  # Add new fields to batch

                batch.commit()  # Committing changes within batch
            else:
                batch_has_new_entities = False

        logging.info(
            f"Marked {count_out_time_window} trips as 'outside time window' and {count_in_time_window} as "
            f"'inside time window'. Updated {count_driver} trips with driver information")

        # Calculate the sample amount based on the "incorrect" marked trips
        sample_percentage = config.sample_percentage if hasattr(config, 'sample_percentage') else 0
        sample_amount = math.ceil((sample_percentage * count_out_time_window) / 100.0)

        return trips_in_time_window, sample_amount

    def mark_trips_sample(self, trips_in_time_window, sample_amount):
        batch = self.db_client.batch()  # Creating new batch
        current_batch_count = 0

        for doc in random.sample(trips_in_time_window, sample_amount):
            if current_batch_count == 500:  # Committing current and creation new batch if batch is full
                batch.commit()
                batch = self.db_client.batch()
                current_batch_count = 0

            batch.update(doc, {'outside_time_window': True, 'sample': True})  # Add new fields to batch
            current_batch_count += 1

        batch.commit()  # Committing changes within batch

        logging.info(f"Marked a sample of {sample_amount} trips as 'outside time window'")

    def add_fields_to_collection(self):
        if not self.trip_information['drivers']:
            logging.info('No driver information found, aborting execution')
            return

        trips_in_time_window, sample_amount = self.mark_trips_time_window()  # Mark trips based on time window

        # Only mark sample of trips if trips inside time window exist
        if sample_amount > 0 and len(trips_in_time_window) > 0:
            self.mark_trips_sample(trips_in_time_window, sample_amount)

    @staticmethod
    def process_driver(drivers_list, trip_start, trip_end):
        if not drivers_list:
            return None
        if len(drivers_list) == 1:
            return drivers_list[0]

        cur_driver = None
        trip_start = convert_to_datetime(str(trip_start), 'google')
        trip_end = convert_to_datetime(str(trip_end), 'google')

        for driver in sorted(drivers_list, key=lambda i: i['driver_start_date'], reverse=True):
            driver_start = convert_to_datetime(driver['driver_start_date'])
            driver_end = convert_to_datetime(driver['driver_end_date']) if \
                driver['driver_end_date'] is not None else trip_end

            if driver_start <= trip_start and driver_end >= trip_end:
                cur_driver = driver

        return cur_driver

    def process_department(self, department):
        if department:
            department_id = int(department['id']) if isinstance(department, dict) else int(department)
            if self.trip_information['business_units'] and \
                    self.trip_information['business_units'].get(str(department_id)):
                return self.trip_information['business_units'].get(str(department_id))

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


def convert_to_datetime(string, type=None):
    format = '%Y-%m-%d %H:%M:%S%z' if type == 'google' else '%Y-%m-%dT%H:%M:%SZ'
    try:
        value = datetime.strptime(string, format)
    except (ValueError, TypeError, AttributeError):
        pass
        return None
    else:
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
