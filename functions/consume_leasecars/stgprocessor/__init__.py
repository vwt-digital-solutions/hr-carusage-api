import os
import json
import logging

from datetime import datetime
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def process(payload):
    if len(payload.get('leasecars', [])) > 0:
        stg_processor = STGProcessor()
        cur_cars = stg_processor.retrieve_file()
        updated_cars = 0

        for car in payload['leasecars']:
            license = car.get('car_license')
            has_valid_fields = check_object_keys(license, car)
            if not license or car.get('car_kind') != 'bedrijfsauto' or not has_valid_fields:
                continue

            if license in cur_cars:
                cur_cars[license], exists = check_if_object_exist(cur_cars[license], license, car)

                if not exists:
                    cur_cars[license].append(create_object(license, car))
            else:
                cur_cars[license] = [create_object(license, car)]

            updated_cars += 1

        for key in cur_cars:  # Sort drivers on start date
            cur_cars[key] = sorted(cur_cars[key], key=lambda i: i['driver_start_date'], reverse=True)

        if updated_cars > 0:
            stg_processor.update_file(cur_cars)

        logging.info(f"Updated {updated_cars} lease cars")


def check_object_keys(license, car):
    existing_config = {
        'driver_employee_number': False if not car.get('driver_employee_number') else True,
        'driver_start_date': False if not car.get('driver_start_date') else True,
        'department_id': False if not car.get('department_id') else True
    }

    for key in existing_config:
        if not existing_config[key]:
            logging.info(f"Car '{license}' is missing values: {existing_config}")
            return False

    return True


def check_if_object_exist(list, key, n_object):
    exists = False
    for index, e_object in enumerate(list):
        if convert_to_int(e_object['driver_employee_number']) == \
                convert_to_int(n_object['driver_employee_number']) and \
                convert_to_datetime(e_object['driver_start_date']) >= \
                convert_to_datetime(n_object['driver_start_date']):
            list[index] = create_object(key, n_object)
            exists = True

    return list, exists


def create_object(key, dict):
    return {
        "car_brand_name": dict.get('car_brand_name'),
        "car_brand_type": dict.get('car_brand_type'),
        "license": key,
        "driver_employee_number": convert_to_int(dict.get('driver_employee_number')),
        "driver_mail": dict.get('driver_mail'),
        "driver_initials_name": dict.get('driver_initials_name'),
        "driver_first_name": dict.get('driver_first_name'),
        "driver_prefix_name": dict.get('driver_prefix_name'),
        "driver_last_name": dict.get('driver_last_name'),
        "driver_start_date": convert_to_datetime_string(dict.get('driver_start_date')),
        "driver_end_date": convert_to_datetime_string(dict.get('driver_end_date')),
        "department_id": convert_to_int(dict.get('department_id')),
        "department_name": dict.get('department_name'),
    }


def convert_to_int(string):
    try:
        value = int(string)
    except (ValueError, TypeError, AttributeError):
        pass
        return None
    else:
        return value


def convert_to_datetime(string):
    try:
        value = datetime.strptime(string, "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, AttributeError):
        pass
        return None
    else:
        return value


def convert_to_datetime_string(string):
    try:
        value = convert_to_datetime(string).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, AttributeError):
        pass
        return None
    else:
        return value


class STGProcessor(object):
    def __init__(self):
        for key in ['BUCKET_NAME', 'BLOB_NAME']:
            if key not in os.environ:
                raise EnvironmentError(f"Function missing the required environment variable '{key}'")

        self.storage_client = storage.Client()
        self.bucket_name = os.environ.get('BUCKET_NAME')
        self.bucket = self.storage_client.get_bucket(self.bucket_name)
        self.blob_name = os.environ.get('BLOB_NAME')

    def retrieve_file(self):
        if storage.Blob(bucket=self.bucket, name=self.blob_name).exists(self.storage_client):
            blob = self.bucket.get_blob(self.blob_name)
            blob_json = json.loads(blob.download_as_string())
            return blob_json

        return {}

    def update_file(self, json_dict):
        blob = self.bucket.blob(self.blob_name)
        blob.upload_from_string(json.dumps(json_dict), content_type='application/json')
