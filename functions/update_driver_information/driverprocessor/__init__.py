import json
from google.cloud import storage
import config
import logging
import os

logging.basicConfig(level=logging.INFO)


class DriverProcessor(object):
    def __init__(self):
        self.meta = config.DRIVER_INFORMATION_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.storage_client = storage.Client()
        self.storage_bucket = self.storage_client.get_bucket(config.GCP_BUCKET_CAR_INFORMATION)

    def process(self, payload):
        selector_data = payload.get(os.environ.get('DATA_SELECTOR', 'Required parameter is missing'), [])
        # Check if drivers information exists
        blob_name = f"{config.DRIVERS_INFORMATION_PATH}"
        if storage.Blob(bucket=self.storage_bucket, name=blob_name).exists(self.storage_client):
            # Get blob
            blob = self.storage_bucket.get_blob(blob_name)
            # Convert to string
            blob_json_string = blob.download_as_string()
            # Convert to json
            blob_json = json.loads(blob_json_string)
            # Go through list of updated drivers gotten by topic message
            for driver in selector_data:
                # print driver
                logging.info(f"driver is: {json.dumps(driver)}")
                # For every updated driver, update driver info
                driver_json = {
                    "last_name": driver['Achternaam'],
                    "department": driver['Afdeling'],
                    "function_name": driver['Functienaam'],
                    "registration_number": driver['Registratienr'],
                    "prefix": driver['Tussenvg'],
                    "initial": driver['Voorletter']
                }
                blob_json['Kenteken'] = driver_json
                # log blob json
                logging.info(f"blob_json is: {json.dumps(blob_json)}")
            # Update blob
            new_blob = self.storage_bucket.blob(blob_name)
            new_blob.upload_from_string(
                data=json.dumps(blob_json, indent=2),
                content_type='application/json'
            )
        else:
            logging.error(f"File {blob_name} does not exist, cannot update file")
