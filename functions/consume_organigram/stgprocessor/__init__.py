import os
import json
import logging

from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def process(payload):
    if len(payload.get('organigram', [])) > 0:
        stg_processor = STGProcessor()
        cur_orgs = stg_processor.retrieve_file()
        len_old_orgs = len(cur_orgs)

        for org in payload['organigram']:
            department_id = convert_to_int(org.get('department_id'))
            cur_orgs[str(department_id)] = {
                "parent_id": convert_to_int(org.get('department_parent_id')),
                "id": department_id,
                "name": org.get('department_name'),
                "manager_name": org.get('manager_name'),
                "manager_mail": org.get('manager_mail')
            }

        len_new_orgs = len(cur_orgs) - len_old_orgs
        len_upd_orgs = len(payload['organigram']) - len_new_orgs

        stg_processor.update_file(cur_orgs)
        logging.info(f"Added {len_new_orgs} departments, updated {len_upd_orgs}")


def convert_to_int(string):
    try:
        value = int(string)
    except ValueError:
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
