import json
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud import pubsub_v1
import config
from stg_updater import process_carsloc_msg, locations_to_stg
import os
import logging

logging.basicConfig(level=logging.INFO)

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCP_BUCKET_CAR_LOCATIONS)

car_licenses = {}
analyze_date = None


def callback_handle_message(carsloc_msg):
    global analyze_date
    global car_licenses
    carsloc_json = json.loads(carsloc_msg.data.decode())
    process_carsloc_msg(carsloc_json, car_licenses, analyze_date)
    carsloc_msg.ack()


def retrieve_and_parse_carsloc_msgs(request):
    global analyze_date
    global car_licenses
    # Get analyze date in utc
    now_utc = datetime.now(timezone.utc)
    analyze_date = now_utc.date()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(config.PUBSUB_PROJECT_ID, config.PUBSUB_SUBSCRIPTION_NAME)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback_handle_message)
    logging.info(f"Listening for messages on {subscription_path}...")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            streaming_pull_future.result(timeout=5)
        except Exception as e:
            streaming_pull_future.cancel()
            logging.info(f"Listening for messages on {subscription_path} threw an exception: {e}.")

    subscriber.close()

    file_name_locations = str(os.environ.get("FILE_NAME"))
    if not file_name_locations:
        logging.error("Required argument FILE_NAME missing")
    if not file_name_locations.endswith(".json"):
        logging.error("Argument FILE_NAME should have json extension")
    # Put locations in storage
    locations_to_stg(analyze_date, car_licenses, storage_client, storage_bucket, file_name_locations)


if __name__ == '__main__':
    retrieve_and_parse_carsloc_msgs(None)
