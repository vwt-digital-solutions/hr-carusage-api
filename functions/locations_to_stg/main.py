import json
from datetime import date
from google.cloud import storage
from google.cloud import pubsub_v1
import config
from stg_updater import process_carsloc_msg, locations_to_stg

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET_CAR_LOCATIONS)

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
    analyze_date = date.today()

    if analyze_date.weekday() < 5:

        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(config.PUBSUB_PROJECT_ID, config.PUBSUB_SUBSCRIPTION_NAME)
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback_handle_message)
        print(f"Listening for messages on {subscription_path}...")

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                streaming_pull_future.result(timeout=5)
            except Exception as e:
                streaming_pull_future.cancel()
                print(f"Listening for messages on {subscription_path} threw an exception: {e}.")

        subscriber.close()

        # Put locations in storage
        locations_to_stg(car_licenses, storage_client, storage_bucket)


if __name__ == '__main__':
    retrieve_and_parse_carsloc_msgs(None)
