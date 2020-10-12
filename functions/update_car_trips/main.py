import json
from datetime import date
from google.cloud import storage
from google.cloud import pubsub_v1
from analyze_controller import process_carsloc_msg, build_trips
import config

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)

car_licenses = {}
car_starts = {}
license_info = {}
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

        # Make trips
        if car_licenses:
            cars = build_trips(car_licenses)
            for car in cars:
                if cars[car]['trips']:
                    print(f"Car trips for car {car}")
                    print(json.dumps(cars[car]['trips'], indent=2))
                    print("\n")
            print("Finished printing all currently completed trips")

        # print(f"Parsed messages for {analyze_date}, nr cars {len(car_starts)}")
        # if len(trips) > 0:
        #     print(f"Analyzing and storing car_starts of {analyze_date}")
        # Add trips to firestore
        # TODO


if __name__ == '__main__':
    retrieve_and_parse_carsloc_msgs(None)
