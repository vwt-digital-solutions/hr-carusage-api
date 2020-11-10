from google.cloud import firestore
import logging

logging.basicConfig(level=logging.INFO)


def upload_to_firestore(car_trips):
    db = firestore.Client()
    # Make a new trips variable so that actual trips is not changed
    for car in car_trips:
        car_license = car['license']
        car_license_hash = car['license_hash']
        for trip in car['trips']:
            # Get start and end time
            # Since there can be a lot of time between the first
            # stationary location and the first moving location
            # the first start time is the first moving location
            started_at = trip[1]['when']
            ended_at = trip[-1]['when']
            # Make firestore entity
            firestore_entity = {
                "started_at": started_at,
                "ended_at": ended_at,
                "license": car_license,
                "license_hash": car_license_hash,
                "locations": trip,
                "checking_info": {
                    "trip_kind": None,
                    "description": None
                },
                "outside_time_window": None
            }
            # Upload to firestore
            try:
                doc_ref = db.collection("Trips").document()
                doc_ref.set(firestore_entity)
            except Exception as e:
                logging.exception(f"Unable to upload trip of car {trip['license']} "
                                  f"because of {e}")
                return False
    return True
