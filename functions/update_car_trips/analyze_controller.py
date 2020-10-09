import json
import math
import random
from datetime import datetime
from google.cloud import storage

import config

random.seed()
storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET)

start_date = datetime.strptime(config.ANALYZE_START_DATE, '%Y-%m-%d').date()


def process_carsloc_msg(carsloc_msg, car_licenses, analyze_date):
    # List of locations gotten from message
    carsmsglocations_list = carsloc_msg['carlocations']
    # For every location in the message
    for loc in carsmsglocations_list:
        # Skip location if it's not today's date
        when = datetime.strptime(loc['when'], "%Y-%m-%dT%H:%M:%SZ")
        if when.date() != analyze_date:
            print(f"Skipping message for {when.date()} while processing {analyze_date}")
            continue
        # Else check if the car's license is already in car_locations
        car = car_licenses.get(loc['license'], None)
        # If it is
        if car:
            car_loc = {
                "when": loc['when'],
                "geometry": loc['geometry'],
                "what": loc['what']
            }
            # Add car location to the locations key
            car['locations'].append(car_loc)
            # Also sort car's locations on time
            car['locations'] = sorted(car['locations'],
                                      key=lambda k: k.get('when', 0), reverse=False)
        # If it is not
        else:
            # Add the car location with its license to the car_locations
            car_loc = {
                "when": loc['when'],
                "geometry": loc['geometry'],
                "what": loc['what']
            }
            car = {
                loc['license']: {
                    "locations": [car_loc],
                    "trips": []
                }
            }
            car_licenses.update(car)


def build_trips(car_licenses):
    # Get all the trips that have not yet finished
    unfinished_trips = load_unfinished_trips()
    # For every license
    for car_license in car_licenses:
        # Make new trips list
        trips = []
        # Make a new trip
        trip = []
        # Go through the locations of the car
        for i in range(len(car_licenses[car_license]['locations'])):
            # Check if the car is stationary and after that moving
            if i + 1 < len(car_licenses[car_license]['locations'])-1:
                if car_licenses[car_license]['locations'][i] == "Stationary" and \
                   car_licenses[car_license]['locations'][i+1] == "Moving":
                    # A new trip has started
                    trip = [car_licenses[car_license]['locations'][i]]
            # Check if the car is moving
            elif car_licenses[car_license]['locations'][i]['what'] == 'Moving':
                # Add it to the trip
                trip.append(car_licenses[car_license]['locations'][i])
                # If it is the last location in the list
                # The trip will finish somewhere in the next batches
                # Just add it as an unfinished trip
                if i == len(car_licenses[car_license]['locations'])-1:
                    trips.append(trip)
                    trip = []
            elif i - 1 >= 0:
                if car_licenses[car_license]['locations'][i]['what'] == 'Stationary' and \
                   car_licenses[car_license]['locations'][i-1]['what'] == 'Moving':
                    # Add it to the trip
                    trip.append(car_licenses[car_license]['locations'][i])
                    # Add trip to trips
                    trips.append(trip)
                    # And make trip empty again
                    trip = []
        # Add unfinished trips to the GCP storage
        trips_to_remove = []
        for i in range(len(trips)):
            # If the last location of a trip is moving
            if trips[i][-1]['what'] == "Moving":
                # The trip has not finished yet
                # and should be added to the GCP storage containing
                # unfinished trips
                blob_name = f"{car_license}.json"
                add_unfinished_trip_to_storage(trips[i], blob_name)
                trips_to_remove.append(i)
        # Remove unfinished trips from trips list
        for i in trips_to_remove:
            trips.pop(i)
        # Check if the first location of a trip is moving
        # Because if it is, the start of the trip has been in another batch
        trips = patch_trips(unfinished_trips, trips, car_license)
        # Add trips to the trips key of the car
        car_licenses[car_license]['trips'] = trips
    # return car_licenses
    return car_licenses


def patch_trips(unfinished_trips, trips, car_license):
    to_rem = []
    patch_complete = True
    for t in range(len(trips)):
        if trips[t][0]['what'] == 'Moving':
            # Check if the current car is in the unfinished trips storage
            unfinished_trip_stg = unfinished_trips.get(car_license, None)
            if unfinished_trip_stg:
                # Find part that comes before current trip to patch
                # Closest time to current trip
                smallest_time_diff = math.inf
                closest_trip_in_time = math.inf
                current_trip_timestamp = trips[t][0]['when']
                current_trip_time = datetime.datetime.strptime(
                    current_trip_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                # For every unfinished trip of this car license
                for i in range(len(unfinished_trip_stg['trips'])):
                    # Calculate the time difference
                    unfin_trip_end_timestamp = unfinished_trip_stg['trips'][i][-1]['when']
                    unfin_trip_end_time = datetime.datetime.strptime(
                        unfin_trip_end_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                    time_diff = current_trip_time - unfin_trip_end_time
                    time_diff_sec = time_diff.seconds
                    # If time difference is not minus
                    if time_diff_sec >= 0:
                        # Check if the difference is smaller than the
                        # current smallest time difference
                        if time_diff_sec < smallest_time_diff:
                            # If it is, it becomes the newest smallest time difference
                            smallest_time_diff = time_diff_sec
                            # And we need to remember which trip it was
                            closest_trip_in_time = i
                # Add the whole unfinished trip to the current trip
                trips[t][0:0] = unfinished_trips[closest_trip_in_time]
                # Remove the unfinished trip from the storage
                remove_unfinished_trip_from_storage(
                    unfinished_trips[closest_trip_in_time], f"{car_license}.json")
                # Check if the start of the trip is moving
                if trips[t][0]['what'] == 'Moving':
                    # If it is, the trip has not been completely patched yet
                    patch_complete = False
                    # Add the trip to GCP storage
                    add_unfinished_trip_to_storage(trips[t], f"{car_license}.json")
            # If the unfinished trip is not in storage
            else:
                # Remove the trip from trips
                to_rem.pop(t)
    if not to_rem:
        for rem in to_rem:
            trips.remove(rem)
    # If there is a trip that has not been completely patched yet
    if patch_complete is False:
        # Get the new unfinished trips from GCP storage
        unfinished_trips = load_unfinished_trips()
        # Call function again
        trips = patch_trips(unfinished_trips, trips, car_license)
    return trips


def remove_unfinished_trip_from_storage(trip, blob_name):
    # Get blob
    blob = storage_bucket.get_blob(blob_name)
    # Convert to string
    blob_json_string = blob.download_as_string()
    # Convert to json
    blob_json = json.loads(blob_json_string)
    # Update list
    trips = blob_json['trips']
    trips.remove(trip)
    unfinished_trips = {
        "trips": trips
    }
    # If trips is now empty
    if not trips:
        # Remove the blob from storage
        blob.delete()
    else:
        # Update blob
        new_blob = storage_bucket.blob(blob_name)
        new_blob.upload_from_string(
            data=json.dumps(unfinished_trips, indent=2),
            content_type='application/json'
        )


def add_unfinished_trip_to_storage(trip, blob_name):
    # If the blob does not exist yet
    if not storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
        unfinished_trips = {
            "trips": [trip]
        }
        blob = storage_bucket.blob(blob_name)
        blob.upload_from_string(
            data=json.dumps(unfinished_trips, indent=2),
            content_type='application/json'
        )
    # Else
    else:
        print(f"updating file {blob_name}")
        # Get blob
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
        # Update list
        trips = blob_json['trips']
        trips.append(trip)
        unfinished_trips = {
            "trips": trips
        }
        # Update blob
        new_blob = storage_bucket.blob(blob_name)
        new_blob.upload_from_string(
            data=json.dumps(unfinished_trips, indent=2),
            content_type='application/json'
        )


def load_unfinished_trips():
    car_licenses = {}
    for trip in storage_client.list_blobs(config.GCS_BUCKET):
        # Convert to string
        cartrip_license = trip.name
        cartrip_json_string = trip.download_as_string()
        # Get json
        cartrip_json = json.loads(cartrip_json_string)
        # Make json
        car_trips = cartrip_json['trips']
        cartrip = {
            cartrip_license: {
                "trips": car_trips
            }
        }
        # Add to car_licenses
        car_licenses.update(cartrip)
    return car_licenses
