import json
from google.cloud import storage
import config
import datetime
from firestore import upload_to_firestore
import sys
import logging
import os

logging.basicConfig(level=logging.INFO)

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCP_BUCKET_CAR_LOCATIONS)


def make_trips(file_name_locations):
    # Make trips from yesterday's locations
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    year = yesterday.year
    month = '{:02d}'.format(yesterday.month)
    day = '{:02d}'.format(yesterday.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    cars_with_trips = []
    # Check if the file with yesterday's locations exists
    blob_name = f"{bucket_folder}/{file_name_locations}"
    if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
        # If it does, get it
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
        # Loop over every car license in the file
        for hashed_car_license in blob_json:
            # Get the locations of the car_license
            locations = blob_json[hashed_car_license]['locations']
            # Make new trips list
            trips = []
            # Make a new trip
            trip = []
            # Loop over every location of the current car
            for i in range(len(locations)):
                # Location_checked variable is initialized
                # It checks if a location has already been added to the trip
                location_checked = False
                location = locations[i]
                # Set location's when to a datetime instead of a timestamp
                when = location['when']
                when_datetime = datetime.datetime.strptime(when, "%Y-%m-%dT%H:%M:%S")
                location['when'] = when_datetime
                # Check if the car is stationary and after that moving
                # That's the beginning of the trip
                if i + 1 < len(locations)-1 and location_checked is False:
                    if (location['what'] == "Stationary" and locations[i+1]['what'] == "Moving") \
                       or (location['what'] == "ExternalPowerChange" and locations[i+1]['what'] == "Moving"):
                        # A new trip has started
                        trip = [location]
                        location_checked = True
                # Check if the car is stationary and was moving before
                # That's the end of the trip
                if i - 1 >= 0 and location_checked is False:
                    if (location['what'] == 'Stationary' and locations[i-1]['what'] == 'Moving') \
                       or (location['what'] == 'ExternalPowerChange' and locations[i-1]['what'] == 'Moving'):
                        # Add it to the trip
                        if location not in trip:
                            trip.append(location)
                        # Add trip to trips
                        trips.append(trip)
                        # And make trip empty again
                        trip = []
                        location_checked = True
                # Check if the car is moving
                if location['what'] == 'Moving' and location_checked is False:
                    # Add it to the trip
                    if location not in trip:
                        trip.append(location)
                    # If it is the last location in the list
                    # The trip will finish somewhere the next day
                    # Just add it
                    if i == len(locations)-1:
                        # Add trip to trips
                        trips.append(trip)
                        # Make trip empty again
                        trip = []
                    location_checked = True
                # Check if the car is stationary
                elif (location['what'] == "Stationary" and location_checked is False) or \
                     (location['what'] == "ExternalPowerChange" and location_checked is False):
                    # Add it to the trip
                    if location not in trip:
                        trip.append(location)
                    # If it is the last location in the list
                    # This could be the start of a new trip
                    # Just add it
                    if i == len(locations)-1:
                        # Add trip to trips
                        trips.append(trip)
                        # Make trip empty again
                        trip = []
                    location_checked = True
            car_with_trips = {
                "license": blob_json[hashed_car_license]['license'],
                "license_hash": hashed_car_license,
                "trips": trips
            }
            cars_with_trips.append(car_with_trips)
        return cars_with_trips


def patch_trip(trip, car_license_hash, file_name_locations):
    # Get the locations from the day before yesterday
    day_before_yesterday = datetime.datetime.today() - datetime.timedelta(days=2)
    year = day_before_yesterday.year
    month = '{:02d}'.format(day_before_yesterday.month)
    day = '{:02d}'.format(day_before_yesterday.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    # Check if the file with the day before yesterday's locations exists
    blob_name = f"{bucket_folder}/{file_name_locations}"
    if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
        # If it does, get it
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
        # Get locations of car
        locations = blob_json[car_license_hash].get('locations', []) if car_license_hash in blob_json else []
        # Keep adding locations in front of this trip until a Stationary location is found
        add_to_trip = []
        i = len(locations)-1
        while i >= 0:
            location = locations[i]
            # Set location's when to a datetime instead of a timestamp
            when = location['when']
            when_datetime = datetime.datetime.strptime(when, "%Y-%m-%dT%H:%M:%S")
            location['when'] = when_datetime
            # While location is not stationary and is not an external power change
            while location['what'] != "Stationary" and location['what'] != "ExternalPowerChange":
                # Keep adding the location to a trip
                add_to_trip.append(location)
                if i - 1 < 0:
                    # If i - 1 is smaller than zero
                    # No stationary location is found in yesterday's locations
                    # Return empty
                    return []
                i = i - 1
            # Stationary location is found
            add_to_trip.append(location)
            i = -1
        # Now the locations to be added need to be reversed in order to add them to the trip
        add_to_trip = add_to_trip[::-1]
        # Now add the locations in front of the trip
        trip[0:0] = add_to_trip
        return trip
    # If there are no locations the day before yesterday, remove this trip
    return []


def patch_trips(car_trips, file_name_locations):
    # For every car in car_trips
    for car in car_trips:
        car_license_hash = car['license_hash']
        # Check if first trip of the car starts with moving
        # Because if it does, the trip has started yesterday and was only finished today
        trip = car['trips'][0]
        if trip[0]['what'] == "Moving":
            trip = patch_trip(trip, car_license_hash, file_name_locations)
            # If trip is now empty
            if not trip:
                # There were no locations yesterday, trip can be removed
                car['trips'].pop(0)
            else:
                # Else set the unfinished car trip to the finished car trip
                car['trips'][0] = trip
        # Check if the last trip of the car ends with moving
        # Because if it does, the trip starts today but ends tomorrow and can be removed
        # First check if trip is not empty since other check
        if car['trips']:
            trip = car['trips'][-1]
            if trip[-1]['what'] == "Moving":
                car['trips'] = car['trips'][:-1]
        # Remove double stationary
        # First check if trip is not empty since other checks
        if car['trips']:
            to_rem_trip = []
            new_trips = []
            # For every trip of the car
            for t in range(len(car['trips'])):
                to_rem_loc = []
                new_trip = []
                # For every location in a trip
                for loc in range(len(car['trips'][t])):
                    # Check if a previous location get is possible
                    if loc - 1 >= 0:
                        # If the current location is stationary and the last location
                        # is stationary as well
                        if (car['trips'][t][loc-1]['what'] == 'Stationary' and
                           car['trips'][t][loc]['what'] == 'Stationary') or \
                           (car['trips'][t][loc-1]['what'] == 'Stationary' and
                           car['trips'][t][loc]['what'] == 'ExternalPowerChange') or \
                           (car['trips'][t][loc-1]['what'] == 'ExternalPowerChange' and
                           car['trips'][t][loc]['what'] == 'Stationary') or \
                           (car['trips'][t][loc-1]['what'] == 'ExternalPowerChange' and
                           car['trips'][t][loc]['what'] == 'ExternalPowerChange'):
                            # Remove the last stationary location
                            to_rem_loc.append(loc-1)
                # Only add locations that did not have to be removed
                for loc in range(len(car['trips'][t])):
                    if loc not in to_rem_loc:
                        new_trip.append(car['trips'][t][loc])
                car['trips'][t] = new_trip
                # Now check if the current trip only has one value
                if len(car['trips'][t]) == 1:
                    # If it does, remove the trip
                    to_rem_trip.append(t)
            for t in range(len(car['trips'])):
                if t not in to_rem_trip:
                    new_trips.append(car['trips'][t])
            car['trips'] = new_trips
    # Remove car licenses that have empty trips
    new_car_trips = []
    to_rem_car_trip = []
    for c in range(len(car_trips)):
        if not car_trips[c]["trips"]:
            to_rem_car_trip.append(c)
    for c in range(len(car_trips)):
        if c not in to_rem_car_trip:
            new_car_trips.append(car_trips[c])
    car_trips = new_car_trips
    return car_trips


def entrypoint(request):
    # Get file name of file with locations
    file_name_locations = str(os.environ.get("FILE_NAME"))
    if not file_name_locations:
        logging.error("Required argument FILE_NAME missing")
    if not file_name_locations.endswith(".json"):
        logging.error("Argument FILE_NAME should have json extension")
    # Make trips
    car_trips = make_trips(file_name_locations)
    # Patch trips
    car_trips = patch_trips(car_trips, file_name_locations)
    # Upload to firestore
    upload_to_firestore_success = upload_to_firestore(car_trips)
    if not upload_to_firestore_success:
        sys.exit(1)
    logging.info("Finished uploading trips to firestore")
    # Safe car trips locally
    # for car_trip in car_trips:
    #     for trip in car_trip['trips']:
    #         for loc in trip:
    #             loc['when'] = loc['when'].isoformat()
    # with open('trips.json', 'w', encoding='utf-8') as f:
    #     json.dump(car_trips, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    entrypoint(None)
