import json
from google.cloud import storage
import functionsconfig as config
import datetime
from hashlib import sha256

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(config.GCS_BUCKET_CAR_LOCATIONS)


def make_trips():
    # Make trips from today's locations
    today = datetime.datetime.today()
    year = today.year
    month = '{:02d}'.format(today.month)
    day = '{:02d}'.format(today.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    cars_with_trips = []
    # Loop over every blob in storage, every blob is a single car with locations
    for car in storage_client.list_blobs(
               config.GCS_BUCKET_CAR_LOCATIONS, prefix=bucket_folder):
        # Get its JSON
        car_json_string = car.download_as_string()
        car_json = json.loads(car_json_string)
        car_license = car_json['license']
        # Make new trips list
        trips = []
        # Make a new trip
        trip = []
        # Loop over every location of the current car
        for i in range(len(car_json['locations'])):
            # location_checked variable is initialized
            # it checks if a location has already been added to the trip
            location_checked = False
            location = car_json['locations'][i]
            # Check if the car is stationary and after that moving
            # That's the beginning of the trip
            if i + 1 < len(car_json['locations'])-1 and \
               location_checked is False:
                if location['what'] == "Stationary" and \
                   car_json['locations'][i+1]['what'] == "Moving":
                    # A new trip has started
                    trip = [location]
                    location_checked = True
            # Check if the car is stationary and was moving before
            # That's the end of the trip
            if i - 1 >= 0 and location_checked is False:
                if location['what'] == 'Stationary' and \
                   car_json['locations'][i-1]['what'] == 'Moving':
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
                # print(f"Car {car_license} is moving!")
                # Add it to the trip
                if location not in trip:
                    trip.append(location)
                # If it is the last location in the list
                # The trip will finish somewhere the next day
                # Just add it
                if i == len(car_json['locations'])-1:
                    # print(f"Car {car_license}'s last location is Moving")
                    # Add trip to trips
                    trips.append(trip)
                    # Make trip empty again
                    trip = []
                location_checked = True
            # Check if the car is stationary
            elif location['what'] == "Stationary" and location_checked is False:
                # print(f"Car {car_license} is stationary!")
                # Add it to the trip
                if location not in trip:
                    trip.append(location)
                # If it is the last location in the list
                # This could be the start of a new trip
                # Just add it
                if i == len(car_json['locations'])-1:
                    # print(f"Car {car_license}'s last location is Stationary")
                    # Add trip to trips
                    trips.append(trip)
                    # Make trip empty again
                    trip = []
                location_checked = True
        car_with_trips = {
            "license": car_license,
            "trips": trips
        }
        cars_with_trips.append(car_with_trips)
    return cars_with_trips


def patch_trip(trip, car_license):
    # Get yesterday's locations
    yesterday = datetime.datetime.today() - datetime.timedelta(1)
    year = yesterday.year
    month = '{:02d}'.format(yesterday.month)
    day = '{:02d}'.format(yesterday.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    car_li = car_license
    car_license_hash = sha256(car_li.encode('utf-8')).hexdigest()
    blob_name = f"{bucket_folder}/{car_license_hash}.json"
    # Check if there are locations yesterday for this license
    if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
        # Get blob
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
        # Get locations of blob
        locations = blob_json['locations']
        # Keep adding locations in front of this trip until a Stationary location is found
        add_to_trip = []
        i = len(locations)-1
        while i >= 0:
            while locations[i]['what'] != "Stationary":
                add_to_trip.append(locations[i])
                if i - 1 < 0:
                    # If i - 1 is smaller than zero
                    # No stationary location is found in yesterday's locations
                    # Return empty
                    return []
                i = i - 1
            # Stationary location is found
            add_to_trip.append(locations[i])
            i = -1
        # Now the locations to be added need to be reversed in order to add them to the trip
        add_to_trip = add_to_trip[::-1]
        # Now add the locations in front of the trip
        trip[0:0] = add_to_trip
        return trip
    # If there are no locations yesterday, remove this trip
    return []


def remove_multipe_stationary_locations(trip):
    trip_without_mul_stat_locs = []
    for t in range(len(trip)):
        if t + 1 < len(trip) - 1:
            if not trip[t]['what'] == 'Stationary' and \
                   trip[t+1]['what'] == 'Stationary':
                trip_without_mul_stat_locs.append(trip[t])
        else:
            trip_without_mul_stat_locs.append(trip[t])
    return trip_without_mul_stat_locs


def patch_trips(car_trips):
    # For every car in car_trips
    for car in car_trips:
        car_license = car['license']
        # Check if first trip of the car starts with moving
        # Because if it does, the trip has started yesterday and was only finished today
        trip = car['trips'][0]
        if trip[0]['what'] == "Moving":
            trip = patch_trip(trip, car_license)
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
            for t in range(len(car['trips'])):
                to_rem_loc = []
                new_trip = []
                for loc in range(len(car['trips'][t])):
                    if loc - 1 >= 0:
                        if car['trips'][t][loc-1]['what'] == 'Stationary' and \
                           car['trips'][t][loc]['what'] == 'Stationary':
                            to_rem_loc.append(loc-1)
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


if __name__ == '__main__':
    # Make trips
    car_trips = make_trips()
    # Patch trips
    car_trips = patch_trips(car_trips)
    # Print car trips
    with open('trips.json', 'w', encoding='utf-8') as f:
        json.dump(car_trips, f, ensure_ascii=False, indent=2)
    # Upload to firestore
