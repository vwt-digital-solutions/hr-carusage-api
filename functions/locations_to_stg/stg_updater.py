from google.cloud import storage
import json
from datetime import datetime
from hashlib import sha256


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
                    "locations": [car_loc]
                }
            }
            car_licenses.update(car)


def locations_to_stg(car_licenses, storage_client, storage_bucket):
    # Get today's date for the storage bucket
    today = datetime.today()
    year = today.year
    month = '{:02d}'.format(today.month)
    day = '{:02d}'.format(today.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    for car_license in car_licenses:
        # hash the license
        car_li = car_license
        license_hash = sha256(car_li.encode('utf-8')).hexdigest()
        blob_name = f"{bucket_folder}/{license_hash}.json"
        # Check if license is not already in storage
        if not storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
            # If it isn't, upload locations to storage
            blob = storage_bucket.blob(blob_name)
            # Make json
            car = {
                "license": car_license,
                "locations": car_licenses[car_license]['locations']
            }
            blob.upload_from_string(
                data=json.dumps(car, indent=2),
                content_type='application/json'
            )
        # Else
        else:
            # If it is, update the blob
            # print(f"updating file {blob_name}")
            # Get blob
            blob = storage_bucket.get_blob(blob_name)
            # Convert to string
            blob_json_string = blob.download_as_string()
            # Convert to json
            blob_json = json.loads(blob_json_string)
            # Get locations of blob
            locations = blob_json['locations']
            # For every location
            for loc in car_licenses[car_license]['locations']:
                # If the location is not yet in locations
                if loc not in locations:
                    locations.append(loc)
            # Make new json
            car = {
                "license": car_license,
                "locations": locations
            }
            # Update blob
            new_blob = storage_bucket.blob(blob_name)
            new_blob.upload_from_string(
                data=json.dumps(car, indent=2),
                content_type='application/json'
            )
