from google.cloud import storage
import json
from datetime import datetime, timezone


def process_carsloc_msg(carsloc_msg, car_licenses, analyze_date):
    # List of locations gotten from message
    carsmsglocations_list = carsloc_msg['carlocations']
    # For every location in the message
    for loc in carsmsglocations_list:
        # Skip location if it's not today's date
        when = datetime.strptime(loc['when'], "%Y-%m-%dT%H:%M:%SZ")
        # Convert 'when' to UTC time
        when_timestamp = when.timestamp()
        when_utc = datetime.fromtimestamp(when_timestamp, tz=timezone.utc)
        when_iso = when_utc.isoformat()
        # Remove the +... behind the timestamp
        # and set the when of the location to UTC time
        loc['when'] = when_iso.split('+')[0]
        when = datetime.strptime(loc['when'], "%Y-%m-%dT%H:%M:%S")
        # Check if it's today's date
        if when.date() != analyze_date:
            print(f"Skipping message for {when.date()} while processing {analyze_date}")
            continue
        # Skip location if it does not have a hashed license
        license_hash = loc.get('license_hash', None)
        if not license_hash:
            print(f"Skipping message for {when.date()} while processing {analyze_date} \
                    because it does not have a hashed license")
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
                    "license_hash": license_hash
                }
            }
            car_licenses.update(car)


def locations_to_stg(analyze_date, car_licenses, storage_client, storage_bucket):
    # Get date for the storage bucket
    year = analyze_date.year
    month = '{:02d}'.format(analyze_date.month)
    day = '{:02d}'.format(analyze_date.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    for car_license in car_licenses:
        # hashed license is the name of the blob
        license_hash = car_licenses[car_license]['license_hash']
        blob_name = f"{bucket_folder}/{license_hash}.json"
        # Check if license is not already in storage
        if not storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
            # If it isn't, upload locations to storage
            blob = storage_bucket.blob(blob_name)
            # Make json
            car = {
                "license": car_license,
                "license_hash": license_hash,
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
                "license_hash": license_hash,
                "locations": locations
            }
            # Update blob
            new_blob = storage_bucket.blob(blob_name)
            new_blob.upload_from_string(
                data=json.dumps(car, indent=2),
                content_type='application/json'
            )
