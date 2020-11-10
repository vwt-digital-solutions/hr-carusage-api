from google.cloud import storage
import json
from datetime import datetime, timezone
import logging

logging.basicConfig(level=logging.INFO)


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
            logging.debug(f"Skipping message for {when.date()} while processing {analyze_date}")
            continue
        # Skip location if it does not have a hashed license
        license_hash = loc.get('license_hash', None)
        if not license_hash:
            logging.debug(f"Skipping message for {when.date()} while processing {analyze_date} \
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


def locations_to_stg(analyze_date, car_licenses, storage_client, storage_bucket, file_name_locations):
    # Get date for the storage bucket
    year = analyze_date.year
    month = '{:02d}'.format(analyze_date.month)
    day = '{:02d}'.format(analyze_date.day)
    bucket_folder = '{}/{}/{}'.format(year, month, day)
    blob_name = f"{bucket_folder}/{file_name_locations}"
    # Check if locations file is already in storage
    if storage.Blob(bucket=storage_bucket, name=blob_name).exists(storage_client):
        # If it is, get it
        blob = storage_bucket.get_blob(blob_name)
        # Convert to string
        blob_json_string = blob.download_as_string()
        # Convert to json
        blob_json = json.loads(blob_json_string)
    else:
        # If it is not, make a new one
        blob_json = {}
    # For every license
    for car_license in car_licenses:
        # hashed license
        license_hash = car_licenses[car_license]['license_hash']
        # Check if hashed license is already in blob_json
        license_in_blob = blob_json.get(license_hash)
        if license_in_blob:
            # If it is, update the license info
            # Get locations of license
            locations = blob_json[license_hash]['locations']
            # Make new locations
            new_locations = []
            # For every location already in blob
            for blob_loc in locations:
                # Check if location has today's date
                when_blob_loc = datetime.strptime(blob_loc['when'], "%Y-%m-%dT%H:%M:%S")
                if when_blob_loc.date() == analyze_date:
                    # If it does, add it to new locations
                    new_locations.append(blob_loc)
            # For every location
            for loc in car_licenses[car_license]['locations']:
                # If the location is not yet in new_locations
                if loc not in new_locations:
                    # And the location has today as date
                    when_loc = datetime.strptime(loc['when'], "%Y-%m-%dT%H:%M:%S")
                    if when_loc.date() == analyze_date:
                        # Add location
                        new_locations.append(loc)
            # If new locations is not empty
            if new_locations:
                # Set license locations to new locations
                blob_json[license_hash]['locations'] = new_locations
            # If it is
            else:
                # License should be removed
                del blob_json[license_hash]
        else:
            # If hashed license is not yet in blob_json
            # Update blob_json
            car = {
                license_hash: {
                    "license": car_license,
                    "locations": car_licenses[car_license]['locations']
                }
            }
            blob_json.update(car)
    # If car licenses is not empty
    if car_licenses:
        # Update the blob_json to storage
        new_blob = storage_bucket.blob(blob_name)
        new_blob.upload_from_string(
            data=json.dumps(blob_json, indent=2),
            content_type='application/json'
        )
        logging.info("Locations have been added to storage file")
