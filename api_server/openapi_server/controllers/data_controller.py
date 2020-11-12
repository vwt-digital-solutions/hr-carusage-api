import logging
from flask import make_response

logging.basicConfig(level=logging.INFO)


def update_database_freq_offenders(response):
    # Get all licenses that were outside of time window
    licenses = []
    results = response.get('results')
    if results:
        for trip in results:
            trip_license = trip.get('license')
            if trip_license:
                licenses.append(trip_license)
            else:
                return make_response("Result does not have a 'license' key", 500)
    else:
        return make_response("Response does not have a 'results' key", 500)
    # Update firestore database collection with frequent offenders
    logging.info("TODO, update frequent offenders")
