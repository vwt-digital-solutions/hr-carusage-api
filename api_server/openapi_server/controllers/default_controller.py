import config
import io
import logging
import operator
import pandas as pd
from gobits import Gobits
from google.cloud import pubsub_v1
import json

from functools import reduce
from datetime import datetime, timedelta
from flask import request, jsonify, make_response, g
from google.cloud import firestore

logging.basicConfig(level=logging.INFO)


def export_trips(ended_after, ended_before):  # noqa: E501
    """Exports all trip entities to a file

    :param ended_after: Filter for trips that ended after a specific date
    :type ended_after: string
    :param ended_before: Filter for trips that ended before a specific date
    :type ended_before: string

    :rtype: blob
    """

    # Check which user is logged in
    user = g.user
    if not user:
        return make_response('The user is not authorised to make this request', 401)

    # First update the frequent offenders collection on Firestore

    db_client = firestore.Client()

    query_fs_upate = db_client.collection(config.COLLECTION_NAME)

    # Get last sunday
    today = datetime.utcnow()
    idx = (today.weekday() + 1) % 7  # MON = 1
    last_sun = today - timedelta(idx)
    last_sun = last_sun.replace(hour=23, minute=59, second=59)
    # Get monday 2 months before that
    eight_weeks_mon = last_sun - timedelta(55)
    eight_weeks_mon = eight_weeks_mon.replace(hour=00, minute=00, second=00)

    query_fs_upate = query_fs_upate.where('ended_at', '>=', eight_weeks_mon)
    query_fs_upate = query_fs_upate.where('ended_at', '<=', last_sun)
    query_fs_upate = query_fs_upate.where('outside_time_window', '==', True)

    docs_fs_update = query_fs_upate.stream()

    if docs_fs_update:
        response_fs_update = []
        every_trip_checkt = True
        for doc in docs_fs_update:
            if get_from_dict(doc, ['checking_info', 'checked']) is True or \
               get_from_dict(doc, ['checking_info', 'checked']) is False:
                offender_dict = {
                    'department_name': get_from_dict(doc, ['department', 'name']),
                    'department_id': get_from_dict(doc, ['department', 'id']),
                    'ended_at': get_from_dict(doc, ['ended_at']),
                    'function_name': get_from_dict(doc, ['driver_info', 'function_name']),
                    'initial': get_from_dict(doc, ['driver_info', 'initial']),
                    'last_name': get_from_dict(doc, ['driver_info', 'last_name']),
                    'license': get_from_dict(doc, ['license']),
                    'prefix': get_from_dict(doc, ['driver_info', 'prefix']),
                    'started_at': get_from_dict(doc, ['started_at']),
                    'trip_kind': get_from_dict(doc, ['checking_info', 'trip_kind']),
                    'trip_description': get_from_dict(doc, ['checking_info', 'description'])
                }
                response_fs_update.append(offender_dict)
            else:
                every_trip_checkt = False

        if every_trip_checkt is True:
            # Count frequent offenders and update database
            frequent_offenders = get_frequent_offenders(response_fs_update)
            update_collection_response = update_frequent_offenders_collection(frequent_offenders, eight_weeks_mon)
            if update_collection_response is False:
                return make_response("Firestore could not be updated with frequent offenders", 500)
        else:
            return make_response("Not every trip is checked yet", 409)

    # Then export all trips after and before an end date if they have been checked already

    query_export = db_client.collection(config.COLLECTION_NAME)

    query_export = query_export.where('ended_at', '>=', datetime.strptime(ended_after, "%Y-%m-%dT%H:%M:%SZ"))
    query_export = query_export.where('ended_at', '<=', datetime.strptime(ended_before, "%Y-%m-%dT%H:%M:%SZ"))
    query_export = query_export.where('outside_time_window', '==', True)

    docs_export = query_export.stream()

    if docs_export:
        response_export = []
        response_licenses = []
        every_trip_checkt = True
        for doc in docs_export:
            if get_from_dict(doc, ['checking_info', 'checked']) is True or \
               get_from_dict(doc, ['checking_info', 'checked']) is False:
                trip_dict = {
                    'afdeling_naam': get_from_dict(doc, ['department', 'name']),
                    'afdeling_id': get_from_dict(doc, ['department', 'id']),
                    'eindigde_op': get_from_dict(doc, ['ended_at']),
                    'functie_naam': get_from_dict(doc, ['driver_info', 'function_name']),
                    'voornaam': get_from_dict(doc, ['driver_info', 'initial']),
                    'achternaam': get_from_dict(doc, ['driver_info', 'last_name']),
                    'kenteken': get_from_dict(doc, ['license']),
                    'initialen': get_from_dict(doc, ['driver_info', 'prefix']),
                    'begon_op': get_from_dict(doc, ['started_at']),
                    'trip_soort': get_from_dict(doc, ['checking_info', 'trip_kind']),
                    'trip_beschrijving': get_from_dict(doc, ['checking_info', 'description'])
                }
                response_export.append(trip_dict)
                response_licenses.append(get_from_dict(doc, ['license']))
            else:
                every_trip_checkt = False

        if every_trip_checkt is True:
            # Update trips database with export information
            exported_entities = update_trips_collection(response_licenses, ended_after, ended_before)
            if not exported_entities:
                return make_response("Firestore could not be updated with frequent offenders", 500)
            # Send trips to topic
            # TODO: uncomment below
            # trips_to_topic_response = exported_trips_to_topic(exported_entities)
            # if trips_to_topic_response is False:
            #     return make_response("Exported trips could not be send to topic", 500)
            return ContentResponse().create_content_response(response_export, frequent_offenders, request.content_type)
        else:
            return make_response("Not every trip is checked yet", 409)

    return make_response(jsonify([]), 204)


def get_frequent_offenders(results):
    # Get all licenses that were outside of time window
    licenses = []
    trip_info_dicts = {}
    if results:
        for result in results:
            if result.get("license") in trip_info_dicts:
                trip_info_dicts[result.get("license")].append(result)
            else:
                trip_info_dicts[result.get("license")] = [result]
            if result.get("license") not in licenses:
                licenses.append(result.get("license"))
    else:
        return make_response("Response is missing", 500)

    frequent_offenders = {}
    # For offenders in this period
    for car_license in licenses:
        # Count how many times in the past three weeks a driver drove outside of the time window
        count = len(trip_info_dicts[car_license])
        # If this value is more than or equal to 3, the driver is a frequent offender
        if count >= 3:
            for trip in trip_info_dicts[car_license]:
                trip_info = {
                        "ended_at": trip.get("ended_at"),
                        "started_at": trip.get("started_at"),
                        "trip_kind": trip.get("trip_kind"),
                        "trip_description": trip.get("trip_description")
                    }
                # If the name is not yet in frequent offenders
                if car_license not in frequent_offenders:
                    offender_info = {
                        "department_name": trip.get("department_name"),
                        "department_id": trip.get("department_id"),
                        "function_name": trip.get("function_name"),
                        "initial": trip.get("initial"),
                        "last_name": trip.get("last_name"),
                        "license": car_license,
                        "prefix": trip.get("prefix")
                    }
                    frequent_offender = {
                        car_license: {
                            "offender_info": offender_info,
                            "trips": [trip_info]
                        }
                    }
                    frequent_offenders.update(frequent_offender)
                # If it is
                else:
                    # Update the trips of the offender
                    frequent_offenders[car_license]['trips'].append(trip_info)

    return frequent_offenders


def update_frequent_offenders_collection(frequent_offenders, ended_after):
    batch_limit = 500
    batch_has_new_entities = True
    batch_last_reference = None

    updated_freq_off = []

    # While the batch contains new entries
    while batch_has_new_entities:
        # Query
        db_client = firestore.Client()
        query = db_client.collection(config.COLLECTION_NAME_FREQ_OFF)

        if batch_last_reference:
            query = query.start_after(batch_last_reference)

        query = query.limit(batch_limit)
        docs = query.stream()

        if docs:
            batch = db_client.batch()  # Creating new batch
            docs_list = list(docs)

            if len(docs_list) < batch_limit:
                batch_has_new_entities = False

            update_batch = False
            for doc in docs_list:
                batch_last_reference = doc
                doc_dict = doc.to_dict()

                car_license_collection = doc_dict.get('offender_info').get('license')
                if car_license_collection:
                    # Check if car_license is in found frequent offenders
                    car_license_freq_off = frequent_offenders.get(car_license_collection)
                    if car_license_freq_off:
                        # If it is
                        # Get the current trips where the offender went outside of their time window
                        trips = doc_dict['trips']
                        update_trip = False
                        # Update these trips
                        for trip in car_license_freq_off['trips']:
                            # If trip is not already in the trips of the Firestore entity
                            # and the trip has an end date after a certain date
                            if trip not in trips and trip['ended_at'] > ended_after:
                                # Add trip to trips
                                trips.append(trip)
                                update_trip = True
                                update_batch = True
                        if update_trip is True:
                            # Update the entity
                            field = {
                                "trips": trips
                            }
                            batch.update(doc.reference, field)
                        updated_freq_off.append(car_license_collection)
            if update_batch is True:
                batch.commit()  # Committing changes within batch
        else:
            batch_has_new_entities = False

    # Also update the collection with frequent offenders that were not yet in the collection
    for offender in frequent_offenders:
        if offender not in updated_freq_off:
            # Upload to firestore
            try:
                doc_ref = db_client.collection(config.COLLECTION_NAME_FREQ_OFF).document()
                doc_ref.set(frequent_offenders[offender])
            except Exception as e:
                logging.exception(f"Unable to upload frequent offender {frequent_offenders[offender]['offender_info']['license']} "
                                  f"because of {e}")

    logging.info("Updated the frequent offenders collection in the GCP Firestore")

    return True


def update_trips_collection(response_licenses, ended_after, ended_before):
    batch_limit = 500
    batch_has_new_entities = True
    batch_last_reference = None

    exported_docs = []

    # While the batch contains new entries
    while batch_has_new_entities:
        # Query
        db_client = firestore.Client()
        query = db_client.collection(config.COLLECTION_NAME)

        query = query.where('ended_at', '>=', datetime.strptime(ended_after, "%Y-%m-%dT%H:%M:%SZ"))
        query = query.where('ended_at', '<=', datetime.strptime(ended_before, "%Y-%m-%dT%H:%M:%SZ"))
        query = query.where('outside_time_window', '==', True)
        query = query.order_by("ended_at", direction="ASCENDING")

        if batch_last_reference:
            query = query.start_after(batch_last_reference)

        query = query.limit(batch_limit)
        docs = query.stream()

        if docs:
            batch = db_client.batch()  # Creating new batch
            docs_list = list(docs)

            if len(docs_list) < batch_limit:
                batch_has_new_entities = False

            update_batch = False
            for doc in docs_list:
                batch_last_reference = doc
                doc_dict = doc.to_dict()

                doc_license = doc_dict.get('license')
                if doc_license:
                    # Check if the license is in export info
                    if doc_license in response_licenses:
                        # Get time in utc
                        time_now = datetime.utcnow()
                        # Check which user is logged in
                        user = g.user
                        if not user:
                            return make_response('The user is not authorised to make this request', 401)
                        # Update the entity
                        field = {
                            "exported": {
                                "exported_at": time_now,
                                "exported_by": user
                            }
                        }
                        batch.update(doc.reference, field)
                        # Add doc to list of exported entities
                        doc_dict = doc.to_dict()
                        doc_dict['started_at'] = doc_dict['started_at'].isoformat()
                        doc_dict['ended_at'] = doc_dict['ended_at'].isoformat()
                        export_field_dict = {
                            "exported": {
                                "exported_at": time_now.isoformat(),
                                "exported_by": user
                            }
                        }
                        doc_dict.update(export_field_dict)
                        for loc in doc_dict['locations']:
                            loc['when'] = loc['when'].isoformat()
                        exported_docs.append(doc_dict)
                        update_batch = True
            if update_batch is True:
                batch.commit()  # Committing changes within batch
        else:
            batch_has_new_entities = False

    logging.info("Updated the trips collection with export info in the GCP Firestore")

    return exported_docs


def exported_trips_to_topic(response):
    trip_batches = chunks(response, 50)
    for trip_batch in trip_batches:
        topic_response = to_topic(trip_batch)
        if topic_response is False:
            return False
        break
    return True


def to_topic(batch):
    try:
        # Get gobits
        gobits = Gobits()
        # Project ID where the topic is
        topic_project_id = config.PROJECT_ID_TOPIC
        # Topic name
        topic_name = config.TOPIC_NAME
        # Publish to topic
        publisher = pubsub_v1.PublisherClient()
        topic_path = f"projects/{topic_project_id}/topics/{topic_name}"
        msg = {
            "gobits": [gobits.to_json()],
            "trips": batch
        }
        logging.info(msg)
        # print(json.dumps(msg, indent=4, sort_keys=True))
        future = publisher.publish(
            topic_path, bytes(json.dumps(msg).encode('utf-8')))
        future.add_done_callback(
            lambda x: logging.debug(f"Published {len(batch)} exported trips"))
        return True
    except Exception as e:
        logging.exception('Unable to publish exported trips ' +
                          'to topic because of {}'.format(e))
    return False


def chunks(the_list, n):
    n = max(1, n)
    return (the_list[i:i+n] for i in range(0, len(the_list), n))


def get_from_dict(data_dict, map_list):
    """Returns a dictionary based on a mapping"""
    try:
        if not isinstance(data_dict, dict):
            data_dict = data_dict.to_dict()

        return reduce(operator.getitem, map_list, data_dict)
    except KeyError:
        return None


class ContentResponse(object):
    def __init__(self):
        pass

    @staticmethod
    def create_dataframe_trips(content):
        df = pd.DataFrame(content)
        for col in df.select_dtypes(include=['datetimetz']):
            df[col] = df[col].apply(lambda a: a.tz_convert('Europe/Amsterdam').tz_localize(None))

        return df

    @staticmethod
    def create_dataframe_frequent_offenders(content):
        department_names = []
        department_ids = []
        function_names = []
        initials = []
        last_names = []
        licenses = []
        prefixes = []
        for car_license in content:
            department_names.append(content[car_license]['offender_info']['department_name'])
            department_ids.append(content[car_license]['offender_info']['department_id'])
            function_names.append(content[car_license]['offender_info']['function_name'])
            initials.append(content[car_license]['offender_info']['initial'])
            last_names.append(content[car_license]['offender_info']['last_name'])
            licenses.append(content[car_license]['offender_info']['license'])
            prefixes.append(content[car_license]['offender_info']['prefix'])
        content_json = {
            "achternaam": last_names,
            "voornaam": initials,
            "initialen": prefixes,
            "kenteken": licenses,
            "afdeling_naam": department_names,
            "afdeling_id": department_ids,
            "functie_naam": function_names
        }
        df = pd.DataFrame(content_json)

        return df

    @staticmethod
    def create_dataframe_frequent_offenders_trips(content):
        last_names = []
        licenses = []
        ended_ats = []
        started_ats = []
        trip_kinds = []
        trip_descriptions = []
        for car_license in content:
            for trip in content[car_license]['trips']:
                last_names.append(content[car_license]['offender_info']['last_name'])
                licenses.append(content[car_license]['offender_info']['license'])
                ended_ats.append(trip['ended_at'])
                started_ats.append(trip['started_at'])
                trip_kinds.append(trip['trip_kind'])
                trip_descriptions.append(trip['trip_description'])
        content_json = {
            "achternaam": last_names,
            "kenteken": licenses,
            "begon_op": started_ats,
            "eindigde_op": ended_ats,
            "trip_soort": trip_kinds,
            "trip_beschrijving": trip_descriptions
        }

        df = pd.DataFrame(content_json)
        for col in df.select_dtypes(include=['datetimetz']):
            df[col] = df[col].apply(lambda a: a.tz_convert('Europe/Amsterdam').tz_localize(None))

        return df

    def response_xlsx(self, response_sheet1, response_sheet2):
        """Returns the data as a XLSX file"""

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        try:
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')

            df1 = self.create_dataframe_trips(response_sheet1)
            df2 = self.create_dataframe_frequent_offenders(response_sheet2)
            df3 = self.create_dataframe_frequent_offenders_trips(response_sheet2)
            df1.to_excel(writer, sheet_name=config.COLLECTION_NAME, index=False)
            df2.to_excel(writer, sheet_name="veelplegers", index=False)
            df3.to_excel(writer, sheet_name="veelplegers_trips", index=False)

            writer.save()

            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f"attachment; filename={config.COLLECTION_NAME}_{timestamp}.xlsx"
            return response
        except Exception as e:
            logging.info(f"Generating XLSX file failed: {str(e)}")
            return make_response('Something went wrong during the generation of a XLSX file', 400)

    def create_content_response(self, response_sheet1, response_sheet2, content_type):
        """Creates a response based on the request's content-type"""
        # Give response
        if content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':  # XLSX
            return self.response_xlsx(response_sheet1, response_sheet2)

        return make_response('Something went wrong during the generation of a XLSX file', 400)  # JSON
