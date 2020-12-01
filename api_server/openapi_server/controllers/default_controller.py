import config
import io
import logging
import operator
import pandas as pd

from functools import reduce
from datetime import datetime
from flask import request, jsonify, make_response, g
from google.cloud import firestore

from openapi_server.controllers.export_controller import ExportProcessor

logging.basicConfig(level=logging.INFO)


def export_trips(ended_after, ended_before):  # noqa: E501
    """Exports all trip entities to a file

    :param ended_after: Filter for trips that ended after a specific date
    :type ended_after: string
    :param ended_before: Filter for trips that ended before a specific date
    :type ended_before: string

    :rtype: blob
    """

    if 'user' not in g:  # Check if user is logged in
        return make_response(
            {"detail": "De gebruiker is niet bevoegd om dit verzoek te doen", "status": 401, "title": "Unauthorized",
             "type": "about:blank"}, 401)

    export_processor = ExportProcessor(ended_after, ended_before)
    trips_to_export, all_trips_marked = export_processor.get_trips_to_export()

    if not all_trips_marked:
        return make_response(
            {"detail": "Nog niet elke rit is gemarkeerd", "status": 405, "title": "Method Not Allowed",
             "type": "about:blank"}, 405), None

    if len(trips_to_export) > 0:
        # Retrieve active and existing frequent offenders
        fo_active, fo_existing, fo_to_update = export_processor.process_frequent_offenders(trips_to_export)

        trips_to_export = export_processor.add_export_info(trips_to_export)

        # Send trips to topic
        trips_to_topic_response = export_processor.exported_trips_to_topic(trips_to_export)
        if trips_to_topic_response is False:
            return make_response(
                {"detail": "Er is iets fout gegaan tijdens het archiveren van de ritten", "status": 400,
                 "title": "Bad Request", "type": "about:blank"}, 400)

        # Update all entities with transaction
        try:
            export_processor.update_entities(fo_existing, fo_to_update, trips_to_export)
        except Exception as e:
            logging.error(e)
            return make_response(
                {"detail": "Er is iets fout gegaan tijdens het opslaan van de ritten", "status": 409,
                 "title": "Conflict", "type": "about:blank"}, 409)

        return ContentResponse().create_content_response_freq_offenders(
            trips_to_export, fo_active, request.content_type)

    return make_response(jsonify([]), 204)


def check_open_trips(ended_after, ended_before):
    """Result is a list of open trips

    :param ended_after: Filter for trips that ended after a specific date
    :type ended_after: string
    :param ended_before: Filter for trips that ended before a specific date
    :type ended_before: string

    :rtype: blob
    """

    db_client = firestore.Client()

    # Get the open trips
    open_trips_response = get_open_trips(db_client, ended_after, ended_before)
    return open_trips_response


def get_open_trips(db_client, ended_after, ended_before):
    query_trips = db_client.collection(config.COLLECTION_NAME)

    query_trips = query_trips.where('ended_at', '>=', datetime.strptime(ended_after, "%Y-%m-%dT%H:%M:%SZ"))
    query_trips = query_trips.where('ended_at', '<=', datetime.strptime(ended_before, "%Y-%m-%dT%H:%M:%SZ"))
    query_trips = query_trips.where('outside_time_window', '==', True)

    docs_trips = query_trips.stream()

    if docs_trips:
        response_open_trips = []
        for doc in docs_trips:
            if not get_from_dict(doc, ['exported', 'exported_at']):
                trip_kind = get_from_dict(doc, ['checking_info', 'trip_kind'])

                trip_dict = {
                    'kenteken': get_from_dict(doc, ['license']),
                    'begon_op': get_from_dict(doc, ['started_at']),
                    'eindigde_op': get_from_dict(doc, ['ended_at']),
                    'voornaam': get_from_dict(doc, ['driver_info', 'driver_first_name']),
                    'achternaam': get_from_dict(doc, ['driver_info', 'driver_last_name']),
                    'afdeling_naam': get_from_dict(doc, ['department', 'department_name']),
                    'afdeling_nummer': get_from_dict(doc, ['department', 'department_id']),
                    'rit_soort': 'werk' if trip_kind == 'work' else ('privé' if trip_kind == 'personal' else None),
                    'rit_beschrijving': get_from_dict(doc, ['checking_info', 'description'])
                }
                response_open_trips.append(trip_dict)

        # If there are open trips
        if response_open_trips:
            return ContentResponse().create_content_response(response_open_trips, request.content_type)

    return make_response(
        {"detail": "Er zijn geen open ritten", "status": 204, "title": "No Content",
         "type": "about:blank"}, 204)


def chunks(the_list, n):
    n = max(1, n)
    return (the_list[i:i + n] for i in range(0, len(the_list), n))


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
        trips = []
        for trip in content:
            trip_kind = get_from_dict(trip, ['checking_info', 'trip_kind'])

            trips.append({
                'kenteken': get_from_dict(trip, ['license']),
                'begon_op': datetime.strptime(get_from_dict(trip, ['started_at']), "%Y-%m-%dT%H:%M:%SZ"),
                'eindigde_op': datetime.strptime(get_from_dict(trip, ['ended_at']), "%Y-%m-%dT%H:%M:%SZ"),
                'voornaam': get_from_dict(trip, ['driver_info', 'driver_first_name']),
                'achternaam': get_from_dict(trip, ['driver_info', 'driver_last_name']),
                "personeelsnummer": get_from_dict(trip, ['driver_info', 'driver_employee_number']),
                'afdeling_naam': get_from_dict(trip, ['department', 'department_name']),
                'afdeling_nummer': get_from_dict(trip, ['department', 'department_id']),
                'rit_soort': 'werk' if trip_kind == 'work' else ('privé' if trip_kind == 'personal' else None),
                'rit_beschrijving': get_from_dict(trip, ['checking_info', 'description'])
            })

        df = pd.DataFrame(trips)
        for col in df.select_dtypes(include=['datetimetz']):
            df[col] = df[col].apply(lambda a: a.tz_convert('Europe/Amsterdam').tz_localize(None))

        return df

    @staticmethod
    def create_dataframe_frequent_offenders(content):
        fo_list = []
        for fo_id in content:
            fo_list.append({
                "personeelsnummer": get_from_dict(content[fo_id], ['driver_info', 'driver_employee_number']),
                "achternaam": get_from_dict(content[fo_id], ['driver_info', 'driver_last_name']),
                "voornaam": get_from_dict(content[fo_id], ['driver_info', 'driver_first_name']),
                "afdeling_naam": get_from_dict(content[fo_id], ['department', 'department_name']),
                "afdeling_nummer": get_from_dict(content[fo_id], ['department', 'department_id']),
                "aantal_overtredingen": len(get_from_dict(content[fo_id], ['trips']))
            })

        df = pd.DataFrame(fo_list)

        return df

    @staticmethod
    def create_dataframe_frequent_offenders_trips(content):
        fo_trips = []
        for fo_id in content:
            driver_employee_number = get_from_dict(content[fo_id], ['driver_info', 'driver_employee_number'])
            driver_last_name = get_from_dict(content[fo_id], ['driver_info', 'driver_last_name'])
            driver_first_name = get_from_dict(content[fo_id], ['driver_info', 'driver_first_name'])
            car_license = get_from_dict(content[fo_id], ['license'])

            for trip in content[fo_id]['trips']:
                trip_kind = get_from_dict(trip, ['trip_kind'])
                fo_trips.append({
                    "personeelsnummer": driver_employee_number,
                    "achternaam": driver_last_name,
                    "voornaam": driver_first_name,
                    "kenteken": car_license,
                    "begon_op": datetime.strptime(get_from_dict(trip, ['started_at']), "%Y-%m-%dT%H:%M:%SZ"),
                    "eindigde_op": datetime.strptime(get_from_dict(trip, ['ended_at']), "%Y-%m-%dT%H:%M:%SZ"),
                    "soort": 'werk' if trip_kind == 'work' else ('privé' if trip_kind == 'personal' else None),
                    "beschrijving": get_from_dict(trip, ['trip_description'])
                })

        df = pd.DataFrame(fo_trips)
        for col in df.select_dtypes(include=['datetimetz']):
            df[col] = df[col].apply(lambda a: a.tz_convert('Europe/Amsterdam').tz_localize(None))

        return df

    def response_xlsx_freq_offenders(self, response_sheet1, response_sheet2):
        """Returns the data as a XLSX file"""

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        try:
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')

            df1 = self.create_dataframe_trips(response_sheet1)
            df1.to_excel(writer, sheet_name='Ritten', index=False)

            if len(response_sheet2) > 0:
                df2 = self.create_dataframe_frequent_offenders(response_sheet2)
                df3 = self.create_dataframe_frequent_offenders_trips(response_sheet2)

                df2.to_excel(writer, sheet_name="Veelplegers", index=False)
                df3.to_excel(writer, sheet_name="Veelplegers ritten", index=False)

            writer.save()

            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f"attachment; filename={config.COLLECTION_NAME}_{timestamp}.xlsx"
            return response
        except Exception as e:
            logging.info(f"Generating XLSX file failed: {str(e)}")
            return make_response(
                {"detail": "Er is iets misgegaan tijdens het genereren van het Excel bestand", "status": 400,
                 "title": "Bad Request", "type": "about:blank"}, 400)

    def response_xlsx(self, response_sheet1):
        """Returns the data as a XLSX file"""

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        try:
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')

            df1 = self.create_dataframe_trips(response_sheet1)
            df1.to_excel(writer, sheet_name=config.COLLECTION_NAME, index=False)

            writer.save()

            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f"attachment; filename={config.COLLECTION_NAME}_open_{timestamp}.xlsx"
            return response
        except Exception as e:
            logging.info(f"Generating XLSX file failed: {str(e)}")
            return make_response(
                {"detail": "Er is iets misgegaan tijdens het genereren van het Excel bestand", "status": 400,
                 "title": "Bad Request", "type": "about:blank"}, 400)

    def create_content_response_freq_offenders(self, response_sheet1, response_sheet2, content_type):
        """Creates a response based on the request's content-type"""
        # Give response
        if content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':  # XLSX
            return self.response_xlsx_freq_offenders(response_sheet1, response_sheet2)

        return make_response(
            {"detail": f"Het content-type '{content_type}' wordt niet ondersteund", "status": 400, "title": "Bad Request",
             "type": "about:blank"}, 400)

    def create_content_response(self, response_sheet1, content_type):
        """Creates a response based on the request's content-type"""
        # Give response
        if content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':  # XLSX
            return self.response_xlsx(response_sheet1)

        return make_response(
            {"detail": f"Het content-type '{content_type}' wordt niet ondersteund", "status": 400,
             "title": "Bad Request", "type": "about:blank"}, 400)
