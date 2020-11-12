import config
import io
import logging
import operator
import pandas as pd

from functools import reduce
from datetime import datetime
from flask import request, jsonify, make_response
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

    db_client = firestore.Client()

    query = db_client.collection(config.COLLECTION_NAME)
    query = query.where('ended_at', '>=', datetime.strptime(ended_after, "%Y-%m-%dT%H:%M:%SZ"))
    query = query.where('ended_at', '<=', datetime.strptime(ended_before, "%Y-%m-%dT%H:%M:%SZ"))
    query = query.where('outside_time_window', '==', True)

    docs = query.stream()

    if docs:
        response = [{
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
        } for doc in docs]

        update_database_freq_offenders(response)  # Count frequent offenders and update database

        return ContentResponse().create_content_response(response, request.content_type)

    return make_response(jsonify([]), 204)


def update_database_freq_offenders(results):
    # Get all licenses that were outside of time window
    licenses = []
    if results:
        for trip in results:
            trip_license = trip.get('license')
            if trip_license:
                licenses.append(trip_license)
            else:
                return make_response("Result does not have a 'license' key", 500)
    else:
        return make_response("Response does not have a 'results' key", 500)

    # TODO: update frequent offenders


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
    def create_dataframe(content):
        df = pd.DataFrame(content)
        for col in df.select_dtypes(include=['datetimetz']):
            df[col] = df[col].apply(lambda a: a.tz_convert('Europe/Amsterdam').tz_localize(None))

        return df

    def response_csv(self, response):
        """Returns the data as a CSV file"""

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        try:
            output = io.StringIO()

            df = self.create_dataframe(response)
            csv_response = df.to_csv(sep=";", index=False, decimal=",")

            output.write(csv_response)

            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'text/csv'
            response.headers['Content-Disposition'] = f"attachment; filename={config.COLLECTION_NAME}_{timestamp}.csv"
            return response
        except Exception as e:
            logging.info(f"Generating CSV file failed: {str(e)}")
            return make_response('Something went wrong during the generation of a CSV file', 400)

    def response_xlsx(self, response):
        """Returns the data as a XLSX file"""

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        try:
            output = io.BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')

            df = self.create_dataframe(response)
            df.to_excel(writer, sheet_name=config.COLLECTION_NAME, index=False)

            writer.save()

            response = make_response(output.getvalue())
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f"attachment; filename={config.COLLECTION_NAME}_{timestamp}.xlsx"
            return response
        except Exception as e:
            logging.info(f"Generating XLSX file failed: {str(e)}")
            return make_response('Something went wrong during the generation of a XLSX file', 400)

    def create_content_response(self, response, content_type):
        """Creates a response based on the request's content-type"""
        # Give response
        if content_type == 'text/csv':  # CSV
            return self.response_csv(response)
        elif content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':  # XLSX
            return self.response_xlsx(response)

        return response  # JSON
