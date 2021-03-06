# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from openapi_server.models.base_model_ import Model
from openapi_server.models.trip_flatten import TripFlatten  # noqa: E501
from openapi_server import util


class TripsResponseFile(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, results=None):  # noqa: E501
        """TripsResponseFile - a model defined in OpenAPI

        :param results: The results of this TripsResponseFile.  # noqa: E501
        :type results: List[TripFlatten]
        """
        self.openapi_types = {
            'results': List[TripFlatten]
        }

        self.attribute_map = {
            'results': 'results'
        }

        self._results = results

    @classmethod
    def from_dict(cls, dikt) -> 'TripsResponseFile':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The TripsResponseFile of this TripsResponseFile.  # noqa: E501
        :rtype: TripsResponseFile
        """
        return util.deserialize_model(dikt, cls)

    @property
    def results(self):
        """Gets the results of this TripsResponseFile.


        :return: The results of this TripsResponseFile.
        :rtype: List[TripFlatten]
        """
        return self._results

    @results.setter
    def results(self, results):
        """Sets the results of this TripsResponseFile.


        :param results: The results of this TripsResponseFile.
        :type results: List[TripFlatten]
        """

        self._results = results
