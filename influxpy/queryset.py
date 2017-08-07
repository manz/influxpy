import copy
from collections import OrderedDict
from typing import Dict

from influxpy.client import client_wrapper
from influxpy.compiler import InfluxCompiler
from influxpy.aggregates import BaseAggregate
from influxpy.query import InfluxQuery


class InfluxQuerySet(object):
    """Represent a lazy database lookup."""

    def __init__(self):
        self.query = InfluxQuery()
        self.compiler = None  # type: InfluxCompiler
        self.model = None

    def _fetch_results(self):
        influx_query = self.compiler.compile(self.query)
        client = client_wrapper.get_client()
        result_set = client.query(influx_query, database='quota')
        raw = result_set.raw
        return raw.get('series', [])

    def __iter__(self):
        return iter(self._fetch_results())

    def iql_query(self):
        """Returns the Influx query"""
        return self.compiler.compile(self.query)

    def filter(self, **kwargs):
        """
        Return a new QuerySet instance with the args ANDed to the existing
        set.
        """
        clone = self.clone()
        ordered_kwargs = OrderedDict(sorted(kwargs.items(), key=lambda t: t[0]))
        clone.query.filters.update(ordered_kwargs)
        return clone

    def group_by(self, *args: str):
        """
        Returns a query set with additional group bys.
        :param args:
        :return:
        """
        clone = self.clone()
        clone.query.group_by = clone.query.group_by + list(args)
        return clone

    def into(self, destination: str):
        """

        :param destination:
        :return:
        """
        clone = self.clone()
        clone.query.destination = destination
        return clone

    def resolution(self, resolution):
        """
        Returns a query set with time group by set to resolution.
        :param resolution:
        :return:
        """
        clone = self.clone()
        clone.query.resolution = resolution
        return clone

    def fill(self, value: int):
        """
        Return a query set where time frames with no data will be filled with value.
        :param value:
        """
        clone = self.clone()
        clone.query.fill = value
        return clone

    def annotate(self, *args: BaseAggregate, **kwargs: BaseAggregate):
        """
        Return a query set in which the returned objects have been annotated
        with extra data or aggregations.
        """
        clone = self.clone()
        for annotation in args:
            clone.query.annotations.append(annotation)

        for name, annotation in kwargs.items():
            annotation.name = name
            clone.query.annotations.append(annotation)

        return clone

    def use_downsampled(self):
        """
        Returns a query set but can use downsampled tables
        :return:
        """
        clone = self.clone()
        clone.query.can_use_aggregated_measurement = True
        return clone

    # FIXME: move this out. This is too specific to be part of the QuerySet Api
    def dict_values(self, primary_tag, primary_field) -> Dict[str, Dict[str, int or float]]:
        """
        Fetches the resultSet from influx but returns a dictionary where keys are primary_tag value and values are dicts
        :param primary_tag: str
        :param primary_field: str
        :return: Dict[str, Dict[str, int or float]]
        """
        series = self._fetch_results()

        results = {}
        for serie in series:
            field_index = serie['columns'].index(primary_field)
            time_index = serie['columns'].index('time')
            serie_results = {}
            for value in serie['values']:
                serie_results[value[time_index]] = value[field_index]
            results[serie['tags'][primary_tag]] = serie_results
        return results

    def clone(self):
        clone = InfluxQuerySet()
        clone.query = copy.deepcopy(self.query)
        clone.model = self.model
        clone.compiler = self.compiler
        return clone

    def contribute_to_class(self, model, name):
        self.model = model
        self.compiler = InfluxCompiler(model)
        setattr(model, name, self)
