import copy
from collections import OrderedDict, namedtuple
from typing import Dict, List

from influxdb.resultset import ResultSet

from influxpy.client import client_wrapper
from influxpy.compiler import InfluxCompiler
from influxpy.aggregates import BaseAggregate
from influxpy.fields import BaseDuration
from influxpy.query import InfluxQuery


class InfluxSeries(namedtuple('InfluxResult', ['points', 'tags'])):
    def __eq__(self, other: 'InfluxSeries'):
        return self.points == other.points and self.tags == other.tags


class InfluxQuerySet(object):
    """Represent a lazy database lookup."""

    def __init__(self):
        self.query = InfluxQuery()
        self.compiler = None  # type: InfluxCompiler
        self.model = None

    def _fetch_results(self):  # -> List[InfluxResult]:
        influx_query = self.compiler.compile(self.query)
        result_set = client_wrapper.query(influx_query)  # type: ResultSet
        results = []
        series = result_set.raw.get('series', [])
        for serie in series:

            columns = serie['columns']
            tags = serie.get('tags')
            points = []
            for value in serie['values']:
                point = {}
                for col_index, col in enumerate(columns):
                    point[col] = value[col_index]
                points.append(point)

            yield InfluxSeries(points=points, tags=tags)

    def __iter__(self):
        return iter(self._fetch_results())

    def iql_query(self) -> str:
        """Returns the Influx query"""
        return self.compiler.compile(self.query)

    def using(self, database_alias: str) -> 'InfluxQuerySet':
        clone = self.clone()
        clone.query.database = database_alias
        return clone

    def filter(self, **kwargs) -> 'InfluxQuerySet':
        """
        Return a new QuerySet instance with the args ANDed to the existing
        set.
        """
        clone = self.clone()
        ordered_kwargs = OrderedDict(sorted(kwargs.items(), key=lambda t: t[0]))
        clone.query.filters.update(ordered_kwargs)
        return clone

    def all(self) -> 'InfluxQuerySet':
        """
        Returns an unfiltered
        :return:
        """
        clone = self.clone()
        return clone

    def group_by(self, *args: str) -> 'InfluxQuerySet':
        """
        Returns a query set with additional group bys.
        :param args:
        :return:
        """
        clone = self.clone()
        clone.query.group_by = clone.query.group_by + list(args)
        return clone

    def into(self, destination) -> 'InfluxQuerySet':
        """

        :param destination:
        :return:
        """
        clone = self.clone()
        clone.query.destination = destination
        return clone

    def resolution(self, resolution: BaseDuration) -> 'InfluxQuerySet':
        """
        Returns a query set with time group by set to resolution.
        :param resolution:
        :return:
        """
        clone = self.clone()
        clone.query.resolution = resolution
        return clone

    def fill(self, value) -> 'InfluxQuerySet':
        """
        Return a query set where time frames with no data will be filled with value.
        :param value:
        """
        clone = self.clone()
        clone.query.fill = value
        return clone

    def annotate(self, *args: BaseAggregate, **kwargs: BaseAggregate) -> 'InfluxQuerySet':
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

    def use_downsampled(self) -> 'InfluxQuerySet':
        """
        Returns a query set but can use downsampled tables
        :return:
        """
        clone = self.clone()
        clone.query.can_use_aggregated_measurement = True
        return clone

    def clone(self) -> 'InfluxQuerySet':
        clone = InfluxQuerySet()
        clone.query = copy.deepcopy(self.query)
        clone.model = self.model
        clone.compiler = self.compiler
        return clone

    def contribute_to_class(self, model, name):
        self.model = model
        self.compiler = InfluxCompiler(model)
        setattr(model, name, self)
