import copy
from datetime import timedelta
from unittest.case import TestCase

import datetime

from influxpy.aggregates import Sum
from influxpy.fields import InfluxField, InfluxTag, Days
from influxpy.model import InfluxMeasurement
from influxpy.query import InfluxQuery


class RealtimeCounter(InfluxMeasurement):
    measurement = 'counter'

    counter = InfluxField()
    organization = InfluxTag()
    project = InfluxTag()
    product = InfluxTag()
    kind = InfluxTag()


class QueryTest(TestCase):
    def test_query_deepcopy(self):
        query = InfluxQuery()
        query.filters = {
            'toto': ['a']
        }

        query_copy = copy.deepcopy(query)

        query_copy.filters['toto'].append('b')
        query_copy.group_by.append('tag')
        query_copy.annotations.append(Sum('counter'))

        self.assertEqual(query.filters, {'toto': ['a']})
        self.assertEqual(query_copy.filters, {'toto': ['a', 'b']})

        self.assertEqual(query.group_by, [])
        self.assertEqual(query_copy.group_by, ['tag'])

        self.assertEqual(query.annotations, [])
        self.assertEqual(query_copy.annotations, [Sum('counter')])

    def test_query(self):
        qs = RealtimeCounter.series.filter(product='STORES',
                                           time__between=(datetime.datetime(2017, 8, 1),
                                                          datetime.datetime(2017, 8, 1) + timedelta(days=30)))
        qs = qs.annotate(Sum('counter')).group_by('product').resolution(Days(30))
