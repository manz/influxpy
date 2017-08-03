import copy
from unittest.case import TestCase

from influxpy.aggregates import Sum
from influxpy.query import InfluxQuery


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
