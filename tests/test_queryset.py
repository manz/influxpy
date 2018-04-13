from unittest import TestCase

from influxpy.fields import InfluxTag, Days
from influxpy.model import InfluxMeasurement
from influxpy.queryset import InfluxQuerySet


class TestModel(InfluxMeasurement):
    measurement = 'cpu'

    organization = InfluxTag()
    project = InfluxTag()


class QuerySetTest(TestCase):
    def test_queryset(self):
        qs = TestModel.series.filter(organization=7, project=9)

        qs = qs.group_by('organization')
        qs = qs.group_by('project')
        qs = qs.resolution(Days(1))

        self.assertEqual(
            qs.iql_query(),
            "SELECT * FROM cpu WHERE \"organization\" = '7' AND \"project\" = '9' GROUP BY time(1d), \"organization\", \"project\"")
