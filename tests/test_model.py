from datetime import datetime, timedelta
from unittest.case import TestCase

from influxpy.client import client_wrapper
from influxpy.model import InfluxMeasurement, ContinuousQuery
from influxpy.fields import InfluxField, InfluxTag, Hours, Days, Minutes
from influxpy.aggregates import Raw, Sum, Mean


# def build_results(series):
#     results = None
#
#     results = {}
#     for serie in series:
#         res = {}
#         for value in serie['values']:
#             point = {}
#             for index, field in enumerate(value):
#                 field_name = serie['columns'][index]
#                 point[field_name] = field
#             res[point['time']] = point['my_SuM']
#
#             # try:
#             tags_values = list(serie['tags'].values())
#             results[tags_values[0]] = res
#             # except (KeyError, IndexError):
#             #     results = res
#     return results


# def transform_results(series, primary_tag, primary_field):
#     if series:
#         results = {}
#         for serie in series:
#             field_index = serie['columns'].index(primary_field)
#             time_index = serie['columns'].index('time')
#             serie_results = {}
#             for value in serie['values']:
#                 serie_results[value[time_index]] = value[field_index]
#             results[serie['tags'][primary_tag]] = serie_results
#         return results
#     else:
#         return None

class CounterMeasurement(InfluxMeasurement):
    measurement = 'counter'

    counter = InfluxField()
    organization = InfluxTag()
    project = InfluxTag()
    kind = InfluxTag()
    product = InfluxTag()

    class Meta:
        aggregated_measurements = {
            Days(1): 'counter_day',
            Hours(1): 'counter_hour',
            Minutes(1): 'counter_minute'
        }


class InfluxTestCase(TestCase):
    def beforeAll(self):
        client = client_wrapper.client
        # client.get


class ModelTest(TestCase):
    def setUp(self):
        client_wrapper.get_client().query('DROP SERIES FROM counter')

    def test_model(self):
        pivot = datetime(2017, 12, 12, 8, 57, 22)
        CounterMeasurement(time=pivot - timedelta(days=5),
                           organization=2,
                           product='STORES',
                           kind='map_load',
                           project=1,
                           counter=10).save()

        CounterMeasurement(time=pivot - timedelta(days=5, hours=1),
                           organization=2,
                           product='STORES',
                           kind='map_load',
                           project=1,
                           counter=15).save()

        CounterMeasurement(time=pivot - timedelta(days=5, hours=2),
                           organization=2,
                           product='STORES',
                           kind='search',
                           project=1,
                           counter=100).save()

        CounterMeasurement(time=pivot - timedelta(days=5, hours=2),
                           organization=2,
                           product='STORES',
                           kind='search',
                           project=2,
                           counter=100).save()

        qs = CounterMeasurement.series.filter(
            time__between=(
                pivot - timedelta(days=6),
                pivot + timedelta(days=4)
            ),
            organization=2,
            product='STORES',
            kind='map_load',
            project=1
        )
        qs = qs.group_by('product', 'organization', 'project', 'kind')
        qs = qs.annotate(my_SuM=Sum('counter'))
        qs = qs.resolution(Days(1)).fill(0)

        print(qs.iql_query())

        for result_set in qs:
            print(result_set)
        # self.assertEqual(qs.iql_query(), "SELECT mean(counter) AS \"my_SuM\" FROM counter WHERE "
        #                                  "kind = 'map_load' AND "
        #                                  "time >= '2017-12-02T08:57:22Z' "
        #                                  "AND time <= '2017-12-13T08:57:22Z' "
        #                                  "GROUP BY time(1h), organization, project, kind, product fill(0)")

        results = list(qs)
        print(qs.dict_values('kind', 'my_SuM'))
        self.assertEqual(len(results), 1)

        first_result = results[0]
        self.assertEqual(first_result['columns'], [
            'time',
            'my_SuM'
        ])

        self.assertEqual(first_result['name'], 'counter')

        self.assertEqual(first_result['tags'], {
            'organization': '2',
            'product': 'STORES',
            'kind': 'map_load',
            'project': '1'
        })

        self.assertEqual(first_result['values'][1], ['2017-12-07T00:00:00Z', 25])

    def test_continuous_queries(self):
        qs = CounterMeasurement.series.group_by(
            'organization',
            'project',
            'kind',
            'product'
        ).resolution(Days(1)).annotate(counter=Sum('counter')).into('counter_day')

        cq = ContinuousQuery(queryset=qs,
                             name='cq_counter_day',
                             source='influx',
                             resample_every=Hours(1),
                             resample_for=Days(2))
        self.assertEqual(
            cq.iql_query(),
            'CREATE CONTINUOUS QUERY "cq_counter_day" ON influx RESAMPLE EVERY 1h FOR 2d BEGIN SELECT sum(counter) AS "counter" INTO counter_day FROM counter GROUP BY time(1d), organization, project, kind, product END')

    def test_model_save(self):
        point1 = CounterMeasurement(
            organization=1,
            project=2,
            counter=22,
            kind='map_load',
            product='STORES',
            time=datetime.now())

        point2 = CounterMeasurement(
            organization=2,
            project=3,
            counter=6,
            kind='kiki',
            product='pouet',
            time=datetime.now())
        point2.save()
        point1.save()
