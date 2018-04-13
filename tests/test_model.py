from datetime import datetime, timedelta

from influxpy.model import InfluxMeasurement, ContinuousQuery
from influxpy.fields import InfluxField, InfluxTag, Hours, Days, Minutes
from influxpy.aggregates import Raw, Sum, Mean
from influxpy.queryset import InfluxSeries
from tests import InfluxTestCase


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


class ServerMeasurement(InfluxMeasurement):
    measurement = 'server'

    cpu_percent = InfluxField()
    memory_free = InfluxField()
    memory_used = InfluxField()
    memory_total = InfluxField()
    region = InfluxTag()
    name = InfluxTag()


class ModelTest(InfluxTestCase):
    def test_server_model(self):
        pivot = datetime(2017, 12, 12, 8, 57, 22)

        ServerMeasurement(
            cpu_percent=13,
            memory_free=128,
            memory_used=200,
            memory_total=512,

            region='us-east-1',
            name='i-12345678',

            time=pivot
        ).save()

        ServerMeasurement(
            cpu_percent=50,
            memory_free=0,
            memory_used=512,
            memory_total=512,

            region='us-east-1',
            name='i-12345678',
            time=pivot + timedelta(minutes=1)
        ).save()

        ServerMeasurement(
            cpu_percent=70,
            memory_free=2048,
            memory_used=2048,
            memory_total=4096,

            region='eu-west-1',
            name='i-87654321',
            time=pivot
        ).save()

        qs = ServerMeasurement.series.filter(
            time__between=(
                pivot,
                pivot + timedelta(minutes=1)
            )).group_by('name')

        qs = qs.annotate(
            cpu_percent_m=Mean('cpu_percent'),
            memory_used_m=Mean('memory_used')
        )

        qs = qs.resolution(Hours(1))

        def order_by_name(series):
            return sorted(series, key=lambda e: e['tags']['name'])

        results = list(qs)
        self.assertEqual(results,
                         [
                             InfluxSeries(
                                 points=[{'time': '2017-12-12T08:00:00Z', 'memory_used_m': 356, 'cpu_percent_m': 31.5}],
                                 tags={'name': 'i-12345678'}),
                             InfluxSeries(
                                 points=[{'time': '2017-12-12T08:00:00Z', 'memory_used_m': 2048, 'cpu_percent_m': 70}],
                                 tags={'name': 'i-87654321'})
                         ])

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


          # self.assertEqual(qs.iql_query(), "SELECT mean(counter) AS \"my_SuM\" FROM counter WHERE "
        #                                  "kind = 'map_load' AND "
        #                                  "time >= '2017-12-02T08:57:22Z' "
        #                                  "AND time <= '2017-12-13T08:57:22Z' "
        #                                  "GROUP BY time(1h), organization, project, kind, product fill(0)")

        results = list(qs)
        self.assertEqual(len(results), 1)

        first_result = results[0]
        self.assertEqual(sorted(list(first_result.points[0].keys())), [
            'my_SuM',
            'time'
        ])

        self.assertEqual(dict(first_result.tags), {
            'organization': '2',
            'product': 'STORES',
            'kind': 'map_load',
            'project': '1'
        })

        self.assertEqual(first_result.points[1], {'time': '2017-12-07T00:00:00Z', 'my_SuM': 25})

    def test_continuous_queries(self):
        qs = CounterMeasurement.series.group_by(
            'organization',
            'project',
            'kind',
            'product'
        ).resolution(Days(1)).annotate(counter=Sum('counter')).into('counter_day').use_downsampled()

        cq = ContinuousQuery(queryset=qs,
                             name='cq_counter_day',
                             source='influx',
                             resample_every=Hours(1),
                             resample_for=Days(2))
        self.assertEqual(
            cq.iql_query(),
            'CREATE CONTINUOUS QUERY "cq_counter_day" ON influx RESAMPLE EVERY 1h FOR 2d BEGIN SELECT sum(counter) AS "counter" INTO counter_day FROM counter_minute GROUP BY time(1d), "organization", "project", "kind", "product" END')

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

    def test_model_multiple_fields(self):
        pivot = datetime(2017, 12, 12, 8, 57, 22)

        class MultipleFieldsMeasurement(InfluxMeasurement):
            measurement = "toto"
            cpu = InfluxField()
            memory = InfluxField()
            disk_io = InfluxField()

        MultipleFieldsMeasurement(time=pivot - timedelta(hours=1), cpu=50, memory=750, disk_io=100).save()
        MultipleFieldsMeasurement(time=pivot, cpu=30, memory=1024, disk_io=25).save()
        MultipleFieldsMeasurement(time=pivot + timedelta(hours=1), cpu=300, memory=880, disk_io=0).save()

        qs = MultipleFieldsMeasurement.series
        qs = qs.annotate(cpu_a=Mean('cpu'), memory_a=Mean('memory'), disk_io_a=Mean('disk_io'))
        qs = qs.fill(0)
        qs = qs.filter(time__between=(pivot - timedelta(hours=3), pivot + timedelta(hours=3)))
        qs = qs.resolution(Hours(1))


        results = list(qs)

    def test_docs(self):
        ServerMeasurement(cpu_percent=34,
                          memory_total=8192,
                          memory_used=6542,
                          memory_free=8192 - 6542,
                          name='i-3a99f2b',
                          region='us-east-1',
                          time=datetime(2017, 12, 12, 9, 45)).save()
        ServerMeasurement(cpu_percent=75,
                          memory_total=8192,
                          memory_used=6542,
                          memory_free=8192 - 6542,
                          name='i-3a99f2b',
                          region='us-east-1',
                          time=datetime(2017, 12, 12, 9, 46)).save()
        ServerMeasurement(cpu_percent=200,
                          memory_total=8192,
                          memory_used=6542,
                          memory_free=8192 - 6542,
                          name='i-3a99f2b',
                          region='us-east-1',
                          time=datetime(2017, 12, 12, 9, 47)).save()

        qs = ServerMeasurement.series.filter(
            name='i-3a99f2b',
            time__gte=datetime(2017, 12, 12, 9, 45),
            time__lt=datetime(2017, 12, 12, 9, 50))

        qs = qs.annotate(cpu_m=Mean('cpu_percent'))
        qs = qs.resolution(Minutes(5))

