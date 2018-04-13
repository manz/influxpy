influxpy: time series for humans and robots
###########################################

Influxpy is a library  designed to avoid writing raw influx queries.
It's design and functionality are heavily inspired by Django models.


Installation
============

.. code-block:: bash

    $ pip install influxpy

Tests
=====

To run the tests you need a docker environment.

.. code-block:: bash

    ./run_tests.sh

But you can run them localy but you will need to have an influxdb running.


.. code-block:: bash

    pip install -r requirements.txt
    INFLUX_DATABASE_URL='influxdb://{username}:{password}@{host}/{database}' python -m unittest discover tests

Usage
=====

Model Definition
++++++++++++++++

.. code-block:: python

    from influxpy import models, fields

    class ServerMeasurement(models.InfluxMeasurement):
        measurement = 'servers'
        cpu = fields.InfluxField()
        memory = fields.InfluxField()
        free_memory = fields.InfluxField()

        name = fields.InfluxTag()
        region = fields.InfluxTag()
        optional_tag = fields.InfluxTag(null=True)


Save data points
++++++++++++++++

.. code-block:: python

    data_point = ServerMeasurement(cpu=34,
                                   memory=8192,
                                   free_memory=6542,
                                   name='i-3a99f2b',
                                   region='us-east-1',
                                   time=datetime.datetime(2017, 12, 12, 9, 45))
    data_point.save()

Queries
+++++++

To get Mean cpu between 12th december 2017 (>= 9:45 to < 9:50)  by 5 minutes slices.

.. code-block:: python

    from influxpy.aggregates import Mean
    from .measurements import ServerMeasurement

    if __name__=='__main__':
       qs = ServerMeasurement.series.filter(
    name='i-3a99f2b',
    time__gte=datetime(2017, 12, 12, 9, 45),
    time__lt=datetime(2017, 12, 12, 9, 50))
        
    qs = qs.annotate(cpu_m=Mean('cpu'))
    qs = qs.resolution(Minute(5))
    qs = qs.group_by('name')

    results = list(qs)

Would execute the following query:

.. code-block:: none

    SELECT mean(cpu_percent) AS "cpu_m" FROM server WHERE "name" = 'i-3a99f2b' AND "time" >= '2017-12-12T09:45:00Z' AND "time" < '2017-12-12T09:50:00Z' GROUP BY time(5m)

And have the follwing results

.. code-block:: none

    [
        InfluxResult(points=[{'time': '2017-12-12T09:45:00Z', 'cpu_m':103}])
    ]
