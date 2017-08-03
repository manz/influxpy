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

    class MyMeasurement(models.InfluxMeasurement):
        measurement = 'hey'

        my_tag = fields.InfluxTag()
        optional_tag = fields.InfluxTag(null=True)


Save data points
++++++++++++++++

.. code-block:: python

    data_point = MyMeasurement(my_tag='huit', time=datetime.datetime(2017, 12, 12, 9, 45))
    data_point.save()

Queries
+++++++

.. code-block:: python

    qs = MyMeasurement.series.filter(
        my_tag='huit',
        time__between=(
            datetime.datetime(2017, 12, 11, 9, 45),
            datetime.datetime(2017, 12, 13, 9, 45)))

    results = list(qs)

    print(results)

Would execute the following query:

`SELECT * FROM hey WHERE my_tag='huit' AND time >= {} AND time <= {}`
