import os

import logging
from influxdb import InfluxDBClient
import os
import binascii

from influxdb.resultset import ResultSet

logger = logging.getLogger('influxpy.client')


class SingletonMetaClass(type):
    def __new__(cls, name, bases, dict):
        original_new = cls.__new__

        def my_new(cls, *args, **kwargs):
            if cls.instance is None:
                cls.instance = original_new(cls, *args, **kwargs)
            return cls.instance

        cls.instance = None
        cls.__new__ = staticmethod(my_new)
        return super(SingletonMetaClass, cls).__new__(cls, name, bases, dict)


class InfluxClientWrapper(metaclass=SingletonMetaClass):
    def __init__(self, client=None):
        self._database = None
        self._original_database = None
        self.client = client  # type: InfluxDBClient
        _ = self.get_client()  # ensures that client is initialized

    # FIXME: This should be less shitty
    def get_default_client(self) -> InfluxDBClient:
        database_url = os.environ.get('INFLUXDB_URL', None)
        from urllib.parse import urlparse
        parse_result = urlparse(database_url)

        if parse_result.scheme != 'influxdb':
            logger.error('The DATABASE_URL scheme should be influxdb, returning default influx client.')
            return InfluxDBClient()

        client_params = dict()
        client_params['host'] = parse_result.hostname
        client_params['username'] = parse_result.username
        client_params['password'] = parse_result.password
        self._database = parse_result.path.replace('/', '')

        return InfluxDBClient(**client_params)

    def get_client(self) -> InfluxDBClient:
        if self.client is None:
            self.client = self.get_default_client()

        return self.client

    def setup_test_database(self):
        """Uses config to draft a new database"""
        if self._original_database is not None:
            logger.error('Client was already modified for tests.')
        else:
            self._original_database = self.database
            self._database = 'test_' + self.database + '_' + binascii.b2a_hex(os.urandom(8)).decode('utf-8')
            logger.debug('using {}'.format(self.database))

    def restore_test_database(self):
        if self._original_database is None:
            logger.error('Client was not modified for tests.')
        else:
            self._database = self._original_database
            self._original_database = None

    def query(self, query):
        return self.get_client().query(query, database=self.database)

    def write_points(self, points):
        return self.get_client().write_points(points, database=self.database)

    def create_database(self):
        return self.get_client().create_database(self.database)

    def drop_database(self):
        return self.get_client().drop_database(self.database)

    def drop_measurements(self):
        measurements = self.query('SHOW MEASUREMENTS')
        return self.query('DROP MEASUREMENT counter')

    @property
    def database(self):
        return self._database


client_wrapper = InfluxClientWrapper()
