import os
from influxdb import InfluxDBClient


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
        self.client = client  # type: InfluxDBClient

    @staticmethod
    def get_default_client() -> InfluxDBClient:
        database_url = os.environ.get('INFLUXDB_URL', None)
        from urllib.parse import urlparse
        parse_result = urlparse(database_url)

        if parse_result.scheme != 'influxdb':
            raise RuntimeError('The DATABASE_URL scheme should be influxdb.')

        client_params = dict()
        client_params['host'] = parse_result.hostname
        client_params['username'] = parse_result.username
        client_params['password'] = parse_result.password
        client_params['database'] = parse_result.path.replace('/', '')

        return InfluxDBClient(**client_params)

    def get_client(self) -> InfluxDBClient:
        if self.client is None:
            self.client = self.get_default_client()

        return self.client

    def set_client(self, new_client):
        self.client = new_client


client_wrapper = InfluxClientWrapper()
