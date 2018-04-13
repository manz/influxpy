import inspect

import copy
from influxpy.client import client_wrapper
from influxpy.fields import InfluxBaseField, InfluxTimeField, BaseDuration
from influxpy.queryset import InfluxQuerySet


class Options(object):
    def __init__(self, meta, base_meta):
        self.parent_meta = base_meta
        self.tags = {}
        self.fields = {}
        self.all_fields = {}

        if base_meta:
            self.tags = copy.deepcopy(base_meta.tags)
            self.fields = copy.deepcopy(base_meta.fields)
            self.all_fields = copy.deepcopy(base_meta.all_fields)

        if meta:
            self.aggregated_measurements = copy.deepcopy(getattr(meta, 'aggregated_measurements'))
        else:
            self.aggregated_measurements = {}

    def add_field(self, field):
        self.all_fields[field.name] = field
        self.fields[field.name] = field

    def add_tag(self, tag):
        self.all_fields[tag.name] = tag
        self.tags[tag.name] = tag

    def set_time(self, time):
        self.all_fields['time'] = time


class InfluxMeasurementBase(type):
    def __new__(cls, name, bases, attrs):
        """
        :param cls: InfluxMeasurementBase
        :param name:
        :param bases:
        :param attrs:
        :return:
        """
        module = attrs.pop('__module__')
        new_attrs = {'__module__': module}

        new_class = super().__new__(cls, name, bases, new_attrs)  # type: InfluxMeasurementBase

        attrs_meta = attrs.get('Meta', None)
        base_meta = getattr(new_class, '_meta', None)

        meta = Options(attrs_meta, base_meta)

        new_class.add_to_class('_meta', meta)

        for name, attr in attrs.items():
            # avoids overwriting _meta that's because we defined _meta and series on InfluxMeasurement to avoid
            # pycharm errors.
            if name != '_meta':
                new_class.add_to_class(name, attr)

        new_class.add_to_class('series', InfluxQuerySet())

        return new_class

    def add_to_class(cls, name, value):
        if not inspect.isclass(value) and hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)


class InfluxMeasurement(metaclass=InfluxMeasurementBase):
    measurement = None
    time = InfluxTimeField()
    series = None  # type: InfluxQuerySet
    _meta = None  # type: Options

    def __init__(self, **kwargs):
        if self.measurement is None:
            raise RuntimeError('measurement cannot be None.')

        # populates attributes for fields on self
        for key, value in kwargs.items():
            field = self._meta.all_fields.get(key)
            if field is None:
                raise RuntimeError('The field {key} does not exists.'.format(key=key))

            if not isinstance(field, InfluxBaseField):
                raise RuntimeError('Only fields and tags can be set through __init__.')
            setattr(self, field.name, value)

    def get_tags_and_fields(self):
        """
        Gets the tags and fields from _meta
        :return: [Dict,Dict]
        """
        fields = {}
        tags = {}

        for field_name, field in self._meta.fields.items():
            fields[field_name] = field.db_value(getattr(self, field.name))

        for tag_name, tag in self._meta.tags.items():
            tags[tag_name] = tag.db_value(getattr(self, tag.name))

        return tags, fields

    def save(self):
        """
        Sends the data point to influxdb using the client_wrapper.
        :return: None
        """
        tags, fields = self.get_tags_and_fields()
        data = [{
            'measurement': self.measurement,
            'tags': tags,
            'time': self._meta.all_fields['time'].db_value(self.time),
            'fields': fields
        }]

        client = client_wrapper.write_points(data)


class ContinuousQuery(object):
    """
    Builds a continuous query using __init__ params.
    """

    def __init__(self,
                 queryset: InfluxQuerySet,
                 name: str,
                 source: str,
                 resample_every: BaseDuration,
                 resample_for: BaseDuration = None):
        self.query = queryset.iql_query()
        self.name = name
        self.resample_every = resample_every
        self.resample_for = resample_for
        self.source = source

    def iql_query(self) -> str:
        parts = [
            'CREATE CONTINUOUS QUERY',
            '"{name}"'.format(name=self.name),
            'ON',
            self.source,
            'RESAMPLE EVERY',
            self.resample_every.as_iql()
        ]

        if self.resample_for:
            parts.append('FOR')
            parts.append(self.resample_for.as_iql())

        parts.append('BEGIN')
        parts.append(self.query)
        parts.append('END')

        return ' '.join(parts)

    def save(self, force=False):
        if force:
            client_wrapper.query('DROP CONTINUOUS QUERY {name}')
        client_wrapper.query(self.iql_query())
