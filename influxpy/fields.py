import inspect
import logging

import functools

import datetime

logger = logging.getLogger()


class InfluxBaseField(object):
    def __init__(self, default=None, null=False):
        self.default = default
        # self._value = None
        self.null = null
        # valued by InfluxMeasurementBase.__new__
        self.name = null

    # @property
    # def value(self):
    #     return self._value or self.default
    #
    # @value.setter
    # def value(self, new_value):
    #     self._value = new_value

    def db_value(self, value):
        if value is None and not self.null:
            raise RuntimeError('The field {name} is mandatory'.format(name=self.name))
        return value

    def from_db_value(self, value):
        self.value = value

    def contribute_to_class(self, cls, name):
        self.name = name


class InfluxField(InfluxBaseField):
    def contribute_to_class(self, cls, name):
        super().contribute_to_class(cls, name)
        cls._meta.add_field(self)


class InfluxLookupField(InfluxBaseField):
    @staticmethod
    def merge_dicts(dicts):
        """
        Merge dicts in reverse to preference the order of the original list. e.g.,
        merge_dicts([a, b]) will preference the keys in 'a' over those in 'b'.
        """
        merged = {}
        for d in reversed(dicts):
            merged.update(d)
        return merged

    @classmethod
    @functools.lru_cache()
    def get_lookups(cls):
        class_lookups = [parent.__dict__.get('class_lookups', {}) for parent in inspect.getmro(cls)]
        return cls.merge_dicts(class_lookups)

    @classmethod
    def register_lookup(cls, lookup, lookup_name=None):
        if lookup_name is None:
            lookup_name = lookup.lookup_name
        if 'class_lookups' not in cls.__dict__:
            cls.class_lookups = {}
        cls.class_lookups[lookup_name] = lookup
        return lookup

    def get_lookup(self, lookup_name):
        from .lookups import Lookup

        lookup_class = self.get_lookups().get(lookup_name)

        if lookup_class and callable(lookup_class):
            return lookup_class(self)
        else:
            raise RuntimeError('{lookup_name} does not exists for {class_name}.'.format(
                lookup_name=lookup_name,
                class_name=self.__name__
            ))


class InfluxTag(InfluxLookupField):
    def contribute_to_class(self, cls, name):
        super().contribute_to_class(cls, name)
        cls._meta.add_tag(self)


class InfluxTimeField(InfluxLookupField):
    RFC3339 = '%Y-%m-%dT%H:%M:%S'

    def db_value(self, value: datetime.datetime) -> str:
        """
        Transforms a datetime into a RFC3339 datetime string.
        :param value: the datetime to transform
        :return: the RFC3339 string version of value.
        """
        return value.strftime(self.RFC3339) + "Z"

    def contribute_to_class(self, cls, name):
        super().contribute_to_class(cls, name)
        cls._meta.set_time(self)


class BaseDuration(object):
    unit = None

    def __init__(self, value):
        self.value = value

    def as_iql(self):
        try:
            return '{value}{unit}'.format(value=int(self.value), unit=self.unit)
        except ValueError:
            raise ValueError('Durations must be integers.')

    def __eq__(self, o: 'BaseDuration') -> bool:
        return self.value == o.value

    def __hash__(self) -> int:
        return self.value.__hash__()


class Seconds(BaseDuration):
    unit = 's'


class Minutes(BaseDuration):
    unit = 'm'


class Hours(BaseDuration):
    unit = 'h'


class Days(BaseDuration):
    unit = 'd'
