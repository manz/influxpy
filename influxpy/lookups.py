from influxpy.fields import InfluxLookupField, InfluxTimeField


class Lookup(object):
    def __init__(self, field):
        self.field = field

    def as_iql(self, rhs):
        pass


class BuiltinLookup(Lookup):
    operator = None

    def as_iql(self, rhs):  # , compiler, rhs):
        lhs_iql = self.field.name
        rhs_iql = "'%s'" % self.field.db_value(rhs)
        return '%s %s %s' % (lhs_iql, self.operator, rhs_iql)


@InfluxLookupField.register_lookup
class Equal(BuiltinLookup):
    lookup_name = 'equals'
    operator = '='


@InfluxLookupField.register_lookup
class GreaterThan(BuiltinLookup):
    lookup_name = 'gt'
    operator = '>'


@InfluxLookupField.register_lookup
class GreaterThanOrEqual(BuiltinLookup):
    lookup_name = 'gte'
    operator = '>='


@InfluxLookupField.register_lookup
class LessThan(BuiltinLookup):
    lookup_name = 'lt'
    operator = '<'


@InfluxLookupField.register_lookup
class LessThanOrEqual(BuiltinLookup):
    lookup_name = 'lte'
    operator = '<='


@InfluxTimeField.register_lookup
class Between(BuiltinLookup):
    lookup_name = 'between'

    def as_iql(self, rhs):
        if isinstance(rhs, tuple) and len(rhs) == 2:
            first_rhs = rhs[0]
            second_rhs = rhs[1]

            gte = GreaterThanOrEqual(self.field)
            lte = LessThanOrEqual(self.field)

            return ' AND '.join([gte.as_iql(first_rhs),
                                 lte.as_iql(second_rhs)])
