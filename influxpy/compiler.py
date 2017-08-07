from influxpy.fields import BaseDuration


class InfluxCompiler(object):
    def __init__(self, model) -> None:
        super().__init__()
        self.model = model

    def get_table_for_time(self, query):
        if query.destination is None:
            if query.can_use_aggregated_measurement:
                mapping = getattr(self.model._meta, 'aggregated_measurements', {})
                return mapping.get(query.resolution, self.model.measurement)

        return self.model.measurement

    def compile_group_by(self, query):
        parts = []

        if query.resolution:
            value = query.resolution

            if isinstance(query.resolution, BaseDuration):
                value = query.resolution.as_iql()

            parts.append('time({resolution})'.format(resolution=value))

        if query.group_by:
            parts = parts + query.group_by

        return parts

    def compile_selected_fields(self, query):
        parts = []
        if query.annotations:
            for annotation in query.annotations:
                parts.append(annotation.as_iql())
        return parts

    @staticmethod
    def quote(value):
        if type(value) == str:
            return '"{value}"'.format(value=value)
        else:
            return str(value)

    @staticmethod
    def single_quote(value):
        return "'{value}'".format(value=value)

    def compile_where(self, query):
        filters = []
        
        for f, value in query.filters.items():
            lookup = None
            field_name = None
            field = None

            if '__' in f:
                field_name, lookup_key = f.split('__')
            else:
                field_name = f
                lookup_key = 'equals'

            try:
                field = self.model._meta.all_fields[field_name]
            except KeyError:
                raise RuntimeError('Field "{}" does not exists for "{}"'.format(field_name, self.model.__name__))

            lookup = field.get_lookup(lookup_key)
            if lookup is not None:
                filters.append(lookup.as_iql(value))

        return ' AND '.join(filters)

    def get_query_parts(self, query):
        selected_fields = self.compile_selected_fields(query)
        where = self.compile_where(query)
        group_by = self.compile_group_by(query)
        table_name = self.get_table_for_time(query)
        resolution = query.resolution
        destination = query.destination
        fill = query.fill

        parts = ['SELECT']

        if selected_fields:
            parts.append(', '.join(selected_fields))
        else:
            parts.append('*')

        if destination:
            parts.append('INTO')
            parts.append(destination)

        parts.append('FROM')
        parts.append(table_name)

        if where:
            parts.append('WHERE')
            parts.append(where)

        if group_by:
            parts.append('GROUP BY')
            parts.append(', '.join(group_by))
            if fill is not None:
                parts.append('fill({})'.format(str(fill)))
        return parts

    def compile(self, query):
        return ' '.join(self.get_query_parts(query))
