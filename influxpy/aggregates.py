class BaseAggregate(object):
    def __init__(self):
        self.name = None

    def as_iql_part(self) -> str:
        pass

    def as_iql(self):
        parts = [
            self.as_iql_part()
        ]
        if self.name is not None:
            parts.append('AS')
            parts.append('"{name}"'.format(name=self.name))
        return ' '.join(parts)

    def __eq__(self, other):
        return self.as_iql() == other.as_iql()


class Aggregate(BaseAggregate):
    func = None

    def __init__(self, measurement):
        super().__init__()
        self.measurement = measurement

    def as_iql_part(self):
        return '{func}({measurement})'.format(
            func=self.func,
            measurement=self.measurement)


class Raw(BaseAggregate):
    def __init__(self, raw):
        super().__init__()
        self.raw = raw

    def as_iql_part(self):
        return self.raw


class Sum(Aggregate):
    func = 'sum'


class Mean(Aggregate):
    func = 'mean'
