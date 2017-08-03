import copy
from collections import OrderedDict


class InfluxQuery(object):
    def __init__(self):
        self.resolution = None
        self.group_by = []
        self.filters = OrderedDict()
        self.destination = None
        self.fill = None
        self.annotations = []
        self.can_use_aggregated_measurement = False

    def __deepcopy__(self, memo):
        obj = self.__class__()
        for k, v in self.__dict__.items():
            obj.__dict__[k] = copy.deepcopy(v, memo=memo)
        return obj
