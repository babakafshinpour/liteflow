import dask.bag as db
from functools import reduce

class DaskRDD:
    def __init__(self, bag):
        self.bag = bag

    def map(self, func):
        return DaskRDD(self.bag.map(func))

    def filter(self, func):
        return DaskRDD(self.bag.filter(func))

    def reduce(self, func):
        return self.bag.reduction(lambda x: x, func).compute()

    def collect(self):
        return self.bag.compute()

    def count(self):
        return self.bag.count().compute()


class DaskContext:
    def __init__(self, npartitions=2):
        print("Initialized DaskContext")
        self.npartitions = npartitions

    def parallelize(self, data):
        return DaskRDD(db.from_sequence(data, npartitions=self.npartitions))