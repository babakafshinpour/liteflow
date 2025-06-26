import dask.bag as db

class DaskRDD:
    def __init__(self, bag):
        self.bag = bag

    def map(self, func):
        return DaskRDD(self.bag.map(func))

    def flatMap(self, func):
        return DaskRDD(self.bag.flatmap(func))

    def filter(self, func):
        return DaskRDD(self.bag.filter(func))

    def reduce(self, func):
        return self.bag.reduction(lambda x: x, func).compute()

    def collect(self):
        return self.bag.compute()

    def count(self):
        return self.bag.count().compute()

    def groupByKey(self):
        return DaskRDD(self.bag.groupby(lambda x: x[0]))

    def reduceByKey(self, func):
        return DaskRDD(self.bag.foldby(key=lambda x: x[0],
                                       binop=lambda acc, x: func(acc, x[1]),
                                       initial=None,
                                       combine=func))

    def sortByKey(self, ascending=True):
        # Sorting in Dask is lazy; converting to list for in-memory use
        sorted_list = sorted(self.bag.compute(), key=lambda x: x[0], reverse=not ascending)
        return DaskRDD(db.from_sequence(sorted_list))


class DaskContext:
    def __init__(self):
        print("DaskContext initialized.")

    def parallelize(self, data, npartitions=2):
        return DaskRDD(db.from_sequence(data, npartitions=npartitions))
