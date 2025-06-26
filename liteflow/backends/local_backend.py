class LocalRDD:
    def __init__(self, data):
        self.data = data

    def map(self, func):
        return LocalRDD([func(x) for x in self.data])

    def filter(self, func):
        return LocalRDD([x for x in self.data if func(x)])

    def reduce(self, func):
        from functools import reduce
        return reduce(func, self.data)

    def collect(self):
        return self.data

    def count(self):
        return len(self.data)

    # Add groupByKey, reduceByKey, etc., if needed


class LocalContext:
    def __init__(self):
        print("Initialized LocalContext")

    def parallelize(self, data):
        return LocalRDD(data)
