from collections import defaultdict
from typing import Callable, List, Tuple, Any


class LocalRDD:
    def __init__(self, data: List[Any]):
        self.data = data

    def map(self, func: Callable):
        return LocalRDD([func(x) for x in self.data])

    def flatMap(self, func: Callable):
        return LocalRDD([y for x in self.data for y in func(x)])

    def filter(self, func: Callable):
        return LocalRDD([x for x in self.data if func(x)])

    def reduce(self, func: Callable):
        from functools import reduce
        return reduce(func, self.data)

    def collect(self):
        return self.data

    def count(self):
        return len(self.data)

    def sortByKey(self, ascending=True):
        return LocalRDD(sorted(self.data, key=lambda x: x[0], reverse=not ascending))

    def groupByKey(self):
        grouped = defaultdict(list)
        for k, v in self.data:
            grouped[k].append(v)
        return LocalRDD(list(grouped.items()))

    def reduceByKey(self, func: Callable):
        grouped = defaultdict(list)
        for k, v in self.data:
            grouped[k].append(v)
        reduced = [(k, self._reduce_list(vs, func)) for k, vs in grouped.items()]
        return LocalRDD(reduced)

    def _reduce_list(self, values: List[Any], func: Callable):
        from functools import reduce
        return reduce(func, values)


class LocalSparkContext:
    def __init__(self):
        print("LocalSparkContext initialized.")

    def parallelize(self, data: List[Any], num_partitions: int = 1) -> LocalRDD:
        return LocalRDD(data)
