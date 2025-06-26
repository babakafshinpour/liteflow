import ray
from typing import Callable, List, Any
from functools import reduce

ray.init(ignore_reinit_error=True)

class RayRDD:
    def __init__(self, data_refs: List[ray.ObjectRef]):
        self.data_refs = data_refs

    def map(self, func: Callable):
        @ray.remote
        def map_chunk(chunk):
            return list(map(func, chunk))

        new_refs = [map_chunk.remote(ref) for ref in self.data_refs]
        return RayRDD(new_refs)

    def flatMap(self, func: Callable):
        @ray.remote
        def flatmap_chunk(chunk):
            return [y for x in chunk for y in func(x)]

        new_refs = [flatmap_chunk.remote(ref) for ref in self.data_refs]
        return RayRDD(new_refs)

    def filter(self, func: Callable):
        @ray.remote
        def filter_chunk(chunk):
            return [x for x in chunk if func(x)]

        new_refs = [filter_chunk.remote(ref) for ref in self.data_refs]
        return RayRDD(new_refs)

    def reduce(self, func: Callable):
        chunks = ray.get(self.data_refs)
        flattened = [item for sublist in chunks for item in sublist]
        return reduce(func, flattened)

    def collect(self):
        return [item for sublist in ray.get(self.data_refs) for item in sublist]

    def count(self):
        return sum(len(sublist) for sublist in ray.get(self.data_refs))

    def sortByKey(self, ascending=True):
        collected = self.collect()
        sorted_list = sorted(collected, key=lambda x: x[0], reverse=not ascending)
        return RayRDD([ray.put(sorted_list)])

    def groupByKey(self):
        @ray.remote
        def group_chunk(chunk):
            from collections import defaultdict
            grouped = defaultdict(list)
            for k, v in chunk:
                grouped[k].append(v)
            return list(grouped.items())

        grouped_refs = [group_chunk.remote(ref) for ref in self.data_refs]
        grouped = [item for sublist in ray.get(grouped_refs) for item in sublist]

        # Merge same keys across chunks
        from collections import defaultdict
        final = defaultdict(list)
        for k, vlist in grouped:
            final[k].extend(vlist)

        return RayRDD([ray.put(list(final.items()))])

    def reduceByKey(self, func: Callable):
        grouped_rdd = self.groupByKey()
        @ray.remote
        def reduce_chunk(chunk):
            return [(k, reduce(func, v)) for k, v in chunk]

        new_refs = [reduce_chunk.remote(ref) for ref in grouped_rdd.data_refs]
        return RayRDD(new_refs)


class RayContext:
    def __init__(self, num_splits: int = 4):
        print("RayContext initialized.")
        self.num_splits = num_splits

    def parallelize(self, data: List[Any]):
        # Split into chunks
        chunk_size = max(1, len(data) // self.num_splits)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        data_refs = [ray.put(chunk) for chunk in chunks]
        return RayRDD(data_refs)
