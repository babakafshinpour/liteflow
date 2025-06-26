import ray
from functools import reduce
ray.init(ignore_reinit_error=True)

class RayRDD:
    def __init__(self, data_refs):
        self.data_refs = data_refs

    def map(self, func):
        @ray.remote
        def map_chunk(chunk):
            return list(map(func, chunk))

        return RayRDD([map_chunk.remote(ref) for ref in self.data_refs])

    def filter(self, func):
        @ray.remote
        def filter_chunk(chunk):
            return [x for x in chunk if func(x)]

        return RayRDD([filter_chunk.remote(ref) for ref in self.data_refs])

    def reduce(self, func):
        chunks = ray.get(self.data_refs)
        flat = [x for chunk in chunks for x in chunk]
        return reduce(func, flat)

    def collect(self):
        return [x for chunk in ray.get(self.data_refs) for x in chunk]

    def count(self):
        return sum(len(chunk) for chunk in ray.get(self.data_refs))



class RayContext:
    def __init__(self, address=None, num_splits=4, **kwargs):
        """
        :param address: Ray cluster address (e.g. 'auto' or '192.168.1.10:6379')
        :param num_splits: Number of partitions to split data into
        :param kwargs: Passed to ray.init()
        """
        self.address = address
        self.num_splits = num_splits

        if not ray.is_initialized():
            ray.init(address=self.address, **kwargs)

        print(f"Initialized RayContext (address={self.address or 'local'})")

    def parallelize(self, data):
        chunk_size = max(1, len(data) // self.num_splits)
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        return RayRDD([ray.put(chunk) for chunk in chunks])