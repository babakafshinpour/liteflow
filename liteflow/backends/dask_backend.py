import dask.bag as db
from dask.distributed import Client, default_client
from functools import reduce
from functools import reduce as _reduce

class DaskRDD:
    def __init__(self, bag):
        self.bag = bag

    def map(self, func):
        return DaskRDD(self.bag.map(func))

    def filter(self, func):
        return DaskRDD(self.bag.filter(func))
    
    def reduce(self, func):
        return self.bag.reduction(lambda x: _reduce(func, x), lambda x: _reduce(func, x)).compute()
    
    def collect(self):
        return self.bag.compute()

    def count(self):
        return self.bag.count().compute()



class DaskContext:
    def __init__(self, scheduler_address=None, npartitions=2, **kwargs):
        """
        :param scheduler_address: Dask scheduler address (e.g. 'tcp://127.0.0.1:8786')
        :param npartitions: Number of partitions for parallelism
        :param kwargs: Additional args for dask Client
        """
        self.scheduler_address = scheduler_address
        self.npartitions = npartitions

        try:
            # Check if a client is already connected
            self.client = default_client()
            print("Using existing Dask client")
        except ValueError:
            # Create new client (local or remote)
            self.client = Client(address=scheduler_address, **kwargs)
            print(f"Initialized DaskContext (scheduler={scheduler_address or 'local'})")

    def parallelize(self, data):
        return DaskRDD(db.from_sequence(data, npartitions=self.npartitions))
