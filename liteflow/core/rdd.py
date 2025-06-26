class BaseRDD:
    def map(self, func):
        raise NotImplementedError()

    def filter(self, func):
        raise NotImplementedError()

    def reduce(self, func):
        raise NotImplementedError()

    def collect(self):
        raise NotImplementedError()
