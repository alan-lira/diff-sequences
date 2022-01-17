class ResilientDistributedDatasetDifferentiator(Exception):
    pass


class InvalidMaxRDDError(ResilientDistributedDatasetDifferentiator):
    pass


class InvalidPartitioningError(ResilientDistributedDatasetDifferentiator):
    pass
