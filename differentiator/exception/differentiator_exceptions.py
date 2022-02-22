class DifferentiatorError(Exception):
    pass


class InvalidSparkApplicationPropertiesError(DifferentiatorError):
    pass


class InvalidPathError(DifferentiatorError):
    pass


class InvalidAllowProducerConsumerError(DifferentiatorError):
    pass


class InvalidAllowSimultaneousJobsError(DifferentiatorError):
    pass


class InvalidMaximumToleranceTimeWithoutResourcesError(DifferentiatorError):
    pass


class InvalidIntervalTimeBeforeFetchingResourcesError(DifferentiatorError):
    pass


class InvalidDataStructureError(DifferentiatorError):
    pass


class InvalidDiffPhaseError(DifferentiatorError):
    pass


class InvalidMaxSError(DifferentiatorError):
    pass


class InvalidCollectionPhaseError(DifferentiatorError):
    pass


class InvalidPartitioningError(DifferentiatorError):
    pass


class InvalidNumberOfProducersError(DifferentiatorError):
    pass


class InvalidProductsQueueMaxSizeError(DifferentiatorError):
    pass


class InvalidNumberOfConsumersError(DifferentiatorError):
    pass


class InsufficientResourcesOnClusterError(DifferentiatorError):
    pass


class InvalidFixedKError(DifferentiatorError):
    pass


class InvalidInitialKError(DifferentiatorError):
    pass


class InvalidResetKError(DifferentiatorError):
    pass
