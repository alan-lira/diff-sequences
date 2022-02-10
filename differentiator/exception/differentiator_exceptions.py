class DifferentiatorError(Exception):
    pass


class InvalidSparkApplicationPropertiesError(DifferentiatorError):
    pass


class InvalidPathError(DifferentiatorError):
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


class InvalidFixedKError(DifferentiatorError):
    pass


class InvalidInitialKError(DifferentiatorError):
    pass


class InvalidResetKError(DifferentiatorError):
    pass
