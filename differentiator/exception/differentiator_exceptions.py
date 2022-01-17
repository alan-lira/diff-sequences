class DifferentiatorError(Exception):
    pass


class InvalidSparkApplicationPropertiesError(DifferentiatorError):
    pass


class InvalidPathError(DifferentiatorError):
    pass


class InvalidDiffPhaseError(DifferentiatorError):
    pass


class InvalidCollectionPhaseError(DifferentiatorError):
    pass


class InvalidDataStructureError(DifferentiatorError):
    pass
