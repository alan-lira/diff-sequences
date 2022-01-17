class DataFrameDifferentiator(Exception):
    pass


class InvalidMaxDFError(DataFrameDifferentiator):
    pass


class InvalidPartitioningError(DataFrameDifferentiator):
    pass
