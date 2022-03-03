from differentiator.differentiator import Differentiator
from differentiator.differentiator_df import DataFrameDifferentiator
from differentiator.differentiator_rdd import ResilientDistributedDatasetDifferentiator
from pathlib import Path
from sys import argv


def diff(argv_list: list) -> None:
    # Begin

    # Print Application Start Notice
    print("Application Started!")

    # Read Differentiator Config File
    differentiator_config_file = Path(argv_list[1])

    # Init Differentiator Object
    d = Differentiator(differentiator_config_file)

    # Determine Data Structure
    data_structure = d.determine_data_structure()

    # Delete Differentiator Object
    del d

    # Diff Sequences
    if data_structure == "DataFrame":
        # Init DataFrameDifferentiator Object
        df_d = DataFrameDifferentiator(differentiator_config_file)
        df_d.start()
        df_d.diff_sequences()
        df_d.end()
        # Delete DataFrameDifferentiator Object
        del df_d
    elif data_structure == "RDD":
        # Init ResilientDistributedDatasetDifferentiator Object
        rdd_d = ResilientDistributedDatasetDifferentiator(differentiator_config_file)
        rdd_d.start()
        rdd_d.diff_sequences()
        rdd_d.end()
        # Delete ResilientDistributedDatasetDifferentiator Object
        del rdd_d

    # Print Application End Notice
    print("Application Finished Successfully!")

    # End
    exit(0)


if __name__ == "__main__":
    diff(argv)
