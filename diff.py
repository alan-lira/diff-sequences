from differentiator.differentiator import Differentiator
from differentiator.differentiator_df import DataFrameDifferentiator
from differentiator.differentiator_rdd import ResilientDistributedDatasetDifferentiator


def diff():
    # Begin

    # Print Application Start Notice
    print("Application Started!")

    # Init Differentiator Object
    d = Differentiator()

    # Determine Data Structure
    data_structure = d.determine_data_structure()

    if data_structure == "DataFrame":
        # Init DataFrameDifferentiator Object
        df_d = DataFrameDifferentiator()
        df_d.start()
        df_d.diff_sequences()
        df_d.end()
        # Delete DataFrameDifferentiator Object
        del df_d
    elif data_structure == "RDD":
        # Init ResilientDistributedDatasetDifferentiator Object
        rdd_d = ResilientDistributedDatasetDifferentiator()
        rdd_d.start()
        rdd_d.diff_sequences()
        rdd_d.end()
        # Delete ResilientDistributedDatasetDifferentiator Object
        del rdd_d

    # Delete Differentiator Object
    del d

    # Print Application End Notice
    print("Application Finished Successfully!")

    # End
    exit(0)


if __name__ == "__main__":
    diff()
