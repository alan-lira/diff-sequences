from differentiator.differentiator import Differentiator
from differentiator.differentiator_df import DataFrameDifferentiator
from differentiator.differentiator_rdd import ResilientDistributedDatasetDifferentiator
from pathlib import Path
from sys import argv


def check_if_has_valid_number_of_arguments(argv_list: list) -> None:
    number_of_arguments_expected = 1
    arguments_expected_list = ["differentiator_config_file"]
    number_of_arguments_provided = len(argv_list) - 1
    if number_of_arguments_provided != number_of_arguments_expected:
        number_of_arguments_expected_message = \
            "".join([str(number_of_arguments_expected),
                     " arguments were" if number_of_arguments_expected > 1 else " argument was"])
        number_of_arguments_provided_message = \
            "".join([str(number_of_arguments_provided),
                     " arguments were" if number_of_arguments_provided > 1 else " argument was"])
        invalid_number_of_arguments_message = \
            "Invalid number of arguments provided!\n" \
            "{0} expected: {1}\n" \
            "{2} provided: {3}".format(number_of_arguments_expected_message,
                                       ", ".join(arguments_expected_list),
                                       number_of_arguments_provided_message,
                                       ", ".join(argv_list[1:]))
        raise ValueError(invalid_number_of_arguments_message)


def check_if_file_exists(file_path: Path) -> None:
    if not file_path.is_file():
        file_not_found_message = "'{0}' not found. The application will halt!".format(str(file_path))
        raise FileNotFoundError(file_not_found_message)


def diff(argv_list: list) -> None:
    # Begin
    # Print Application Start Notice
    print("Application Started!")
    # Check if Has Valid Number of Arguments
    check_if_has_valid_number_of_arguments(argv_list)
    # Read Differentiator Config File
    differentiator_config_file = Path(argv_list[1])
    # Check If Differentiator Config File Exists
    check_if_file_exists(differentiator_config_file)
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
