from configparser import ConfigParser
from differentiator.differentiator_df import DifferentiatorDF
from differentiator.differentiator_rdd import DifferentiatorRDD
from differentiator.exception.differentiator_exceptions import InvalidDataStructureError
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


def determine_data_structure(differentiator_config_file: Path) -> str:
    # Init ConfigParser Object
    differentiator_config_parser = ConfigParser()
    # Case Preservation of Each Option Name
    differentiator_config_parser.optionxform = str
    # Load differentiator_config_parser
    differentiator_config_parser.read(differentiator_config_file,
                                      encoding="utf-8")
    # Read Data Structure
    exception_message = "{0}: 'data_structure' must be a string value!" \
        .format(differentiator_config_file)
    try:
        data_structure = str(differentiator_config_parser.get("General Settings",
                                                              "data_structure"))
    except ValueError:
        raise InvalidDataStructureError(exception_message)
    # Validate Data Structure
    supported_data_structures = ["DataFrame", "RDD"]
    exception_message = "Supported Data Structures: {0}" \
        .format(" | ".join(supported_data_structures))
    if data_structure not in supported_data_structures:
        raise InvalidDataStructureError(exception_message)
    # Delete ConfigParser Object
    del differentiator_config_parser
    return data_structure


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
    # Determine Data Structure
    data_structure = determine_data_structure(differentiator_config_file)
    # Diff Sequences
    if data_structure == "DataFrame":
        # Init DifferentiatorDF Object
        d_df = DifferentiatorDF(differentiator_config_file)
        d_df.start()
        d_df.diff_sequences()
        d_df.end()
        # Delete DifferentiatorDF Object
        del d_df
    elif data_structure == "RDD":
        # Init DifferentiatorRDD Object
        d_rdd = DifferentiatorRDD(differentiator_config_file)
        d_rdd.start()
        d_rdd.diff_sequences()
        d_rdd.end()
        # Delete DifferentiatorRDD Object
        del d_rdd
    # Print Application End Notice
    print("Application Finished Successfully!")
    # End
    exit(0)


if __name__ == "__main__":
    diff(argv)
