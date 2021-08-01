from configparser import ConfigParser
from differentiator_exceptions import *
from pathlib import Path
import ast
import sys


class DiffSequencesParameters:

    def __init__(self):
        self.sequences_path_list_text_file = None


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("DiffSequencesParameters.dict", number_of_arguments_provided - 1)
        raise InvalidNumberofArgumentsError(invalid_number_of_arguments_message)


def read_parameters_dictionary_file(parameters_dictionary_file_path: Path) -> dict:
    with open(parameters_dictionary_file_path, mode="r") as dictionary_file:
        dictionary_file_content = dictionary_file.read()
    return ast.literal_eval(dictionary_file_content)


def check_if_is_valid_dictionary(parameters_dictionary: dict) -> None:
    if not isinstance(parameters_dictionary, dict):
        invalid_dict_message = "Invalid Dictionary Provided!"
        raise InvalidDictionaryError(invalid_dict_message)


def parse_parameters_dictionary(parameters_dictionary: dict) -> ConfigParser:
    config_parser = ConfigParser()
    config_parser.read_dict(parameters_dictionary)
    return config_parser


def load_parameters(dsp: DiffSequencesParameters,
                    parsed_parameters_dictionary: dict) -> None:
    # READ FASTA SEQUENCES PATH LIST TEXT FILE
    dsp.sequences_path_list_text_file = \
        Path(str(parsed_parameters_dictionary["DiffSequences"]["sequences_path_list_text_file"]))


def diff(argv: list) -> None:
    # BEGIN

    # GET NUMBER OF ARGUMENTS PROVIDED
    number_of_arguments_provided = len(argv)

    # VALIDATE NUMBER OF ARGUMENTS PROVIDED
    check_if_is_valid_number_of_arguments(number_of_arguments_provided)

    # GET PARAMETERS DICTIONARY FILE PATH
    parameters_dictionary_file_path = Path(argv[1])

    # READ PARAMETERS DICTIONARY FILE
    parameters_dictionary = read_parameters_dictionary_file(parameters_dictionary_file_path)

    # VALIDATE PARAMETERS DICTIONARY
    check_if_is_valid_dictionary(parameters_dictionary)

    # PARSE PARAMETERS DICTIONARY
    parsed_parameters_dictionary = parse_parameters_dictionary(parameters_dictionary)

    dsp = DiffSequencesParameters()

    # LOAD PARAMETERS FROM PARSED PARAMETERS DICTIONARY
    load_parameters(dsp, parsed_parameters_dictionary)

    # END
    sys.exit(0)


if __name__ == "__main__":
    diff(sys.argv)
