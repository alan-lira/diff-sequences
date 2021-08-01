import diffsequences_differentiator.differentiator_exceptions
import sys


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("DiffSequencesAppParameters.dict", number_of_arguments_provided - 1)
        raise diffsequences_differentiator.differentiator_exceptions \
            .InvalidNumberofArgumentsError(invalid_number_of_arguments_message)


def diff() -> None:
    # BEGIN

    # GET NUMBER OF ARGUMENTS PROVIDED
    number_of_arguments_provided = len(sys.argv)

    # VALIDATE NUMBER OF ARGUMENTS PROVIDED
    check_if_is_valid_number_of_arguments(number_of_arguments_provided)

    # END
    sys.exit(0)


if __name__ == "__main__":
    diff()
