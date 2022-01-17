from pathlib import Path
from re import search
from sequences_handler.exception.sequences_handler_exceptions import *


class SequencesHandler:

    def __init__(self,
                 sequences_list_text_file: Path) -> None:
        self.sequences_list_text_file = sequences_list_text_file
        self.sequences_list_length = None

    def __get_sequences_list_text_file(self) -> Path:
        return self.sequences_list_text_file

    @staticmethod
    def __validate_sequence_line_format_length(sequences_list_text_file: Path,
                                               sequence_line_format_length: int) -> None:
        exception_message = \
            "Each line of '{0}' must be formatted as follow: {1} " \
            "(e.g., {2}; where [0,L-1] interval stands for entire sequence length)." \
            .format(sequences_list_text_file,
                    "sequence_file, [diff_start_position,diff_end_position]",
                    "sequence.fasta, [0,L-1]")
        if sequence_line_format_length != 2:
            raise InvalidSequenceLineFormatLengthError(exception_message)

    @staticmethod
    def __check_if_sequence_file_exists(sequence_file: Path) -> None:
        exception_message = \
            "'{0}' file not found." \
            .format(sequence_file)
        if not sequence_file.is_file():
            raise FileNotFoundError(exception_message)

    @staticmethod
    def __validate_sequences_line_count(sequences_list_text_file: Path,
                                        sequences_line_count: int) -> None:
        exception_message = \
            "'{0}' must have at least 2 lines (i.e., 2 sequences files)." \
            .format(sequences_list_text_file)
        if sequences_line_count < 2:
            raise InvalidSequencesLineCountError(exception_message)

    def __validate_sequences_list_text_file(self,
                                            sequences_list_text_file: Path) -> None:
        sequences_line_count = 0
        with open(sequences_list_text_file, mode="r", encoding="utf-8") as sequences_list:
            for sequence_line in sequences_list:
                sequence_line_format = sequence_line.strip().split(", ")
                self.__validate_sequence_line_format_length(sequences_list_text_file,
                                                            len(sequence_line_format))
                sequence_file = Path(sequence_line_format[0]).resolve()
                self.__check_if_sequence_file_exists(sequence_file)
                sequences_line_count = sequences_line_count + 1
        self.__validate_sequences_line_count(sequences_list_text_file,
                                             sequences_line_count)

    @staticmethod
    def __get_sequences_list_length(sequences_list_text_file: Path) -> int:
        return sum(1 for _ in open(sequences_list_text_file, mode="r", encoding="utf-8"))

    def get_sequences_list_length(self) -> int:
        # Get Sequences List Text File
        sequences_list_text_file = self.__get_sequences_list_text_file()
        # Validate Sequences List Text File
        self.__validate_sequences_list_text_file(sequences_list_text_file)
        # Get Sequences List Length
        sequences_list_length = self.__get_sequences_list_length(sequences_list_text_file)
        return sequences_list_length

    @staticmethod
    def generate_sequences_indices_list(N: int,
                                        max_DS: int) -> list:
        sequences_indices_list = []
        first_data_structure_sequences_indices_list = []
        second_data_structure_sequences_indices_list = []
        first_data_structure_first_sequence_index = 0
        first_data_structure_last_sequence_index = N - 1
        first_data_structure_sequences_index_range = range(first_data_structure_first_sequence_index,
                                                           first_data_structure_last_sequence_index)
        for first_data_structure_sequence_index in first_data_structure_sequences_index_range:
            second_data_structure_first_sequence_index = first_data_structure_sequence_index + 1
            second_data_structure_last_sequence_index = N
            second_data_structure_last_sequence_added = 0
            while second_data_structure_last_sequence_added != second_data_structure_last_sequence_index - 1:
                first_data_structure_sequences_indices_list.append(first_data_structure_sequence_index)
                sequences_on_second_data_structure_count = 0
                second_data_structure_sequence_index = 0
                for second_data_structure_sequence_index in range(second_data_structure_first_sequence_index,
                                                                  second_data_structure_last_sequence_index):
                    second_data_structure_sequences_indices_list.extend([second_data_structure_sequence_index])
                    sequences_on_second_data_structure_count = sequences_on_second_data_structure_count + 1
                    if sequences_on_second_data_structure_count == max_DS:
                        break
                if len(first_data_structure_sequences_indices_list) > 0 \
                        and len(second_data_structure_sequences_indices_list) > 0:
                    sequences_indices_list.append([first_data_structure_sequences_indices_list,
                                                   second_data_structure_sequences_indices_list])
                    second_data_structure_last_sequence_added = second_data_structure_sequence_index
                    second_data_structure_first_sequence_index = second_data_structure_last_sequence_added + 1
                first_data_structure_sequences_indices_list = []
                second_data_structure_sequences_indices_list = []
        return sequences_indices_list

    @staticmethod
    def __parse_sequence_file(sequence_file: Path,
                              start_position: str,
                              end_position: str) -> list:
        sequence_start_token = ">"
        sequence_identification = "Seq"
        sequence_data = []
        with open(sequence_file, mode="r", encoding="utf-8") as sequence_file:
            line = sequence_file.readline().rstrip()
            if line.startswith(sequence_start_token):
                sequence_identification = line.split("|", 1)[0].replace(sequence_start_token, "").replace(" ", "")
            for line in sequence_file.readlines():
                sequence_data.append(line.rstrip())
        sequence_data = "".join(sequence_data)
        start_position_to_shrink = int(start_position)
        if end_position == "L-1":
            shrunk_sequence = sequence_data[start_position_to_shrink:]
        else:
            end_position_to_shrink = int(end_position)
            shrunk_sequence = sequence_data[start_position_to_shrink:end_position_to_shrink]
        parsed_sequence_file = [sequence_identification, shrunk_sequence]
        return parsed_sequence_file

    def generate_sequences_list(self,
                                sequences_list_text_file: Path,
                                sequences_indices_list: list) -> list:
        sequences_list = []
        with open(sequences_list_text_file, mode="r", encoding="utf-8") as sequences_list_text_file:
            for line_index, sequence_line in enumerate(sequences_list_text_file):
                if line_index in sequences_indices_list:
                    sequence_line = sequence_line.strip().split(", ")
                    sequence_file = Path(sequence_line[0]).resolve()
                    sequence_interval = sequence_line[1]
                    start_position = search(r"\[(.*),", sequence_interval).group(1)
                    end_position = search(r",(.*)]", sequence_interval).group(1)
                    parsed_sequence_file = self.__parse_sequence_file(sequence_file,
                                                                      start_position,
                                                                      end_position)
                    sequences_list.append(parsed_sequence_file)
        return sequences_list
