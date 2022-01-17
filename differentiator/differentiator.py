from abc import abstractmethod
from configparser import ConfigParser
from differentiator.exception.differentiator_exceptions import *
from inspect import stack
from interval_timer.interval_timer import IntervalTimer
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from sequences_handler.sequences_handler import SequencesHandler
from time import time


class Differentiator:

    def __init__(self) -> None:
        self.differentiator_config_file = Path("config/differentiator.cfg")
        self.differentiator_config_parser = None
        self.app_start_time = None
        self.spark_application_properties = None
        self.logging_directory = None
        self.output_directory = None
        self.sequences_list_text_file = None
        self.diff_phase = None
        self.collection_phase = None
        self.data_structure = None
        self.spark_conf = None
        self.spark_session = None
        self.spark_context = None
        self.time_to_create_spark_session = None
        self.logger = None
        self.N = None

    def get_differentiator_config_file(self) -> Path:
        return self.differentiator_config_file

    def __set_app_start_time(self,
                             app_start_time: time) -> None:
        self.app_start_time = app_start_time

    def __get_app_start_time(self) -> time:
        return self.app_start_time

    def __set_differentiator_config_parser(self,
                                           differentiator_config_parser: ConfigParser) -> None:
        self.differentiator_config_parser = differentiator_config_parser

    def __set_differentiator_config_parser_case_preservation(self) -> None:
        self.differentiator_config_parser.optionxform = str

    def __load_differentiator_config_parser(self,
                                            differentiator_config_file: Path,
                                            encoding: str) -> None:
        self.differentiator_config_parser.read(differentiator_config_file,
                                               encoding=encoding)

    def __set_and_load_differentiator_config_parser(self,
                                                    differentiator_config_file: Path,
                                                    differentiator_config_parser: ConfigParser) -> None:
        # Set differentiator_config_parser
        self.__set_differentiator_config_parser(differentiator_config_parser)
        # Case Preservation of Each Option Name
        self.__set_differentiator_config_parser_case_preservation()
        # Load differentiator_config_parser
        self.__load_differentiator_config_parser(differentiator_config_file,
                                                 "utf-8")

    @staticmethod
    def __read_spark_application_properties(differentiator_config_file: Path,
                                            differentiator_config_parser: ConfigParser) -> list:
        exception_message = "{0}: '[Spark Application Properties (SparkConf)]' section must have key/value pairs!" \
            .format(differentiator_config_file)
        try:
            spark_application_properties = \
                list(differentiator_config_parser.items("Spark Application Properties (SparkConf)"))
        except ValueError:
            raise InvalidSparkApplicationPropertiesError(exception_message)
        return spark_application_properties

    def __set_spark_application_properties(self,
                                           spark_application_properties: list) -> None:
        self.spark_application_properties = spark_application_properties

    def __get_spark_application_properties(self) -> list:
        return self.spark_application_properties

    def __read_and_set_spark_application_properties(self,
                                                    differentiator_config_file: Path,
                                                    differentiator_config_parser: ConfigParser) -> None:
        # Spark Application Properties (SparkConf)
        spark_application_properties = self.__read_spark_application_properties(differentiator_config_file,
                                                                                differentiator_config_parser)
        self.__set_spark_application_properties(spark_application_properties)

    @staticmethod
    def __read_logging_directory(differentiator_config_file: Path,
                                 differentiator_config_parser: ConfigParser) -> Path:
        exception_message = "{0}: 'logging_directory' must be a valid path!" \
            .format(differentiator_config_file)
        try:
            logging_directory = Path(differentiator_config_parser.get("General Settings",
                                                                      "logging_directory"))
        except ValueError:
            raise InvalidPathError(exception_message)
        return logging_directory

    @staticmethod
    def __validate_logging_directory(logging_directory: Path) -> None:
        if not logging_directory.exists():
            logging_directory.mkdir()

    def __set_logging_directory(self,
                                logging_directory: Path) -> None:
        self.logging_directory = logging_directory

    def __get_logging_directory(self) -> Path:
        return self.logging_directory

    @staticmethod
    def __read_output_directory(differentiator_config_file: Path,
                                differentiator_config_parser: ConfigParser) -> Path:
        exception_message = "{0}: 'output_directory' must be a valid path!" \
            .format(differentiator_config_file)
        try:
            output_directory = Path(differentiator_config_parser.get("General Settings",
                                                                     "output_directory"))
        except ValueError:
            raise InvalidPathError(exception_message)
        return output_directory

    @staticmethod
    def __validate_output_directory(output_directory: Path) -> None:
        if not output_directory.exists():
            output_directory.mkdir()

    def __set_output_directory(self,
                               output_directory: Path) -> None:
        self.output_directory = output_directory

    def get_output_directory(self) -> Path:
        return self.output_directory

    def __read_validate_and_set_general_settings(self,
                                                 differentiator_config_file: Path,
                                                 differentiator_config_parser: ConfigParser) -> None:
        # Logging Directory
        logging_directory = self.__read_logging_directory(differentiator_config_file,
                                                          differentiator_config_parser)
        self.__validate_logging_directory(logging_directory)
        self.__set_logging_directory(logging_directory)
        # Output Directory
        output_directory = self.__read_output_directory(differentiator_config_file,
                                                        differentiator_config_parser)
        self.__validate_output_directory(output_directory)
        self.__set_output_directory(output_directory)

    @staticmethod
    def __read_sequences_list_text_file(differentiator_config_file: Path,
                                        differentiator_config_parser: ConfigParser) -> Path:
        exception_message = "{0}: 'sequences_list_text_file' must be a valid path file!" \
            .format(differentiator_config_file)
        try:
            sequences_list_text_file = Path(differentiator_config_parser.get("Input Settings",
                                                                             "sequences_list_text_file"))
        except ValueError:
            raise InvalidPathError(exception_message)
        return sequences_list_text_file

    def __set_sequences_list_text_file(self,
                                       sequences_list_text_file: Path) -> None:
        self.sequences_list_text_file = sequences_list_text_file

    def get_sequences_list_text_file(self) -> Path:
        return self.sequences_list_text_file

    def __read_and_set_input_settings(self,
                                      differentiator_config_file: Path,
                                      differentiator_config_parser: ConfigParser) -> None:
        # Sequences List Text File
        sequences_list_text_file = self.__read_sequences_list_text_file(differentiator_config_file,
                                                                        differentiator_config_parser)
        self.__set_sequences_list_text_file(sequences_list_text_file)

    @staticmethod
    def __read_diff_phase(differentiator_config_file: Path,
                          differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'diff_phase' must be a string value!" \
            .format(differentiator_config_file)
        try:
            diff_phase = str(differentiator_config_parser.get("Diff Sequences Spark Settings",
                                                              "diff_phase"))
        except ValueError:
            raise InvalidDiffPhaseError(exception_message)
        return diff_phase

    @staticmethod
    def __validate_diff_phase(diff_phase: str) -> None:
        supported_diff_phases = ["1", "opt"]
        exception_message = "Supported Diff Phases: {0}" \
            .format(" | ".join(supported_diff_phases))
        if diff_phase not in supported_diff_phases:
            raise InvalidDiffPhaseError(exception_message)

    def __set_diff_phase(self,
                         diff_phase: str) -> None:
        self.diff_phase = diff_phase

    @staticmethod
    def __log_diff_phase(spark_app_name: str,
                         diff_phase: str,
                         logger: Logger) -> None:
        diff_phase_message = "({0}) Diff Phase: {1}" \
            .format(spark_app_name,
                    diff_phase)
        print(diff_phase_message)
        logger.info(diff_phase_message)

    def get_diff_phase(self) -> str:
        return self.diff_phase

    @staticmethod
    def __read_collection_phase(differentiator_config_file: Path,
                                differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'collection_phase' must be a string value!" \
            .format(differentiator_config_file)
        try:
            collection_phase = str(differentiator_config_parser.get("Diff Sequences Spark Settings",
                                                                    "collection_phase"))
        except ValueError:
            raise InvalidCollectionPhaseError(exception_message)
        return collection_phase

    @staticmethod
    def __validate_collection_phase(collection_phase: str) -> None:
        supported_collection_phases = ["None", "SC", "DW", "MW"]
        exception_message = "Supported Collection Phases: {0}" \
            .format(" | ".join(supported_collection_phases))
        if collection_phase not in supported_collection_phases:
            raise InvalidCollectionPhaseError(exception_message)

    def __set_collection_phase(self,
                               collection_phase: str) -> None:
        self.collection_phase = collection_phase

    @staticmethod
    def __log_collection_phase(spark_app_name: str,
                               collection_phase: str,
                               logger: Logger) -> None:
        collection_phase_message = "({0}) Collection Phase: {1}" \
            .format(spark_app_name,
                    collection_phase)
        print(collection_phase_message)
        logger.info(collection_phase_message)

    def get_collection_phase(self) -> str:
        return self.collection_phase

    @staticmethod
    def __read_data_structure(differentiator_config_file: Path,
                              differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'data_structure' must be a string value!" \
            .format(differentiator_config_file)
        try:
            data_structure = str(differentiator_config_parser.get("Diff Sequences Spark Settings",
                                                                  "data_structure"))
        except ValueError:
            raise InvalidDataStructureError(exception_message)
        return data_structure

    @staticmethod
    def __validate_data_structure(data_structure: str) -> None:
        supported_data_structures = ["DataFrame", "RDD"]
        exception_message = "Supported Data Structures: {0}" \
            .format(" | ".join(supported_data_structures))
        if data_structure not in supported_data_structures:
            raise InvalidDataStructureError(exception_message)

    def __set_data_structure(self,
                             data_structure: str) -> None:
        self.data_structure = data_structure

    @staticmethod
    def __log_data_structure(spark_app_name: str,
                             data_structure: str,
                             logger: Logger) -> None:
        data_structure_message = "({0}) Spark Data Structure: {1}" \
            .format(spark_app_name,
                    data_structure)
        print(data_structure_message)
        logger.info(data_structure_message)

    def get_data_structure(self) -> str:
        return self.data_structure

    def determine_data_structure(self) -> str:
        # Get Differentiator Config File
        differentiator_config_file = self.get_differentiator_config_file()
        # Init ConfigParser Object
        config_parser = ConfigParser()
        # Case Preservation of Each Option Name
        config_parser.optionxform = str
        # Load config_parser
        config_parser.read(differentiator_config_file,
                           encoding="utf-8")
        # Data Structure
        data_structure = self.__read_data_structure(differentiator_config_file,
                                                    config_parser)
        self.__validate_data_structure(data_structure)
        # Delete ConfigParser Object
        del config_parser
        return data_structure

    def __read_validate_and_set_diff_sequences_spark_settings(self,
                                                              differentiator_config_file: Path,
                                                              differentiator_config_parser: ConfigParser) -> None:
        # Diff Phase
        diff_phase = self.__read_diff_phase(differentiator_config_file,
                                            differentiator_config_parser)
        self.__validate_diff_phase(diff_phase)
        self.__set_diff_phase(diff_phase)
        # Collection Phase
        collection_phase = self.__read_collection_phase(differentiator_config_file,
                                                        differentiator_config_parser)
        self.__validate_collection_phase(collection_phase)
        self.__set_collection_phase(collection_phase)
        # Data Structure
        data_structure = self.__read_data_structure(differentiator_config_file,
                                                    differentiator_config_parser)
        self.__validate_data_structure(data_structure)
        self.__set_data_structure(data_structure)

    @staticmethod
    def __create_spark_conf(spark_application_properties: list) -> SparkConf:
        spark_conf = SparkConf()
        for key, value in spark_application_properties:
            spark_conf.set(key, value)
        return spark_conf

    def __set_spark_conf(self,
                         spark_conf: SparkConf) -> None:
        self.spark_conf = spark_conf

    def __get_spark_conf(self) -> SparkConf:
        return self.spark_conf

    @staticmethod
    def __get_or_create_spark_session(spark_conf: SparkConf) -> SparkSession:
        spark_session = \
            SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()
        return spark_session

    def __set_spark_session(self,
                            spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_session(self) -> SparkSession:
        return self.spark_session

    @staticmethod
    def __get_spark_context_from_spark_session(spark_session: SparkSession) -> SparkContext:
        spark_context = spark_session.sparkContext
        return spark_context

    def __set_spark_context(self,
                            spark_context: SparkContext) -> None:
        self.spark_context = spark_context

    def get_spark_context(self) -> SparkContext:
        return self.spark_context

    def __set_time_to_create_spark_session(self,
                                           time_to_create_spark_session: time) -> None:
        self.time_to_create_spark_session = time_to_create_spark_session

    def __get_time_to_create_spark_session(self) -> time:
        return self.time_to_create_spark_session

    def __init_spark_environment(self,
                                 spark_application_properties: list) -> None:
        # Create SparkSession Start Time
        create_spark_session_start_time = time()
        # Create SparkConf
        spark_conf = self.__create_spark_conf(spark_application_properties)
        # Set SparkConf
        self.__set_spark_conf(spark_conf)
        # Get or Create SparkSession
        spark_session = self.__get_or_create_spark_session(spark_conf)
        # Set SparkSession
        self.__set_spark_session(spark_session)
        # Get SparkContext
        spark_context = self.__get_spark_context_from_spark_session(spark_session)
        # Set SparkContext
        self.__set_spark_context(spark_context)
        # Time to Create SparkSession in Seconds
        time_to_create_spark_session_in_seconds = time() - create_spark_session_start_time
        # Set Time to Create SparkSession in Seconds
        self.__set_time_to_create_spark_session(time_to_create_spark_session_in_seconds)

    @staticmethod
    def get_spark_app_name(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.app.name")

    @staticmethod
    def get_spark_app_id(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.app.id")

    @staticmethod
    def __get_spark_app_executors_count(spark_context: SparkContext) -> int:
        return int(spark_context.getConf().get("spark.executor.instances"))

    @staticmethod
    def get_spark_app_cores_max_count(spark_context: SparkContext) -> int:
        return int(spark_context.getConf().get("spark.cores.max"))

    @staticmethod
    def __get_spark_app_cores_per_executor(spark_app_executors_count: int,
                                           spark_app_cores_max_count: int) -> int:
        return int(spark_app_cores_max_count / spark_app_executors_count)

    @staticmethod
    def __get_spark_app_executor_memory(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.executor.memory")

    @staticmethod
    def get_spark_recommended_tasks_per_cpu() -> int:
        return 3

    def __set_logger_with_basic_config(self,
                                       logging_directory: Path,
                                       spark_app_name: str,
                                       spark_app_id: str) -> None:
        app_name_path = logging_directory.joinpath(spark_app_name)
        if not app_name_path.exists():
            app_name_path.mkdir()
        app_id_path = app_name_path.joinpath(spark_app_id)
        if not app_id_path.exists():
            app_id_path.mkdir()
        logger_file_name = "{0}/{1}/logger_output.log" \
            .format(spark_app_name,
                    spark_app_id)
        basicConfig(filename=logging_directory.joinpath(logger_file_name),
                    format="%(asctime)s %(message)s",
                    level=INFO)
        self.logger = getLogger()

    def get_logger(self) -> Logger:
        return self.logger

    @staticmethod
    def __log_time_to_create_spark_session(spark_app_name: str,
                                           time_to_create_spark_session_in_seconds: time,
                                           logger: Logger) -> None:
        time_to_create_spark_session_message = "({0}) Time to Create Spark Session: {1} sec (≈ {2} min)" \
            .format(spark_app_name,
                    str(round(time_to_create_spark_session_in_seconds, 4)),
                    str(round((time_to_create_spark_session_in_seconds / 60), 4)))
        logger.info(time_to_create_spark_session_message)

    @staticmethod
    def __log_spark_application_properties(spark_app_name: str,
                                           spark_app_id: str,
                                           spark_app_executors_count: int,
                                           spark_app_cores_max_count: int,
                                           spark_app_cores_per_executor: int,
                                           spark_app_executor_memory: str,
                                           logger: Logger) -> None:
        spark_app_id_message = "({0}) Application ID: {1}" \
            .format(spark_app_name,
                    spark_app_id)
        logger.info(spark_app_id_message)
        spark_executors_count_message = "({0}) Spark Executors Count: {1}" \
            .format(spark_app_name,
                    str(spark_app_executors_count))
        logger.info(spark_executors_count_message)
        spark_executors_cores_count_message = "({0}) Spark Executors Cores Count: {1}" \
            .format(spark_app_name,
                    str(spark_app_cores_max_count))
        logger.info(spark_executors_cores_count_message)
        cores_per_spark_executor_message = "({0}) Cores per Spark Executor: {1}" \
            .format(spark_app_name,
                    str(spark_app_cores_per_executor))
        logger.info(cores_per_spark_executor_message)
        spark_executor_memory_message = "({0}) Memory per Spark Executor: {1}" \
            .format(spark_app_name,
                    spark_app_executor_memory)
        logger.info(spark_executor_memory_message)

    @staticmethod
    def __init_differentiator_interval_timer(spark_app_name: str,
                                             logger: Logger) -> None:
        it = IntervalTimer(spark_app_name,
                           logger)
        it.start()

    def __set_N(self,
                N: int) -> None:
        self.N = N

    @staticmethod
    def __log_N(spark_app_name: str,
                N: int,
                logger: Logger) -> None:
        number_of_sequences_to_diff_message = "({0}) Number of Sequences to Compare (N): {1}" \
            .format(spark_app_name,
                    str(N))
        print(number_of_sequences_to_diff_message)
        logger.info(number_of_sequences_to_diff_message)

    def get_N(self) -> int:
        return self.N

    @staticmethod
    def estimate_amount_of_diffs(diff_phase: str,
                                 N: int,
                                 max_DS: int) -> int:
        estimated_d_a = 0
        if diff_phase == "1":
            estimated_d_a = int((N * (N - 1)) / 2)
        elif diff_phase == "opt":
            if 1 <= max_DS < (N / 2):
                estimated_d_a = int(((N * (N - 1)) / max_DS) - ((N * (N - max_DS)) / (2 * max_DS)))
            elif (N / 2) <= max_DS < N:
                estimated_d_a = int(2 * (N - 1) - max_DS)
        return estimated_d_a

    @staticmethod
    def log_estimated_amount_of_diffs(spark_app_name: str,
                                      estimate_amount_of_diffs: int,
                                      logger: Logger) -> None:
        estimated_d_a_message = \
            "({0}) Estimated Amount of Diffs (d_a): {1}" \
            .format(spark_app_name,
                    str(estimate_amount_of_diffs))
        print(estimated_d_a_message)
        logger.info(estimated_d_a_message)

    @staticmethod
    def get_actual_amount_of_diffs(sequences_indices_list: list) -> int:
        return len(sequences_indices_list)

    @staticmethod
    def log_actual_amount_of_diffs(spark_app_name: str,
                                   actual_d_a: int,
                                   logger: Logger) -> None:
        actual_d_a_message = \
            "({0}) Actual Amount of Diffs (d_a): {1}" \
            .format(spark_app_name,
                    str(actual_d_a))
        print(actual_d_a_message)
        logger.info(actual_d_a_message)

    @staticmethod
    def calculate_amount_of_diffs_estimation_absolute_error(estimated_d_a: int,
                                                            actual_d_a: int) -> int:
        return abs(actual_d_a - estimated_d_a)

    @staticmethod
    def calculate_amount_of_diffs_estimation_percent_error(estimated_d_a: int,
                                                           actual_d_a: int) -> float:
        return (abs(actual_d_a - estimated_d_a) / abs(estimated_d_a)) * 100

    @staticmethod
    def log_d_a_estimation_errors(spark_app_name: str,
                                  d_a_estimation_absolute_error: int,
                                  d_a_estimation_percent_error: float,
                                  logger: Logger) -> None:
        d_a_estimation_absolute_error_message = \
            "({0}) Amount of Diffs (d_a) Estimation Absolute Error: {1} ({2}%)" \
            .format(spark_app_name,
                    str(d_a_estimation_absolute_error),
                    str(round(d_a_estimation_percent_error, 4)))
        print(d_a_estimation_absolute_error_message)
        logger.info(d_a_estimation_absolute_error_message)

    @staticmethod
    def get_biggest_sequence_length_among_data_structures(first_data_structure_sequences_data_list: list,
                                                          second_data_structure_sequences_data_list: list) -> int:
        biggest_sequence_length_among_data_structures = 0
        for index_first_data_structure_sequences in range(len(first_data_structure_sequences_data_list)):
            first_data_structure_sequence_data = \
                first_data_structure_sequences_data_list[index_first_data_structure_sequences][1]
            first_data_structure_sequence_data_length = len(first_data_structure_sequence_data)
            if biggest_sequence_length_among_data_structures < first_data_structure_sequence_data_length:
                biggest_sequence_length_among_data_structures = first_data_structure_sequence_data_length
        for index_second_data_structure_sequences in range(len(second_data_structure_sequences_data_list)):
            second_data_structure_sequence_data = \
                second_data_structure_sequences_data_list[index_second_data_structure_sequences][1]
            second_data_structure_sequence_data_length = len(second_data_structure_sequence_data)
            if biggest_sequence_length_among_data_structures < second_data_structure_sequence_data_length:
                biggest_sequence_length_among_data_structures = second_data_structure_sequence_data_length
        return biggest_sequence_length_among_data_structures

    @staticmethod
    def get_data_structure_data(data_structure_length: int,
                                data_structure_sequences_data_list: list) -> list:
        data_structure_data_list = []
        data_structure_data_aux_list = []
        for index_data_structure_length in range(data_structure_length):
            data_structure_data_aux_list.append(index_data_structure_length)
            for index_data_structure_sequences_data_list in range(len(data_structure_sequences_data_list)):
                sequence = data_structure_sequences_data_list[index_data_structure_sequences_data_list][1]
                nucleotide_letter = None
                try:
                    nucleotide_letter = sequence[index_data_structure_length]
                except IndexError:
                    # Length of the Biggest Sequence Among Data Structures > Length of This Data Structure's Sequence
                    pass
                data_structure_data_aux_list.append(nucleotide_letter)
            data_structure_data_list.append(data_structure_data_aux_list)
            data_structure_data_aux_list = []
        return data_structure_data_list

    @staticmethod
    def get_collection_phase_destination_file_path(output_directory: Path,
                                                   spark_app_name: str,
                                                   spark_app_id: str,
                                                   first_data_structure_first_sequence_index: int,
                                                   second_data_structure_first_sequence_index: int,
                                                   second_data_structure_last_sequence_index: int) -> Path:
        if second_data_structure_first_sequence_index != second_data_structure_last_sequence_index:
            destination_file_path = Path("{0}/{1}/{2}/sequence_{3}_diff_sequences_{4}_to_{5}"
                                         .format(output_directory,
                                                 spark_app_name,
                                                 spark_app_id,
                                                 str(first_data_structure_first_sequence_index),
                                                 str(second_data_structure_first_sequence_index),
                                                 str(second_data_structure_last_sequence_index)))
        else:
            destination_file_path = Path("{0}/{1}/{2}/sequence_{3}_diff_sequence_{4}"
                                         .format(output_directory,
                                                 spark_app_name,
                                                 spark_app_id,
                                                 str(first_data_structure_first_sequence_index),
                                                 str(second_data_structure_last_sequence_index)))
        return destination_file_path

    @staticmethod
    def log_time_to_compare_sequences(spark_app_name: str,
                                      first_data_structure_first_sequence_index: int,
                                      second_data_structure_first_sequence_index: int,
                                      second_data_structure_last_sequence_index: int,
                                      data_structure: str,
                                      time_to_compare_sequences_in_seconds: time,
                                      logger: Logger) -> None:
        if second_data_structure_first_sequence_index != second_data_structure_last_sequence_index:
            time_to_compare_sequences_message = "({0}) Sequence {1} X Sequences [{2}, ..., {3}] " \
                                                "Comparison Time ({4}s → Create, Diff & Collection): " \
                                                "{5} sec (≈ {6} min)" \
                .format(spark_app_name,
                        str(first_data_structure_first_sequence_index),
                        str(second_data_structure_first_sequence_index),
                        str(second_data_structure_last_sequence_index),
                        data_structure,
                        str(round(time_to_compare_sequences_in_seconds, 4)),
                        str(round((time_to_compare_sequences_in_seconds / 60), 4)))
        else:
            time_to_compare_sequences_message = "({0}) Sequence {1} X Sequence {2} " \
                                                "Comparison Time ({3}s → Create, Diff & Collection): " \
                                                "{4} sec (≈ {5} min)" \
                .format(spark_app_name,
                        str(first_data_structure_first_sequence_index),
                        str(second_data_structure_last_sequence_index),
                        data_structure,
                        str(round(time_to_compare_sequences_in_seconds, 4)),
                        str(round((time_to_compare_sequences_in_seconds / 60), 4)))
        print(time_to_compare_sequences_message)
        logger.info(time_to_compare_sequences_message)

    @staticmethod
    def get_number_of_sequences_comparisons_left(actual_d_a: int,
                                                 sequences_comparisons_count: int) -> int:
        return actual_d_a - sequences_comparisons_count

    @staticmethod
    def get_average_sequences_comparison_time(sequences_comparisons_time_seconds: time,
                                              sequences_comparisons_count: int) -> time:
        return sequences_comparisons_time_seconds / sequences_comparisons_count

    @staticmethod
    def estimate_time_left(number_of_sequences_comparisons_left: int,
                           average_sequences_comparison_time_seconds: time) -> time:
        return number_of_sequences_comparisons_left * average_sequences_comparison_time_seconds

    @staticmethod
    def print_real_time_metrics(spark_app_name: str,
                                sequences_comparisons_count: int,
                                number_of_sequences_comparisons_left: int,
                                average_sequences_comparison_time_in_seconds: time,
                                estimated_time_left_in_seconds: time) -> None:
        real_time_metrics_message = "({0}) Sequences Comparisons Done: {1} ({2} Left) | " \
                                    "Average Sequences Comparison Time: {3} sec (≈ {4} min) | " \
                                    "Estimated Time Left: {5} sec (≈ {6} min)" \
            .format(spark_app_name,
                    str(sequences_comparisons_count),
                    str(number_of_sequences_comparisons_left),
                    str(round(average_sequences_comparison_time_in_seconds, 4)),
                    str(round((average_sequences_comparison_time_in_seconds / 60), 4)),
                    str(round(estimated_time_left_in_seconds, 4)),
                    str(round((estimated_time_left_in_seconds / 60), 4)))
        print(real_time_metrics_message)

    @staticmethod
    def log_average_sequences_comparison_time(spark_app_name: str,
                                              data_structure: str,
                                              average_sequences_comparison_time_in_seconds: time,
                                              logger: Logger) -> None:
        average_sequences_comparison_time_message = \
            "({0}) Average Sequences Comparison Time ({1}s → Create, Diff & Collection): {2} sec (≈ {3} min)" \
            .format(spark_app_name,
                    data_structure,
                    str(round(average_sequences_comparison_time_in_seconds, 4)),
                    str(round((average_sequences_comparison_time_in_seconds / 60), 4)))
        logger.info(average_sequences_comparison_time_message)

    @staticmethod
    def log_sequences_comparisons_count(spark_app_name: str,
                                        sequences_comparisons_count: int,
                                        logger: Logger) -> None:
        sequences_comparisons_count_message = "({0}) Sequences Comparisons Count: {1}" \
            .format(spark_app_name,
                    str(sequences_comparisons_count))
        logger.info(sequences_comparisons_count_message)

    @staticmethod
    def log_diff_phases_time(spark_app_name: str,
                             diff_phase: str,
                             diff_phases_time_in_seconds: time,
                             logger: Logger) -> None:
        diff_phases_time_message = \
            "({0}) Diff Phase {1} Time: {2} sec (≈ {3} min)" \
            .format(spark_app_name,
                    diff_phase,
                    str(round(diff_phases_time_in_seconds, 4)),
                    str(round((diff_phases_time_in_seconds / 60), 4)))
        logger.info(diff_phases_time_message)

    @staticmethod
    def log_collection_phases_time(spark_app_name: str,
                                   collection_phase: str,
                                   collection_phases_time_in_seconds: time,
                                   logger: Logger) -> None:
        collection_phase_description = None
        if collection_phase == "None":
            pass
        elif collection_phase == "SC":
            collection_phase_description = \
                "Collection Phase SC Time (Transformation → Sort | Action → Show)"
        elif collection_phase == "DW":
            collection_phase_description = \
                "Collection Phase DW Time (Transformation → Sort | Action → Write)"
        elif collection_phase == "MW":
            collection_phase_description = \
                "Collection Phase MW Time (Transformations → Coalesce & Sort | Action → Write)"
        if collection_phase_description:
            collection_phases_time_message = "({0}) {1}: {2} sec (≈ {3} min)" \
                .format(spark_app_name,
                        collection_phase_description,
                        str(round(collection_phases_time_in_seconds, 4)),
                        str(round((collection_phases_time_in_seconds / 60), 4)))
            logger.info(collection_phases_time_message)

    @staticmethod
    def log_spark_data_structure_partitions_count(spark_app_name: str,
                                                  data_structure: str,
                                                  spark_data_structure_partitions_count: int,
                                                  logger: Logger) -> None:
        spark_data_structure_partitions_count_message = "({0}) Spark {1} Partitions Count: {2}" \
            .format(spark_app_name,
                    data_structure,
                    str(spark_data_structure_partitions_count))
        logger.info(spark_data_structure_partitions_count_message)

    def start(self) -> None:
        # Set Application Start Time
        app_start_time = time()
        self.__set_app_start_time(app_start_time)
        # Set and Load differentiator_config_parser
        differentiator_config_parser = ConfigParser()
        self.__set_and_load_differentiator_config_parser(self.differentiator_config_file,
                                                         differentiator_config_parser)
        # Read, Validate and Set Spark Application Properties (SparkConf)
        self.__read_and_set_spark_application_properties(self.differentiator_config_file,
                                                         self.differentiator_config_parser)
        # Read, Validate and Set General Settings
        self.__read_validate_and_set_general_settings(self.differentiator_config_file,
                                                      self.differentiator_config_parser)
        # Read and Set Input Settings
        self.__read_and_set_input_settings(self.differentiator_config_file,
                                           self.differentiator_config_parser)
        # Read, Validate and Set Diff Sequences Spark Settings
        self.__read_validate_and_set_diff_sequences_spark_settings(self.differentiator_config_file,
                                                                   self.differentiator_config_parser)
        # Get Spark Application Properties
        spark_application_properties = self.__get_spark_application_properties()
        # Init Spark Environment
        self.__init_spark_environment(spark_application_properties)
        # Get SparkContext
        spark_context = self.get_spark_context()
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Spark App Id
        spark_app_id = self.get_spark_app_id(spark_context)
        # Get Logging Directory
        logging_directory = self.__get_logging_directory()
        # Set Logger with Basic Config (basicConfig)
        self.__set_logger_with_basic_config(logging_directory,
                                            spark_app_name,
                                            spark_app_id)
        # Get Logger
        logger = self.get_logger()
        # Get Time to Create SparkSession in Seconds
        time_to_create_spark_session_in_seconds = self.__get_time_to_create_spark_session()
        # Log Time to Create SparkSession
        self.__log_time_to_create_spark_session(spark_app_name,
                                                time_to_create_spark_session_in_seconds,
                                                logger)
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Spark App Id
        spark_app_id = self.get_spark_app_id(spark_context)
        # Get Spark App Executors Count
        spark_app_executors_count = self.__get_spark_app_executors_count(spark_context)
        # Get Spark App Cores Max Count
        spark_app_cores_max_count = self.get_spark_app_cores_max_count(spark_context)
        # Get Spark App Cores per Executor
        spark_app_cores_per_executor = self.__get_spark_app_cores_per_executor(spark_app_executors_count,
                                                                               spark_app_cores_max_count)
        # Get Spark App Executor Memory
        spark_app_executor_memory = self.__get_spark_app_executor_memory(spark_context)
        # Log Spark Application Properties
        self.__log_spark_application_properties(spark_app_name,
                                                spark_app_id,
                                                spark_app_executors_count,
                                                spark_app_cores_max_count,
                                                spark_app_cores_per_executor,
                                                spark_app_executor_memory,
                                                logger)
        # Get Diff Phase
        diff_phase = self.get_diff_phase()
        # Log Diff Phase
        self.__log_diff_phase(spark_app_name,
                              diff_phase,
                              logger)
        # Get Collection Phase
        collection_phase = self.get_collection_phase()
        # Log Collection Phase
        self.__log_collection_phase(spark_app_name,
                                    collection_phase,
                                    logger)
        # Get Data Structure
        data_structure = self.get_data_structure()
        # Log Data Structure
        self.__log_data_structure(spark_app_name,
                                  data_structure,
                                  logger)
        # Get Sequences List Text File
        sequences_list_text_file = self.get_sequences_list_text_file()
        # Init SequencesHandler Object
        sh = SequencesHandler(sequences_list_text_file)
        # Get Sequences List Length
        sequences_list_length = sh.get_sequences_list_length()
        # Delete SequencesHandler Object
        del sh
        # Set Number of Sequences to Compare (N)
        self.__set_N(sequences_list_length)
        # Get Number of Sequences to Compare (N)
        N = self.get_N()
        # Log Number of Sequences to Compare (N)
        self.__log_N(spark_app_name,
                     N,
                     logger)
        # Init Differentiator Interval Timer
        self.__init_differentiator_interval_timer(spark_app_name,
                                                  logger)

    @staticmethod
    def __stop_spark_session(spark_session: SparkSession) -> None:
        spark_session.stop()

    @staticmethod
    def __log_time_to_stop_spark_session(spark_app_name: str,
                                         time_to_stop_spark_session_in_seconds: time,
                                         logger: Logger) -> None:
        time_to_stop_spark_session_message = "({0}) Time to Stop Spark Session: {1} sec (≈ {2} min)" \
            .format(spark_app_name,
                    str(round(time_to_stop_spark_session_in_seconds, 4)),
                    str(round((time_to_stop_spark_session_in_seconds / 60), 4)))
        logger.info(time_to_stop_spark_session_message)

    @staticmethod
    def __log_time_to_finish_application(spark_app_name: str,
                                         time_to_finish_application_in_seconds: time,
                                         logger: Logger) -> None:
        time_to_finish_application_message = "({0}) Time to Finish Application: {1} sec (≈ {2} min)" \
            .format(spark_app_name,
                    str(round(time_to_finish_application_in_seconds, 4)),
                    str(round((time_to_finish_application_in_seconds / 60), 4)))
        logger.info(time_to_finish_application_message)

    def end(self) -> None:
        # Stop SparkSession Start Time
        stop_spark_session_start_time = time()
        # Get SparkSession
        spark_session = self.get_spark_session()
        # Stop SparkSession
        self.__stop_spark_session(spark_session)
        # Time to Stop SparkSession in Seconds
        time_to_stop_spark_session_in_seconds = time() - stop_spark_session_start_time
        # Get SparkContext
        spark_context = self.get_spark_context()
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Logger
        logger = self.get_logger()
        # Log Time to Stop SparkSession
        self.__log_time_to_stop_spark_session(spark_app_name,
                                              time_to_stop_spark_session_in_seconds,
                                              logger)
        # Time to Finish Application in Seconds
        time_to_finish_application_in_seconds = time() - self.__get_app_start_time()
        # Log Time to Finish Application
        self.__log_time_to_finish_application(spark_app_name,
                                              time_to_finish_application_in_seconds,
                                              logger)

    @abstractmethod
    def diff_sequences(self) -> None:
        raise NotImplementedError("'{0}' function is not implemented yet!".format(stack()[0].function))
