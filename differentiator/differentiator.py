from abc import abstractmethod
from configparser import ConfigParser
from differentiator.exception.differentiator_exceptions import *
from inspect import stack
from math import inf
from thread_builder.thread_builder import ThreadBuilder
from json import loads
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import RDD, SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from re import split
from time import time, sleep
from typing import Union
from urllib.request import urlopen


class Differentiator:

    def __init__(self) -> None:
        self.differentiator_config_file = Path("config/differentiator.cfg")
        self.differentiator_config_parser = None
        self.app_start_time = None
        self.spark_application_properties = None
        self.logging_directory = None
        self.output_directory = None
        self.sequences_list_text_file = None
        self.data_structure = None
        self.diff_phase = None
        self.max_s = None
        self.collection_phase = None
        self.partitioning = None
        self.spark_conf = None
        self.spark_session = None
        self.spark_context = None
        self.time_to_create_spark_session = None
        self.logger = None
        self.current_number_of_executors = None
        self.current_executors_count_per_host = None
        self.total_number_of_cores_of_the_current_executors = None
        self.total_amount_of_memory_in_bytes_of_the_current_executors = None
        self.converted_total_amount_of_memory_of_the_current_executors = None
        self.best_sequences_comparison_time_in_seconds = None
        self.k_list = None
        self.k_index = None
        self.k_i = None
        self.k_opt_found = None
        self.n = None

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

    def __set_data_structure(self,
                             data_structure: str) -> None:
        self.data_structure = data_structure

    @staticmethod
    def __log_data_structure(data_structure: str,
                             logger: Logger) -> None:
        data_structure_message = "Spark Data Structure: {0}" \
            .format(data_structure)
        print(data_structure_message)
        logger.info(data_structure_message)

    def get_data_structure(self) -> str:
        return self.data_structure

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
    def __log_diff_phase(diff_phase: str,
                         logger: Logger) -> None:
        diff_phase_message = "Diff Phase: {0}" \
            .format(diff_phase)
        print(diff_phase_message)
        logger.info(diff_phase_message)

    def get_diff_phase(self) -> str:
        return self.diff_phase

    @staticmethod
    def read_max_s(differentiator_config_file: Path,
                   differentiator_config_parser: ConfigParser) -> Union[int, str]:
        exception_message = "{0}: 'max_s' must be a integer value in range [1, N-1]!" \
            .format(differentiator_config_file)
        try:
            max_s = str(differentiator_config_parser.get("Diff Sequences Spark Settings",
                                                         "max_s"))
            if max_s != "N-1":
                max_s = int(max_s)
        except ValueError:
            raise InvalidMaxSError(exception_message)
        return max_s

    @staticmethod
    def validate_max_s(max_s: Union[int, str]) -> None:
        exception_message = "Multiple Sequences Data Structures must have at least one sequence."
        if max_s == "N-1":
            pass
        else:
            if max_s < 1:
                raise InvalidMaxSError(exception_message)

    def set_max_s(self,
                  n: int,
                  max_s: Union[int, str]) -> None:
        if max_s == "N-1":
            self.max_s = n - 1
        else:
            self.max_s = max_s

    @staticmethod
    def log_max_s(data_structure: str,
                  max_s: int,
                  logger: Logger) -> None:
        maximum_sequences_per_data_structure_message = "Maximum Sequences Per {0} [maxₛ]: {1}" \
            .format(data_structure,
                    str(max_s))
        print(maximum_sequences_per_data_structure_message)
        logger.info(maximum_sequences_per_data_structure_message)

    def get_max_s(self) -> int:
        return self.max_s

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
    def __log_collection_phase(collection_phase: str,
                               logger: Logger) -> None:
        collection_phase_message = "Collection Phase: {0}" \
            .format(collection_phase)
        print(collection_phase_message)
        logger.info(collection_phase_message)

    def get_collection_phase(self) -> str:
        return self.collection_phase

    @staticmethod
    def __read_partitioning(differentiator_config_file: Path,
                            differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'partitioning' must be a string value!" \
            .format(differentiator_config_file)
        try:
            partitioning = \
                str(differentiator_config_parser.get("Diff Sequences Spark Settings",
                                                     "partitioning"))
        except ValueError:
            raise InvalidPartitioningError(exception_message)
        return partitioning

    @staticmethod
    def __validate_partitioning(partitioning: str) -> None:
        supported_partitioning = ["auto", "adaptive"]
        exception_message = "Supported partitioning: {0}" \
            .format(" | ".join(supported_partitioning))
        if partitioning not in supported_partitioning:
            raise InvalidPartitioningError(exception_message)

    def __set_partitioning(self,
                           partitioning: str) -> None:
        self.partitioning = partitioning

    @staticmethod
    def __log_partitioning(partitioning: str,
                           logger: Logger) -> None:
        partitioning_message = "Partitioning: {0}" \
            .format(partitioning.capitalize())
        print(partitioning_message)
        logger.info(partitioning_message)

    def get_partitioning(self) -> str:
        return self.partitioning

    def __read_validate_and_set_diff_sequences_spark_settings(self,
                                                              differentiator_config_file: Path,
                                                              differentiator_config_parser: ConfigParser) -> None:
        # Data Structure
        data_structure = self.__read_data_structure(differentiator_config_file,
                                                    differentiator_config_parser)
        self.__validate_data_structure(data_structure)
        self.__set_data_structure(data_structure)
        # Diff Phase
        diff_phase = self.__read_diff_phase(differentiator_config_file,
                                            differentiator_config_parser)
        self.__validate_diff_phase(diff_phase)
        self.__set_diff_phase(diff_phase)
        # Maximum Sequences Per Spark Data Structure (maxₛ)
        max_s = self.read_max_s(differentiator_config_file,
                                differentiator_config_parser)
        self.validate_max_s(max_s)
        # Collection Phase
        collection_phase = self.__read_collection_phase(differentiator_config_file,
                                                        differentiator_config_parser)
        self.__validate_collection_phase(collection_phase)
        self.__set_collection_phase(collection_phase)
        # Partitioning
        partitioning = self.__read_partitioning(differentiator_config_file,
                                                differentiator_config_parser)
        self.__validate_partitioning(partitioning)
        self.__set_partitioning(partitioning)

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
    def __get_maximum_number_of_cores_requested(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.cores.max")

    @staticmethod
    def __get_number_of_cores_per_executor_requested(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.executor.cores")

    @staticmethod
    def __get_amount_of_memory_per_executor_requested(spark_context: SparkContext) -> str:
        return spark_context.getConf().get("spark.executor.memory")

    @staticmethod
    def __fetch_current_active_executors_properties_using_spark_rest_api(spark_context: SparkContext) -> list:
        spark_ui_web_url = spark_context.uiWebUrl
        spark_application_id = spark_context.applicationId
        active_executors_url = spark_ui_web_url + "/api/v1/applications/" + spark_application_id + "/executors"
        current_active_executors_properties = []
        current_number_of_executors = 0
        current_executors_count_per_host = dict()
        total_number_of_cores_of_the_current_executors = 0
        total_amount_of_memory_in_bytes_of_the_current_executors = 0
        tolerance_time_in_seconds = 15
        start_time = time()
        while current_number_of_executors == 0:
            if time() - start_time >= tolerance_time_in_seconds:  # Insufficient Resources on Cluster (Much Probably)
                break
            with urlopen(active_executors_url) as active_executors_response:
                active_executors_data = loads(active_executors_response.read().decode("utf-8"))
                for executor in active_executors_data:
                    if executor["id"] != "driver" and executor["isActive"] \
                            and executor["totalCores"] > 0 and executor["maxMemory"] > 0:
                        # Number of Executors
                        current_number_of_executors = current_number_of_executors + 1
                        # Executors Count per Host (Worker)
                        executor_host = executor["hostPort"].partition(":")[0]
                        if executor_host in current_executors_count_per_host:
                            current_executors_count_per_host[executor_host] = \
                                current_executors_count_per_host[executor_host] + 1
                        else:
                            current_executors_count_per_host[executor_host] = 1
                        # Number of Cores Available in This Executor
                        total_number_of_cores_of_the_current_executors = \
                            total_number_of_cores_of_the_current_executors + int(executor["totalCores"])
                        # Block Manager Size (Total Amount of Memory Available for Storage in This Executor) in Bytes
                        # Also Known as Heap Space Size
                        total_amount_of_memory_in_bytes_of_the_current_executors = \
                            total_amount_of_memory_in_bytes_of_the_current_executors + int(executor["maxMemory"])
            sleep(1)
        current_active_executors_properties.append(current_number_of_executors)
        current_active_executors_properties.append(current_executors_count_per_host)
        current_active_executors_properties.append(total_number_of_cores_of_the_current_executors)
        current_active_executors_properties.append(total_amount_of_memory_in_bytes_of_the_current_executors)
        return current_active_executors_properties

    def __set_current_number_of_executors(self,
                                          current_number_of_executors: int) -> None:
        self.current_number_of_executors = current_number_of_executors

    def get_current_number_of_executors(self) -> int:
        return self.current_number_of_executors

    def __set_current_executors_count_per_host(self,
                                               current_executors_count_per_host: dict) -> None:
        self.current_executors_count_per_host = current_executors_count_per_host

    def get_current_executors_count_per_host(self) -> dict:
        return self.current_executors_count_per_host

    def __set_total_number_of_cores_of_the_current_executors(self,
                                                             total_number_of_cores: int) -> None:
        self.total_number_of_cores_of_the_current_executors = total_number_of_cores

    def get_total_number_of_cores_of_the_current_executors(self) -> int:
        return self.total_number_of_cores_of_the_current_executors

    def __set_total_amount_of_memory_in_bytes_of_the_current_executors(self,
                                                                       total_amount_of_memory_in_bytes: int) -> None:
        self.total_amount_of_memory_in_bytes_of_the_current_executors = total_amount_of_memory_in_bytes

    def get_total_amount_of_memory_in_bytes_of_the_current_executors(self) -> int:
        return self.total_amount_of_memory_in_bytes_of_the_current_executors

    def __convert_total_amount_of_memory(self,
                                         spark_context: SparkContext,
                                         total_amount_of_memory_in_bytes_of_the_current_executors: int) -> str:
        amount_of_memory_per_executor_requested = self.__get_amount_of_memory_per_executor_requested(spark_context)
        converted_total_amount_of_memory = 0
        memory_size_suffix = "".join(filter(lambda x: x.isalpha(), amount_of_memory_per_executor_requested)).upper()
        if memory_size_suffix == "B":  # Convert to Byte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors
        if memory_size_suffix == "K":  # Convert to Kibibyte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors / 1024
        if memory_size_suffix == "M":  # Convert to Mebibyte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors / 1.049e+6
        if memory_size_suffix == "G":  # Convert to Gibibyte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors / 1.074e+9
        if memory_size_suffix == "T":  # Convert to Tebibyte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors / 1.1e+12
        if memory_size_suffix == "P":  # Convert to Pebibyte
            converted_total_amount_of_memory = \
                total_amount_of_memory_in_bytes_of_the_current_executors / 1.126e+15
        return str(round(converted_total_amount_of_memory, 2)) + " " + memory_size_suffix + "iB"

    def __set_converted_total_amount_of_memory_of_the_current_executors(self,
                                                                        converted_total_amount_of_memory: str) -> None:
        self.converted_total_amount_of_memory_of_the_current_executors = converted_total_amount_of_memory

    def get_converted_total_amount_of_memory_of_the_current_executors(self) -> str:
        return self.converted_total_amount_of_memory_of_the_current_executors

    def __fetch_set_and_log_current_active_executors_properties(self,
                                                                spark_context: SparkContext,
                                                                fetch_stage: str,
                                                                logger: Logger) -> None:
        # Fetch Current Active Executors Properties Using Spark REST API
        current_active_executors_properties = \
            self.__fetch_current_active_executors_properties_using_spark_rest_api(spark_context)
        # Set Current Number of Executors
        current_number_of_executors = current_active_executors_properties[0]
        self.__set_current_number_of_executors(current_number_of_executors)
        # Set Current Executors Count per Host (Worker)
        current_executors_count_per_host = current_active_executors_properties[1]
        self.__set_current_executors_count_per_host(current_executors_count_per_host)
        # Set Total Number of Cores of the Current Executors
        current_total_number_of_cores = current_active_executors_properties[2]
        self.__set_total_number_of_cores_of_the_current_executors(current_total_number_of_cores)
        # Set Total Amount of Memory in Bytes (Heap Space Fraction) of the Current Executors
        current_total_amount_of_memory_in_bytes = current_active_executors_properties[3]
        self.__set_total_amount_of_memory_in_bytes_of_the_current_executors(current_total_amount_of_memory_in_bytes)
        # Convert Total Amount of Memory (Heap Space Fraction) of the Current Executors
        converted_total_amount_of_memory = \
            self.__convert_total_amount_of_memory(spark_context,
                                                  current_total_amount_of_memory_in_bytes)
        # Set Converted Total Amount of Memory (Heap Space Fraction) of the Current Executors
        self.__set_converted_total_amount_of_memory_of_the_current_executors(converted_total_amount_of_memory)
        # Log Current Active Executors Properties
        number_of_executors = \
            "".join([str(current_number_of_executors),
                     " Executors" if current_number_of_executors > 1 else " Executor"])
        total_number_of_cores = \
            "".join([str(current_total_number_of_cores),
                     " Cores" if current_total_number_of_cores > 1 else " Core"])
        current_executors_count_per_host_formatted = \
            "; ".join([str(current_executors_count_per_host[k]) + " @ " + k for k in current_executors_count_per_host])
        current_active_executors_with_hosts_count_message = \
            "{0} Active Executors: {1} ({2})" \
            .format(fetch_stage,
                    number_of_executors,
                    current_executors_count_per_host_formatted)
        print(current_active_executors_with_hosts_count_message)
        logger.info(current_active_executors_with_hosts_count_message)
        current_total_available_resources_message = \
            "{0} Total Available Resources: {1} and {2} RAM (Heap Space Fraction)" \
            .format(fetch_stage,
                    total_number_of_cores,
                    converted_total_amount_of_memory)
        print(current_total_available_resources_message)
        logger.info(current_total_available_resources_message)

    def __set_best_sequences_comparison_time_in_seconds(self,
                                                        best_sequences_comparison_time_in_seconds: time) -> None:
        self.best_sequences_comparison_time_in_seconds = best_sequences_comparison_time_in_seconds

    def __get_best_sequences_comparison_time_in_seconds(self) -> time:
        return self.best_sequences_comparison_time_in_seconds

    def __set_k_list(self,
                     k_list: list) -> None:
        self.k_list = k_list

    def __get_k_list(self) -> list:
        return self.k_list

    def __set_k_index(self,
                      k_index: int) -> None:
        self.k_index = k_index

    def __get_k_index(self) -> int:
        return self.k_index

    def __set_k_i(self,
                  k_i: int) -> None:
        self.k_i = k_i

    def get_k_i(self) -> int:
        return self.k_i

    def __set_k_opt_found(self,
                          k_opt_found: bool) -> None:
        self.k_opt_found = k_opt_found

    def __get_k_opt_found(self) -> bool:
        return self.k_opt_found

    def __set_k_opt_variables(self,
                              k_i_stage: str,
                              logger: Logger) -> None:
        # Initialize Variables Used to Find 'k_opt' (Local Optimal 'k_i' that Minimizes the Sequences Comparison Time)
        best_sequences_comparison_time_in_seconds = inf
        self.__set_best_sequences_comparison_time_in_seconds(best_sequences_comparison_time_in_seconds)
        k_index = 0
        self.__set_k_index(k_index)
        k_opt_found = False
        self.__set_k_opt_found(k_opt_found)
        # Get Number of Available Map Cores (Equals to Total Number of Cores of the Current Executors)
        number_of_available_map_cores = self.get_total_number_of_cores_of_the_current_executors()
        # Find K Set (Set of All Divisors of the Number of Available Map Cores)
        k = self.find_divisors_set(number_of_available_map_cores)
        # Generate List from K Set (Ordered K)
        k_list = sorted(k)
        # Set 'k_list'
        self.__set_k_list(k_list)
        # Set 'k_0' (Initial 'k_i' of 'k_list')
        if 0 <= k_index <= len(k_list) - 1:
            k_i = k_list[k_index]
        else:
            k_i = 1
        self.__set_k_i(k_i)
        # Log 'k_0'
        self.log_k(k_i,
                   k_i_stage,
                   logger)

    def __fetch_set_and_log_current_active_executors_properties_with_interval(self,
                                                                              interval_in_minutes: int,
                                                                              spark_context: SparkContext,
                                                                              fetch_stage: str,
                                                                              logger: Logger) -> None:
        interval_count = 0
        while True:
            start = time()
            while True:
                end = (time() - start) / 60
                if end >= interval_in_minutes:
                    interval_count = interval_count + 1
                    break
                sleep(1)
            # Fetch, Set and Log Current Active Executors Properties (Update)
            self.__fetch_set_and_log_current_active_executors_properties(spark_context,
                                                                         fetch_stage,
                                                                         logger)
            # Get Partitioning
            partitioning = self.get_partitioning()
            # Reinitialize (Reset) Variables Used to Find 'k_opt'
            if partitioning == "adaptive":
                self.__set_k_opt_variables("Reset",
                                           logger)
            ordinal_number_suffix = self.__get_ordinal_number_suffix(interval_count)
            executors_thread_message = \
                "Executors Thread: Fetched and Updated Active Executors Properties... ({0}{1} time)" \
                .format(str(interval_count),
                        ordinal_number_suffix)
            print(executors_thread_message)
            logger.info(executors_thread_message)

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
    def __log_time_to_create_spark_session(time_to_create_spark_session_in_seconds: time,
                                           logger: Logger) -> None:
        time_to_create_spark_session_message = "Time to Create Spark Session: {0} sec (≈ {1} min)" \
            .format(str(round(time_to_create_spark_session_in_seconds, 4)),
                    str(round((time_to_create_spark_session_in_seconds / 60), 4)))
        logger.info(time_to_create_spark_session_message)

    @staticmethod
    def __log_spark_application_properties(spark_app_name: str,
                                           spark_app_id: str,
                                           maximum_number_of_cores_requested: str,
                                           number_of_cores_per_executor_requested: str,
                                           amount_of_memory_per_executor_requested: str,
                                           logger: Logger) -> None:
        spark_app_name_message = "Application Name: {0}" \
            .format(spark_app_name)
        logger.info(spark_app_name_message)
        spark_app_id_message = "Application ID: {0}" \
            .format(spark_app_id)
        logger.info(spark_app_id_message)
        maximum_number_of_cores_requested_message = \
            "Maximum Number of Cores (vCPUs) Requested (if it's possible to fulfill): {0}" \
            .format(maximum_number_of_cores_requested)
        logger.info(maximum_number_of_cores_requested_message)
        number_of_cores_per_executor_requested_message = \
            "Number of Cores (vCPUs) per Executor Requested: {0}" \
            .format(number_of_cores_per_executor_requested)
        logger.info(number_of_cores_per_executor_requested_message)
        split_memory_size_and_suffix = \
            [c for c in split(r"([-+]?\d*\.\d+|\d+)", amount_of_memory_per_executor_requested) if c]
        amount_of_memory_per_executor_requested_message = \
            "Amount of Memory per Executor Requested: {0}" \
            .format(" ".join(split_memory_size_and_suffix) + "iB RAM")
        logger.info(amount_of_memory_per_executor_requested_message)

    @staticmethod
    def __get_ordinal_number_suffix(number: int) -> str:
        number_to_str = str(number)
        if number_to_str.endswith("1"):
            return "st"
        elif number_to_str.endswith("2"):
            return "nd"
        elif number_to_str.endswith("3"):
            return "rd"
        else:
            return "th"

    def __log_application_duration_time_with_interval(self,
                                                      interval_in_minutes: int,
                                                      logger: Logger) -> None:
        interval_count = 0
        while True:
            start = time()
            while True:
                end = (time() - start) / 60
                if end >= interval_in_minutes:
                    interval_count = interval_count + 1
                    break
                sleep(1)
            ordinal_number_suffix = self.__get_ordinal_number_suffix(interval_count)
            number_of_minutes_passed = "".join([str(interval_in_minutes),
                                                " Minutes" if interval_in_minutes > 1 else " Minute"])
            app_duration_time_thread_message = "Application Duration Time Thread: {0} Have Passed... ({1}{2} time)" \
                .format(number_of_minutes_passed,
                        str(interval_count),
                        ordinal_number_suffix)
            print(app_duration_time_thread_message)
            logger.info(app_duration_time_thread_message)

    def set_n(self,
              n: int) -> None:
        self.n = n

    @staticmethod
    def log_n(n: int,
              logger: Logger) -> None:
        number_of_sequences_to_diff_message = "Number of Unique Input Sequences [N]: {0}" \
            .format(str(n))
        print(number_of_sequences_to_diff_message)
        logger.info(number_of_sequences_to_diff_message)

    def get_n(self) -> int:
        return self.n

    @staticmethod
    def estimate_total_number_of_diffs(diff_phase: str,
                                       n: int,
                                       max_s: int) -> int:
        estimate_total_number_of_diffs = 0
        if diff_phase == "1":
            estimate_total_number_of_diffs = int((n * (n - 1)) / 2)
        elif diff_phase == "opt":
            if 1 <= max_s < (n / 2):
                estimate_total_number_of_diffs = int(((n * (n - 1)) / max_s) - ((n * (n - max_s)) / (2 * max_s)))
            elif (n / 2) <= max_s < n:
                estimate_total_number_of_diffs = int(2 * (n - 1) - max_s)
        return estimate_total_number_of_diffs

    @staticmethod
    def log_estimated_total_number_of_diffs(estimated_total_number_of_diffs: int,
                                            logger: Logger) -> None:
        estimated_total_number_of_diffs_message = \
            "Estimation of the Total Number of Diffs to be Performed [Estimated Dₐ]: {0}" \
            .format(str(estimated_total_number_of_diffs))
        print(estimated_total_number_of_diffs_message)
        logger.info(estimated_total_number_of_diffs_message)

    @staticmethod
    def get_actual_total_number_of_diffs(sequences_indices_list: list) -> int:
        return len(sequences_indices_list)

    @staticmethod
    def log_actual_total_number_of_diffs(actual_total_number_of_diffs: int,
                                         logger: Logger) -> None:
        actual_total_number_of_diffs_message = \
            "Total Number of Diffs to be Performed [Dₐ]: {0}" \
            .format(str(actual_total_number_of_diffs))
        print(actual_total_number_of_diffs_message)
        logger.info(actual_total_number_of_diffs_message)

    @staticmethod
    def calculate_absolute_error_of_total_number_of_diffs_estimation(estimated_total_number_of_diffs: int,
                                                                     actual_total_number_of_diffs: int) -> int:
        return abs(actual_total_number_of_diffs - estimated_total_number_of_diffs)

    @staticmethod
    def calculate_percent_error_of_total_number_of_diffs_estimation(estimated_total_number_of_diffs: int,
                                                                    actual_total_number_of_diffs: int) -> float:
        return (abs(actual_total_number_of_diffs - estimated_total_number_of_diffs)
                / abs(estimated_total_number_of_diffs)) * 100

    @staticmethod
    def log_total_number_of_diffs_estimation_errors(absolute_error_of_total_number_of_diffs_estimation: int,
                                                    percent_error_of_total_number_of_diffs_estimation: float,
                                                    logger: Logger) -> None:
        d_a_estimation_absolute_error_message = \
            "Absolute Error of Dₐ Estimation: {0} ({1}%)" \
            .format(str(absolute_error_of_total_number_of_diffs_estimation),
                    str(round(percent_error_of_total_number_of_diffs_estimation, 4)))
        print(d_a_estimation_absolute_error_message)
        logger.info(d_a_estimation_absolute_error_message)

    @staticmethod
    def find_divisors_set(divisible_number: int) -> set:
        k = set()
        for i in range(1, divisible_number + 1):
            if divisible_number % i == 0:
                k.add(i)
        return k

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
    def repartition_data_structure(data_structure: Union[RDD, DataFrame],
                                   new_number_of_partitions: int) -> Union[RDD, DataFrame]:
        current_dataframe_num_partitions = 0
        if type(data_structure) == RDD:
            current_dataframe_num_partitions = data_structure.getNumPartitions()
        elif type(data_structure) == DataFrame:
            current_dataframe_num_partitions = data_structure.rdd.getNumPartitions()
        if current_dataframe_num_partitions > new_number_of_partitions:
            # Execute Coalesce (Spark Less-Wide-Shuffle Transformation) Function
            data_structure = data_structure.coalesce(new_number_of_partitions)
        if current_dataframe_num_partitions < new_number_of_partitions:
            # Execute Repartition (Spark Wider-Shuffle Transformation) Function
            data_structure = data_structure.repartition(new_number_of_partitions)
        return data_structure

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

    def find_and_log_k_opt_using_adaptive_partitioning(self,
                                                       time_to_compare_sequences_in_seconds: time,
                                                       logger: Logger) -> None:
        k_opt_found = self.__get_k_opt_found()
        if not k_opt_found:
            best_sequences_comparison_time_in_seconds = self.__get_best_sequences_comparison_time_in_seconds()
            k_list = self.__get_k_list()
            k_index = self.__get_k_index()
            if best_sequences_comparison_time_in_seconds >= time_to_compare_sequences_in_seconds:
                self.__set_best_sequences_comparison_time_in_seconds(time_to_compare_sequences_in_seconds)
                self.__set_k_index(k_index + 1)
                k_index = self.__get_k_index()
                if 0 <= k_index <= len(k_list) - 1:
                    k_i = k_list[k_index]
                    self.__set_k_i(k_i)
                    # Log 'k_i'
                    self.log_k(k_i,
                               "Updated",
                               logger)
                else:
                    self.__set_k_opt_found(True)
                    k_i = self.get_k_i()
                    # Log 'k_i' = 'k_opt'
                    self.log_k(k_i,
                               "Optimal",
                               logger)
            else:
                self.__set_k_index(k_index - 1)
                k_index = self.__get_k_index()
                k_i = k_list[k_index]
                self.__set_k_i(k_i)
                self.__set_k_opt_found(True)
                # Log 'k_i' = 'k_opt'
                self.log_k(k_i,
                           "Optimal",
                           logger)

    @staticmethod
    def log_k(k_i: int,
              k_i_stage: str,
              logger: Logger) -> None:
        k_i_message = ""
        if k_i_stage == "Initial":
            k_i_message = "Initial K (Divisor of Number of Available Map Cores): k₀ = "
        elif k_i_stage == "Updated":
            k_i_message = "Updated K: kᵢ = "
        elif k_i_stage == "Optimal":
            k_i_message = "Optimal K: kₒₚₜ = "
        elif k_i_stage == "Reset":
            k_i_message = "Reset K: k₀ = "
        logger.info(k_i_message+str(k_i))

    @staticmethod
    def log_time_to_compare_sequences(first_data_structure_first_sequence_index: int,
                                      second_data_structure_first_sequence_index: int,
                                      second_data_structure_last_sequence_index: int,
                                      time_to_compare_sequences_in_seconds: time,
                                      current_number_of_executors: int,
                                      total_number_of_cores_of_the_current_executors: int,
                                      converted_total_amount_of_memory_of_the_current_executors: str,
                                      logger: Logger) -> None:
        number_of_executors = \
            "".join([str(current_number_of_executors),
                     " Executors" if current_number_of_executors > 1 else " Executor"])
        total_number_of_cores = \
            "".join([str(total_number_of_cores_of_the_current_executors),
                     " Cores" if total_number_of_cores_of_the_current_executors > 1 else " Core"])
        if second_data_structure_first_sequence_index != second_data_structure_last_sequence_index:
            time_to_compare_sequences_message = \
                "Sequence {0} X Sequences {{{1}, …, {2}}} " \
                "Comparison Time: {3} sec (≈ {4} min) " \
                "[{5}, {6} and {7} Heap Space RAM]" \
                .format(str(first_data_structure_first_sequence_index),
                        str(second_data_structure_first_sequence_index),
                        str(second_data_structure_last_sequence_index),
                        str(round(time_to_compare_sequences_in_seconds, 4)),
                        str(round((time_to_compare_sequences_in_seconds / 60), 4)),
                        number_of_executors,
                        total_number_of_cores,
                        converted_total_amount_of_memory_of_the_current_executors)
        else:
            time_to_compare_sequences_message = \
                "Sequence {0} X Sequence {1} " \
                "Comparison Time: {2} sec (≈ {3} min) " \
                "[{4}, {5} and {6} Heap Space RAM]" \
                .format(str(first_data_structure_first_sequence_index),
                        str(second_data_structure_last_sequence_index),
                        str(round(time_to_compare_sequences_in_seconds, 4)),
                        str(round((time_to_compare_sequences_in_seconds / 60), 4)),
                        number_of_executors,
                        total_number_of_cores,
                        converted_total_amount_of_memory_of_the_current_executors)
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
        real_time_metrics_message = "Number of Sequences Comparisons (Diffs) Done: {1} ({2} Left) | " \
                                    "Sequences Comparisons Average Time: {3} sec (≈ {4} min) | " \
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
    def log_sequences_comparisons_average_time(data_structure: str,
                                               sequences_comparisons_average_time_in_seconds: time,
                                               logger: Logger) -> None:
        sequences_comparisons_average_time_message = \
            "Sequences Comparisons Average Time ({0}s → Create, Diff & Collection): {1} sec (≈ {2} min)" \
            .format(data_structure,
                    str(round(sequences_comparisons_average_time_in_seconds, 4)),
                    str(round((sequences_comparisons_average_time_in_seconds / 60), 4)))
        logger.info(sequences_comparisons_average_time_message)

    @staticmethod
    def log_tasks_count(phase: str,
                        tasks_count: int,
                        logger: Logger) -> None:
        partitions_count_message = "Total Number of Tasks Processed in {0} Phase: {1}" \
            .format(phase,
                    str(tasks_count))
        logger.info(partitions_count_message)

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
        self.__log_time_to_create_spark_session(time_to_create_spark_session_in_seconds,
                                                logger)
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Spark App Id
        spark_app_id = self.get_spark_app_id(spark_context)
        # Get Maximum Number of Cores Requested (If it's Possible to Fulfill)
        maximum_number_of_cores_requested = self.__get_maximum_number_of_cores_requested(spark_context)
        # Get Number of Cores per Executor Requested
        number_of_cores_per_executor_requested = self.__get_number_of_cores_per_executor_requested(spark_context)
        # Get Amount of Memory per Executor Requested
        amount_of_memory_per_executor_requested = self.__get_amount_of_memory_per_executor_requested(spark_context)
        # Log Spark Application Properties
        self.__log_spark_application_properties(spark_app_name,
                                                spark_app_id,
                                                maximum_number_of_cores_requested,
                                                number_of_cores_per_executor_requested,
                                                amount_of_memory_per_executor_requested,
                                                logger)
        # Get Data Structure
        data_structure = self.get_data_structure()
        # Log Data Structure
        self.__log_data_structure(data_structure,
                                  logger)
        # Get Diff Phase
        diff_phase = self.get_diff_phase()
        # Log Diff Phase
        self.__log_diff_phase(diff_phase,
                              logger)
        # Get Collection Phase
        collection_phase = self.get_collection_phase()
        # Log Collection Phase
        self.__log_collection_phase(collection_phase,
                                    logger)
        # Get Partitioning
        partitioning = self.get_partitioning()
        # Log Partitioning
        self.__log_partitioning(partitioning,
                                logger)
        # Log Application Duration Time With Interval
        tb_app_duration_time_interval_in_minutes = 15
        tb_app_duration_time_target_method = self.__log_application_duration_time_with_interval
        tb_app_duration_time_target_method_arguments = (tb_app_duration_time_interval_in_minutes,
                                                        logger)
        tb_app_duration_time_daemon_mode = True
        tb_app_duration_time = ThreadBuilder(tb_app_duration_time_target_method,
                                             tb_app_duration_time_target_method_arguments,
                                             tb_app_duration_time_daemon_mode)
        tb_app_duration_time.start()
        # Fetch, Set and Log Current Active Executors Properties (Initial)
        self.__fetch_set_and_log_current_active_executors_properties(spark_context,
                                                                     "Initial",
                                                                     logger)
        # Initialize Variables Used to Find 'k_opt'
        if partitioning == "adaptive":
            self.__set_k_opt_variables("Initial",
                                       logger)
        # Fetch, Set and Log Current Active Executors Properties With Interval (Updates)
        tb_current_active_executors_interval_in_minutes = 5
        tb_current_active_executors_target_method = \
            self.__fetch_set_and_log_current_active_executors_properties_with_interval
        tb_current_active_executors_target_method_arguments = (tb_current_active_executors_interval_in_minutes,
                                                               spark_context,
                                                               "Updated",
                                                               logger)
        tb_current_active_executors_daemon_mode = True
        tb_current_active_executors = ThreadBuilder(tb_current_active_executors_target_method,
                                                    tb_current_active_executors_target_method_arguments,
                                                    tb_current_active_executors_daemon_mode)
        tb_current_active_executors.start()

    @staticmethod
    def __stop_spark_session(spark_session: SparkSession) -> None:
        spark_session.stop()

    @staticmethod
    def __log_time_to_stop_spark_session(time_to_stop_spark_session_in_seconds: time,
                                         logger: Logger) -> None:
        time_to_stop_spark_session_message = "Time to Stop Spark Session: {0} sec (≈ {1} min)" \
            .format(str(round(time_to_stop_spark_session_in_seconds, 4)),
                    str(round((time_to_stop_spark_session_in_seconds / 60), 4)))
        logger.info(time_to_stop_spark_session_message)

    @staticmethod
    def __log_time_to_finish_application(time_to_finish_application_in_seconds: time,
                                         logger: Logger) -> None:
        time_to_finish_application_message = "Time to Finish Application: {0} sec (≈ {1} min)" \
            .format(str(round(time_to_finish_application_in_seconds, 4)),
                    str(round((time_to_finish_application_in_seconds / 60), 4)))
        logger.info(time_to_finish_application_message)

    def end(self) -> None:
        # Get SparkContext
        spark_context = self.get_spark_context()
        # Get Logger
        logger = self.get_logger()
        # Fetch, Set and Log Current Active Executors Properties (Final)
        self.__fetch_set_and_log_current_active_executors_properties(spark_context,
                                                                     "Final",
                                                                     logger)
        # Stop SparkSession Start Time
        stop_spark_session_start_time = time()
        # Get SparkSession
        spark_session = self.get_spark_session()
        # Stop SparkSession
        self.__stop_spark_session(spark_session)
        # Time to Stop SparkSession in Seconds
        time_to_stop_spark_session_in_seconds = time() - stop_spark_session_start_time
        # Log Time to Stop SparkSession
        self.__log_time_to_stop_spark_session(time_to_stop_spark_session_in_seconds,
                                              logger)
        # Time to Finish Application in Seconds
        time_to_finish_application_in_seconds = time() - self.__get_app_start_time()
        # Log Time to Finish Application
        self.__log_time_to_finish_application(time_to_finish_application_in_seconds,
                                              logger)

    @abstractmethod
    def diff_sequences(self) -> None:
        raise NotImplementedError("'{0}' function is not implemented yet!".format(stack()[0].function))
