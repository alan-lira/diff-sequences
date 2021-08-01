from configparser import ConfigParser
from differentiator_exceptions import *
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkContext
from pyspark.sql import SparkSession
import ast
import os
import sys
import time


class DiffSequencesSpark:

    def __init__(self):
        self.spark_session = None
        self.spark_context = None
        self.app_name = None
        self.app_id = None
        self.executors_count = None
        self.executor_memory = None
        self.total_cores_count = None
        self.cores_per_executor = None


class DiffSequencesParameters:

    def __init__(self):
        self.sequences_path_list_text_file = None


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("differentiator_parameters.dict", number_of_arguments_provided - 1)
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


def load_diff_sequences_parameters(dsp: DiffSequencesParameters,
                                   parsed_parameters_dictionary: dict) -> None:
    # READ FASTA SEQUENCES PATH LIST TEXT FILE
    dsp.sequences_path_list_text_file = \
        Path(str(parsed_parameters_dictionary["DiffSequences"]["sequences_path_list_text_file"]))


def get_or_create_spark_session() -> SparkSession:
    return SparkSession \
        .builder \
        .getOrCreate()


def get_spark_context(spark_session: SparkSession) -> SparkContext:
    return spark_session.sparkContext


def check_if_is_running_locally_non_cluster(spark_context: SparkContext) -> bool:
    return spark_context._jsc.sc().isLocal()


def get_spark_app_name(spark_context: SparkContext) -> str:
    is_running_locally_non_cluster = check_if_is_running_locally_non_cluster(spark_context)
    if is_running_locally_non_cluster:
        return "DiffSequences"
    else:
        return spark_context.getConf().get("spark.app.name")


def get_spark_app_id(spark_context: SparkContext) -> str:
    return spark_context.applicationId


def get_spark_cores_max_count(spark_context: SparkContext) -> int:
    is_running_locally_non_cluster = check_if_is_running_locally_non_cluster(spark_context)
    if is_running_locally_non_cluster:
        return int(get_spark_executors_count(spark_context) * get_spark_cores_per_executor(spark_context))
    else:
        return int(spark_context.getConf().get("spark.cores.max"))


def get_spark_executors_list(spark_context: SparkContext) -> list:
    return [executor.host() for executor in spark_context._jsc.sc().statusTracker().getExecutorInfos()]


def get_spark_executors_count(spark_context: SparkContext) -> int:
    return len(get_spark_executors_list(spark_context))


def get_spark_cores_per_executor(spark_context: SparkContext) -> int:
    is_running_locally_non_cluster = check_if_is_running_locally_non_cluster(spark_context)
    if is_running_locally_non_cluster:
        return os.cpu_count()
    else:
        return get_spark_cores_max_count(spark_context) * get_spark_executors_count(spark_context)


def get_spark_executor_memory(spark_context: SparkContext) -> str:
    is_running_locally_non_cluster = check_if_is_running_locally_non_cluster(spark_context)
    if is_running_locally_non_cluster:
        return "1G"
    else:
        return spark_context.getConf().get("spark.executor.memory")


def start_diff_sequences_spark(dss: DiffSequencesSpark,
                               logger: Logger) -> None:
    # GET OR CREATE SPARK SESSION
    create_spark_session_start = time.time()
    dss.spark_session = get_or_create_spark_session()
    create_spark_session_end = time.time()
    create_spark_session_seconds = create_spark_session_end - create_spark_session_start
    create_spark_session_minutes = create_spark_session_seconds / 60
    spark_session_creation_duration_message = "Spark Session Creation Duration: {0} sec (≈ {1} min)" \
        .format(str(round(create_spark_session_seconds, 4)), str(round(create_spark_session_minutes, 4)))
    logger.info(spark_session_creation_duration_message)

    # GET SPARK CONTEXT
    dss.spark_context = get_spark_context(dss.spark_session)

    # GET APP NAME
    dss.app_name = get_spark_app_name(dss.spark_context)

    # GET APP ID
    dss.app_id = get_spark_app_id(dss.spark_context)
    app_id_message = "({0}) Application ID: {1}" \
        .format(dss.app_name, dss.app_id)
    logger.info(app_id_message)

    # GET EXECUTORS COUNT (--num-executors)
    dss.executors_count = get_spark_executors_count(dss.spark_context)
    executors_count_message = "({0}) Executors Count (--num-executors): {1}" \
        .format(dss.app_name, str(dss.executors_count))
    logger.info(executors_count_message)

    # GET EXECUTOR MEMORY (--executor-memory)
    dss.executor_memory = get_spark_executor_memory(dss.spark_context)
    executor_memory_message = "({0}) Executor Memory (--executor-memory): {1}" \
        .format(dss.app_name, dss.executor_memory)
    logger.info(executor_memory_message)

    # GET TOTAL CORES COUNT (--total-executor-cores)
    dss.total_cores_count = get_spark_cores_max_count(dss.spark_context)
    total_cores_count_message = "({0}) Total Cores Count (--total-executor-cores): {1}" \
        .format(dss.app_name, str(dss.total_cores_count))
    logger.info(total_cores_count_message)

    # GET CORES PER EXECUTOR
    dss.cores_per_executor = get_spark_cores_per_executor(dss.spark_context)
    cores_per_executor_message = "({0}) Cores Per Executor: {1}" \
        .format(dss.app_name, str(dss.cores_per_executor))
    logger.info(cores_per_executor_message)


def parse_sequence_file(sequence_file_path: Path,
                        sequence_file_start_position: str,
                        sequence_file_end_position: str) -> list:
    sequence_start_token = ">"
    sequence_identification = "Seq"
    sequence_data = []
    with open(sequence_file_path, mode="r") as sequence_file:
        line = sequence_file.readline().rstrip()
        if line.startswith(sequence_start_token):
            sequence_identification = line.split("|", 1)[0].replace(sequence_start_token, "").replace(" ", "")
        for line in sequence_file.readlines():
            sequence_data.append(line.rstrip())
    sequence_data = "".join(sequence_data)
    split_start_position = int(sequence_file_start_position)
    if sequence_file_end_position == "N":
        sequence_data_splitted = sequence_data[split_start_position:]
    else:
        split_end_position = int(sequence_file_end_position)
        sequence_data_splitted = sequence_data[split_start_position:split_end_position]
    parsed_sequence_file = [sequence_identification, sequence_data_splitted]
    return parsed_sequence_file


def parse_sequences_list(sequences_files_path_list_file_path: Path) -> list:
    parsed_sequences_list = []
    with open(sequences_files_path_list_file_path, mode="r") as sequences_files_path_list_file:
        for sequence_file_path_and_interval in sequences_files_path_list_file:
            sequence_file_path_and_interval = sequence_file_path_and_interval.strip().split(",")
            sequence_file_path = Path(sequence_file_path_and_interval[0])
            sequence_file_start_position = sequence_file_path_and_interval[1]
            sequence_file_end_position = sequence_file_path_and_interval[2]
            parsed_sequence_file = parse_sequence_file(sequence_file_path,
                                                       sequence_file_start_position,
                                                       sequence_file_end_position)
            parsed_sequences_list.append(parsed_sequence_file)
    return parsed_sequences_list


def generate_sequences_list(sequences_path_list_text_file: Path,
                            app_name: str,
                            logger: Logger):
    # GENERATE SEQUENCES LIST
    read_sequences_start = time.time()
    sequences_list = parse_sequences_list(sequences_path_list_text_file)
    read_sequences_end = time.time()
    read_sequences_seconds = read_sequences_end - read_sequences_start
    read_sequences_minutes = read_sequences_seconds / 60
    generate_sequences_list_duration_message = "({0}) Generate Sequences List Duration: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(read_sequences_seconds, 4)),
                str(round(read_sequences_minutes, 4)))
    logger.info(generate_sequences_list_duration_message)
    return sequences_list


def stop_diff_sequences_spark(dss: DiffSequencesSpark,
                              logger: Logger) -> None:
    # STOP SPARK SESSION
    stop_spark_session_start = time.time()
    dss.spark_session.stop()
    stop_spark_session_end = time.time()
    stop_spark_session_seconds = stop_spark_session_end - stop_spark_session_start
    stop_spark_session_minutes = stop_spark_session_seconds / 60
    spark_session_stopping_duration_message = "({0}) Spark Session Stopping Duration: {1} sec (≈ {2} min)" \
        .format(dss.app_name, str(round(stop_spark_session_seconds, 4)), str(round(stop_spark_session_minutes, 4)))
    logger.info(spark_session_stopping_duration_message)


def get_total_elapsed_time(app_name: str,
                           app_start_time: time,
                           app_end_time: time,
                           logger: Logger) -> None:
    # GET TOTAL ELAPSED TIME
    app_seconds = (app_end_time - app_start_time)
    app_minutes = app_seconds / 60
    total_elapsed_time_message = "({0}) Total Elapsed Time: {1} sec (≈ {2} min)" \
        .format(app_name, str(round(app_seconds, 4)), str(round(app_minutes, 4)))
    logger.info(total_elapsed_time_message)


def diff(argv: list) -> None:
    # BEGIN
    app_start_time = time.time()
    print("Application Started!")

    # SET LOG FILE CONFIG
    basicConfig(filename="logging.log",
                format="%(asctime)s %(message)s",
                level=INFO)
    logger = getLogger()

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

    # LOAD DIFF SEQUENCES PARAMETERS
    dsp = DiffSequencesParameters()
    load_diff_sequences_parameters(dsp,
                                   parsed_parameters_dictionary)

    # START DIFF SEQUENCES SPARK
    dss = DiffSequencesSpark()
    start_diff_sequences_spark(dss, logger)

    # GENERATE SEQUENCES LIST
    sequences_list = generate_sequences_list(dsp.sequences_path_list_text_file,
                                             dss.app_name,
                                             logger)

    # STOP DIFF SEQUENCES SPARK
    stop_diff_sequences_spark(dss, logger)

    # END
    app_end_time = time.time()
    get_total_elapsed_time(dss.app_name, app_start_time, app_end_time, logger)
    print("Application Finished Successfully!")
    sys.exit(0)


if __name__ == "__main__":
    diff(sys.argv)
