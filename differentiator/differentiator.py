from configparser import ConfigParser
from differentiator_exceptions import *
from differentiator_job_metrics import get_spark_app_numerical_metrics_list
from distutils.util import strtobool
from functools import reduce
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType, StringType, StructType
import ast
import sys
import threading
import time


class DiffSequencesSpark:

    def __init__(self) -> None:
        self.spark_conf = None
        self.spark_session = None
        self.spark_context = None
        self.app_name = None
        self.app_id = None
        self.ui_port = None
        self.spark_executors_count = None
        self.spark_executors_cores_count = None
        self.cores_per_spark_executor = None
        self.memory_per_spark_executor = None


class DiffSequencesParameters:

    def __init__(self) -> None:
        self.logging_directory_path = None
        self.results_directory_path = None
        self.metrics_directory_path = None
        self.sequences_list_text_file_path = None
        self.diff_phase_version = None
        self.max_d = None
        self.enable_customized_partitioning = None
        self.collection_phase_version = None
        self.n = None


class DataFrameStruct:

    def __init__(self,
                 dataframe: DataFrame,
                 schema: StructType,
                 column_names: list,
                 num_rows: int) -> None:
        self.dataframe = dataframe
        self.schema = schema
        self.column_names = column_names
        self.num_rows = num_rows


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("differentiator.dict",
                    number_of_arguments_provided - 1)
        raise InvalidNumberOfArgumentsError(invalid_number_of_arguments_message)


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
    config_parser.optionxform = str
    config_parser.read_dict(parameters_dictionary)
    return config_parser


def load_diff_sequences_parameters(dsp: DiffSequencesParameters,
                                   parsed_parameters_dictionary: dict) -> None:
    # READ LOGGING DIRECTORY PATH
    dsp.logging_directory_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["logging_directory_path"]))

    # READ RESULTS DIRECTORY PATH
    dsp.results_directory_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["results_directory_path"]))

    # READ METRICS DIRECTORY PATH
    dsp.metrics_directory_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["metrics_directory_path"]))

    # READ FASTA SEQUENCES PATH LIST TEXT FILE PATH
    dsp.sequences_list_text_file_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["sequences_list_text_file_path"]))

    # READ DIFF PHASE VERSION
    dsp.diff_phase_version = str(parsed_parameters_dictionary["DiffSequencesParameters"]["diff_phase_version"])

    # READ MAX SEQUENCES PER DATAFRAME (max_d)
    dsp.max_d = \
        str(parsed_parameters_dictionary["DiffSequencesParameters"]["max_d"])

    # READ ENABLE CUSTOMIZED PARTITIONING BOOLEAN
    dsp.enable_customized_partitioning = \
        str(parsed_parameters_dictionary["DiffSequencesParameters"]["enable_customized_partitioning"])

    # READ COLLECTION PHASE VERSION
    dsp.collection_phase_version = \
        str(parsed_parameters_dictionary["DiffSequencesParameters"]["collection_phase_version"])


def validate_logging_directory_path(logging_directory_path: Path) -> None:
    if not logging_directory_path.exists():
        logging_directory_path.mkdir()


def validate_results_directory_path(results_directory_path: Path) -> None:
    if not results_directory_path.exists():
        results_directory_path.mkdir()


def validate_metrics_directory_path(metrics_directory_path: Path) -> None:
    if not metrics_directory_path.exists():
        metrics_directory_path.mkdir()


def validate_sequence_file_path_and_interval_list_length(sequence_file_path_and_interval_list_length: int) -> None:
    if sequence_file_path_and_interval_list_length != 3:
        invalid_sequences_list_text_file_path_message = \
            "Sequences path list text file lines must be formatted as follow: {0} " \
            "(e.g., {1}; (0, L-1) interval stands for entire sequence length)." \
            .format("sequence_file_path,start_position,end_position",
                    "sequence.fasta,0,L-1")
        raise InvalidSequencesPathListTextFileError(invalid_sequences_list_text_file_path_message)


def validate_sequences_path_list_count(sequences_path_list_count: int) -> None:
    if sequences_path_list_count < 2:
        invalid_sequences_list_text_file_path_message = "Sequences path list text file must have at least 2 lines."
        raise InvalidSequencesPathListTextFileError(invalid_sequences_list_text_file_path_message)


def validate_sequences_path_list_text_file(sequences_list_text_file_path: Path) -> None:
    sequences_path_list_count = 0
    with open(sequences_list_text_file_path, mode="r") as sequences_path_list_text_file:
        for sequence_file_path_and_interval in sequences_path_list_text_file:
            sequences_path_list_count = sequences_path_list_count + 1
            sequence_file_path_and_interval_list = sequence_file_path_and_interval.strip().split(",")
            sequence_file_path_and_interval_list_length = len(sequence_file_path_and_interval_list)
            validate_sequence_file_path_and_interval_list_length(sequence_file_path_and_interval_list_length)
    validate_sequences_path_list_count(sequences_path_list_count)


def get_supported_diff_phase_versions_list() -> list:
    return ["1", "opt"]


def validate_diff_phase_version(diff_phase_version: str) -> None:
    supported_diff_phase_versions_list = get_supported_diff_phase_versions_list()
    if diff_phase_version not in supported_diff_phase_versions_list:
        invalid_diff_phase_version_message = "Supported Diff phase versions: {0}." \
            .format(", ".join(supported_diff_phase_versions_list))
        raise InvalidDiffPhaseVersionError(invalid_diff_phase_version_message)


def validate_max_d(max_d: str) -> None:
    if max_d == "N-1":
        pass
    else:
        max_d = int(max_d)
        if max_d <= 0:
            invalid_max_d_message = "Multiple sequences DataFrames must have at least 1 sequence."
            raise InvalidMaxdError(invalid_max_d_message)


def validate_enable_customized_partitioning(enable_customized_partitioning: str) -> None:
    supported_enable_customized_partitioning_values_list = ["True", "False"]
    if enable_customized_partitioning not in supported_enable_customized_partitioning_values_list:
        invalid_enable_customized_partitioning_message = "Supported enable customized partitioning values: {0}." \
            .format(", ".join(supported_enable_customized_partitioning_values_list))
        raise InvalidEnableCustomizedPartitioningError(invalid_enable_customized_partitioning_message)


def get_supported_collection_phase_versions_list() -> list:
    return ["None", "ST", "DW", "MW"]


def validate_collection_phase_version(collection_phase_version: str) -> None:
    supported_collection_phase_versions_list = get_supported_collection_phase_versions_list()
    if collection_phase_version not in supported_collection_phase_versions_list:
        invalid_collection_phase_version_message = "Supported Collection phase versions: {0}." \
            .format(", ".join(supported_collection_phase_versions_list))
        raise InvalidCollectionPhaseVersionError(invalid_collection_phase_version_message)


def validate_diff_sequences_parameters(dsp: DiffSequencesParameters) -> None:
    # VALIDATE LOGGING DIRECTORY PATH
    validate_logging_directory_path(dsp.logging_directory_path)

    # VALIDATE RESULTS DIRECTORY PATH
    validate_results_directory_path(dsp.results_directory_path)

    # VALIDATE METRICS DIRECTORY PATH
    validate_metrics_directory_path(dsp.metrics_directory_path)

    # VALIDATE SEQUENCES PATH LIST TEXT FILE
    validate_sequences_path_list_text_file(dsp.sequences_list_text_file_path)

    # VALIDATE DIFF PHASE VERSION
    validate_diff_phase_version(dsp.diff_phase_version)

    # VALIDATE MAX SEQUENCES PER DATAFRAME (max_d)
    validate_max_d(dsp.max_d)

    # VALIDATE ENABLE CUSTOMIZED PARTITIONING BOOLEAN
    validate_enable_customized_partitioning(dsp.enable_customized_partitioning)

    # CAST ENABLE CUSTOMIZED PARTITIONING STRING VALUE TO BOOLEAN
    dsp.enable_customized_partitioning = strtobool(dsp.enable_customized_partitioning)

    # VALIDATE COLLECT APPROACH
    validate_collection_phase_version(dsp.collection_phase_version)


def create_spark_conf(parsed_parameters_dictionary: dict) -> SparkConf():
    # READ ALL SPARK PROPERTIES AND SET ON SPARK CONF
    spark_conf = SparkConf()
    for key, value in parsed_parameters_dictionary["DiffSequencesSparkProperties"].items():
        spark_conf.set(key, value)
    return spark_conf


def get_or_create_spark_session(spark_conf: SparkConf) -> SparkSession:
    return SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()


def get_spark_context(spark_session: SparkSession) -> SparkContext:
    return spark_session.sparkContext


def get_spark_app_name(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.app.name")


def get_spark_app_id(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.app.id")


def get_spark_executors_count(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.executor.instances"))


def get_spark_cores_max_count(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.cores.max"))


def get_spark_cores_per_executor(spark_context: SparkContext) -> int:
    return int(get_spark_cores_max_count(spark_context) / get_spark_executors_count(spark_context))


def get_spark_executor_memory(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.executor.memory")


def load_diff_sequences_spark_properties(dss: DiffSequencesSpark,
                                         parsed_parameters_dictionary: dict) -> None:
    # CREATE SPARK CONF
    dss.spark_conf = create_spark_conf(parsed_parameters_dictionary)

    # GET OR CREATE SPARK SESSION
    dss.spark_session = get_or_create_spark_session(dss.spark_conf)

    # GET SPARK CONTEXT
    dss.spark_context = get_spark_context(dss.spark_session)

    # GET APP NAME
    dss.app_name = get_spark_app_name(dss.spark_context)

    # GET APP ID
    dss.app_id = get_spark_app_id(dss.spark_context)

    # GET EXECUTORS COUNT (--num-executors)
    dss.spark_executors_count = get_spark_executors_count(dss.spark_context)

    # GET TOTAL CORES COUNT (--total-executor-cores)
    dss.spark_executors_cores_count = get_spark_cores_max_count(dss.spark_context)

    # GET CORES PER EXECUTOR
    dss.cores_per_spark_executor = get_spark_cores_per_executor(dss.spark_context)

    # GET EXECUTOR MEMORY (--executor-memory)
    dss.memory_per_spark_executor = get_spark_executor_memory(dss.spark_context)


def set_logger_basic_config(logging_directory_path: Path,
                            app_name: str,
                            app_id: str) -> None:
    app_name_path = logging_directory_path.joinpath(app_name)
    if not app_name_path.exists():
        app_name_path.mkdir()
    app_id_path = app_name_path.joinpath(app_id)
    if not app_id_path.exists():
        app_id_path.mkdir()
    logging_file_name = "{0}/{1}/logging.log" \
        .format(app_name,
                app_id)
    basicConfig(filename=logging_directory_path.joinpath(logging_file_name),
                format="%(asctime)s %(message)s",
                level=INFO)


def log_create_spark_session_time(app_name: str,
                                  create_spark_session_time_seconds: time,
                                  logger: Logger) -> None:
    spark_session_create_time_message = "({0}) Spark Session Create Time: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(create_spark_session_time_seconds, 4)),
                str(round((create_spark_session_time_seconds / 60), 4)))
    logger.info(spark_session_create_time_message)


def log_diff_sequences_spark(dss: DiffSequencesSpark,
                             logger: Logger) -> None:
    # LOG APP ID
    app_id_message = "({0}) Application ID: {1}" \
        .format(dss.app_name,
                dss.app_id)
    logger.info(app_id_message)

    # LOG NUMBER OF SPARK EXECUTORS (--num-executors)
    spark_executors_count_message = "({0}) Spark Executors Count: {1}" \
        .format(dss.app_name,
                str(dss.spark_executors_count))
    logger.info(spark_executors_count_message)

    # LOG NUMBER OF SPARK EXECUTORS CORES (--total-executor-cores)
    spark_executors_cores_count_message = "({0}) Spark Executors Cores Count: {1}" \
        .format(dss.app_name,
                str(dss.spark_executors_cores_count))
    logger.info(spark_executors_cores_count_message)

    # LOG CORES PER SPARK EXECUTOR
    cores_per_spark_executor_message = "({0}) Cores per Spark Executor: {1}" \
        .format(dss.app_name,
                str(dss.cores_per_spark_executor))
    logger.info(cores_per_spark_executor_message)

    # LOG MEMORY PER SPARK EXECUTOR (--executor-memory)
    spark_executor_memory_message = "({0}) Memory per Spark Executor: {1}" \
        .format(dss.app_name,
                dss.memory_per_spark_executor)
    logger.info(spark_executor_memory_message)


def get_ordinal_number_suffix(number: int) -> str:
    number_to_str = str(number)
    if number_to_str.endswith("1"):
        return "st"
    if number_to_str.endswith("2"):
        return "nd"
    if number_to_str.endswith("3"):
        return "rd"
    else:
        return "th"


def interval_timer_function(app_name: str,
                            logger: Logger) -> None:
    interval_in_minutes = 15
    interval_count = 0
    while True:
        start = time.time()
        while True:
            end = (time.time() - start) / 60
            if end >= interval_in_minutes:
                interval_count = interval_count + 1
                break
            time.sleep(1)
        ordinal_number_suffix = get_ordinal_number_suffix(interval_count)
        interval_timer_message = "({0}) Interval Timer Thread: Interval of {1} minute(s) ({2}{3} time)" \
            .format(app_name,
                    str(interval_in_minutes),
                    str(interval_count),
                    ordinal_number_suffix)
        print(interval_timer_message)
        logger.info(interval_timer_message)


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
    if sequence_file_end_position == "L-1":
        sequence_data_splitted = sequence_data[split_start_position:]
    else:
        split_end_position = int(sequence_file_end_position)
        sequence_data_splitted = sequence_data[split_start_position:split_end_position]
    parsed_sequence_file = [sequence_identification, sequence_data_splitted]
    return parsed_sequence_file


def generate_sequences_list(sequences_list_text_file_path: Path) -> list:
    sequences_list = []
    with open(sequences_list_text_file_path, mode="r") as sequences_path_list_text_file:
        for sequence_file_path_and_interval in sequences_path_list_text_file:
            sequence_file_path_and_interval_list = sequence_file_path_and_interval.strip().split(",")
            sequence_file_path = Path(sequence_file_path_and_interval_list[0]).resolve()
            sequence_file_start_position = sequence_file_path_and_interval_list[1]
            sequence_file_end_position = sequence_file_path_and_interval_list[2]
            parsed_sequence_file = parse_sequence_file(sequence_file_path,
                                                       sequence_file_start_position,
                                                       sequence_file_end_position)
            sequences_list.append(parsed_sequence_file)
    return sequences_list


def log_generate_sequences_list_time(app_name: str,
                                     generate_sequences_list_time_seconds: time,
                                     logger: Logger) -> None:
    generate_sequences_list_time_message = "({0}) Generate Sequences List Time: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(generate_sequences_list_time_seconds, 4)),
                str(round((generate_sequences_list_time_seconds / 60), 4)))
    logger.info(generate_sequences_list_time_message)


def set_n(dsp: DiffSequencesParameters,
          sequences_list_length: int) -> None:
    dsp.n = sequences_list_length


def log_n(app_name: str,
          n: int,
          logger: Logger) -> None:
    number_of_sequences_to_diff_message = "({0}) Number of Sequences to Diff (N): {1}" \
        .format(app_name,
                str(n))
    print(number_of_sequences_to_diff_message)
    logger.info(number_of_sequences_to_diff_message)


def set_max_d(dsp: DiffSequencesParameters) -> None:
    if dsp.max_d == "N-1":
        dsp.max_d = dsp.n - 1
    else:
        dsp.max_d = int(dsp.max_d)


def log_max_d(app_name: str,
              max_d: int,
              logger: Logger) -> None:
    max_d_message = "({0}) Maximum Sequences per Dataframe (max_d): {1}" \
        .format(app_name,
                str(max_d))
    print(max_d_message)
    logger.info(max_d_message)


def estimate_diff_phases_amount_for_diff_phase_1(n: int) -> int:
    dp_a = int((n * (n - 1)) / 2)
    return dp_a


def estimate_diff_phases_amount_for_diff_phase_opt(n: int,
                                                   max_d: int) -> int:
    dp_a = 0
    if 1 <= max_d < (n / 2):
        dp_a = int(((n * (n - 1)) / max_d) - ((n * (n - max_d)) / (2 * max_d)))
    elif (n / 2) <= max_d < n:
        dp_a = int(2 * (n - 1) - max_d)
    return dp_a


def estimate_diff_phases_amount(diff_phase_version: str,
                                n: int,
                                max_d: int) -> int:
    estimated_dp_a = 0
    if diff_phase_version == "1":
        estimated_dp_a = estimate_diff_phases_amount_for_diff_phase_1(n)
    elif diff_phase_version == "opt":
        estimated_dp_a = estimate_diff_phases_amount_for_diff_phase_opt(n,
                                                                        max_d)
    return estimated_dp_a


def log_estimated_diff_phases_amount(app_name: str,
                                     estimated_dp_a: int,
                                     logger: Logger) -> None:
    estimated_diff_phases_amount_message = \
        "({0}) Estimated Diff Phases Amount (dp_a): {1}" \
        .format(app_name,
                str(estimated_dp_a))
    print(estimated_diff_phases_amount_message)
    logger.info(estimated_diff_phases_amount_message)


def generate_sequences_indices_dataframes_list_for_diff_phase_1(n: int) -> list:
    sequences_indices_dataframes_list = []
    for first_sequence_index in range(0, n - 1):
        for second_sequence_index in range(first_sequence_index + 1, n):
            sequences_indices_dataframes_list.append([[first_sequence_index],
                                                      [second_sequence_index]])
    return sequences_indices_dataframes_list


def generate_sequences_indices_dataframes_list_for_diff_phase_opt(n: int,
                                                                  max_d: int) -> list:
    sequences_indices_dataframes_list = []
    first_dataframe_sequences_indices_list = []
    second_dataframe_sequences_indices_list = []
    first_dataframe_first_sequence_index = 0
    first_dataframe_last_sequence_index = n - 1
    first_dataframe_sequences_index_range = range(first_dataframe_first_sequence_index,
                                                  first_dataframe_last_sequence_index)
    for first_dataframe_sequence_index in first_dataframe_sequences_index_range:
        second_dataframe_first_sequence_index = first_dataframe_sequence_index + 1
        second_dataframe_last_sequence_index = n
        second_dataframe_last_sequence_added = 0
        while second_dataframe_last_sequence_added != second_dataframe_last_sequence_index - 1:
            first_dataframe_sequences_indices_list.append(first_dataframe_sequence_index)
            sequences_on_second_dataframe_count = 0
            second_dataframe_sequence_index = 0
            for second_dataframe_sequence_index in range(second_dataframe_first_sequence_index,
                                                         second_dataframe_last_sequence_index):
                second_dataframe_sequences_indices_list.extend([second_dataframe_sequence_index])
                sequences_on_second_dataframe_count = sequences_on_second_dataframe_count + 1
                if sequences_on_second_dataframe_count == max_d:
                    break
            if len(first_dataframe_sequences_indices_list) > 0 and len(second_dataframe_sequences_indices_list) > 0:
                sequences_indices_dataframes_list.append([first_dataframe_sequences_indices_list,
                                                          second_dataframe_sequences_indices_list])
                second_dataframe_last_sequence_added = second_dataframe_sequence_index
                second_dataframe_first_sequence_index = second_dataframe_last_sequence_added + 1
            first_dataframe_sequences_indices_list = []
            second_dataframe_sequences_indices_list = []
    return sequences_indices_dataframes_list


def generate_sequences_indices_dataframes_list(diff_phase_version: str,
                                               n: int,
                                               max_d: int) -> list:
    sequences_indices_dataframes_list = []
    if diff_phase_version == "1":
        sequences_indices_dataframes_list = generate_sequences_indices_dataframes_list_for_diff_phase_1(n)
    elif diff_phase_version == "opt":
        sequences_indices_dataframes_list = generate_sequences_indices_dataframes_list_for_diff_phase_opt(n,
                                                                                                          max_d)
    return sequences_indices_dataframes_list


def get_actual_diff_phases_amount(sequences_indices_dataframes_list: list) -> int:
    return len(sequences_indices_dataframes_list)


def log_actual_diff_phases_amount(app_name: str,
                                  actual_dp_a: int,
                                  logger: Logger) -> None:
    sequences_comparisons_count_message = \
        "({0}) Actual Diff Phases Amount: {1}" \
        .format(app_name,
                str(actual_dp_a))
    print(sequences_comparisons_count_message)
    logger.info(sequences_comparisons_count_message)


def calculate_diff_phases_amount_estimation_absolute_error(estimated_dp_a: int,
                                                           actual_dp_a: int) -> int:
    return abs(actual_dp_a - estimated_dp_a)


def calculate_diff_phases_amount_estimation_percent_error(estimated_dp_a: int,
                                                          actual_dp_a: int) -> float:
    return (abs(actual_dp_a - estimated_dp_a) / abs(estimated_dp_a)) * 100


def log_dp_a_estimation_error(app_name: str,
                              dp_a_estimation_absolute_error: int,
                              dp_a_estimation_percent_error: float,
                              logger: Logger) -> None:
    dp_a_estimation_error_message = \
        "({0}) Diff Phases Amount (dp_a) Estimation Absolute Error: {1} ({2}%)" \
        .format(app_name,
                str(dp_a_estimation_absolute_error),
                str(round(dp_a_estimation_percent_error, 4)))
    print(dp_a_estimation_error_message)
    logger.info(dp_a_estimation_error_message)


def get_first_dataframe_sequences_indices_list(sequences_indices_dataframes_list: list,
                                               index_sequences_indices_dataframes_list: int) -> list:
    return sequences_indices_dataframes_list[index_sequences_indices_dataframes_list][0]


def get_second_dataframe_sequences_indices_list(sequences_indices_dataframes_list: list,
                                                index_sequences_indices_dataframes_list: int) -> list:
    return sequences_indices_dataframes_list[index_sequences_indices_dataframes_list][1]


def get_biggest_sequence_length_among_dataframes(sequences_list: list,
                                                 first_dataframe_sequences_indices_list: list,
                                                 second_dataframe_sequences_indices_list: list) -> int:
    biggest_sequence_length_among_dataframes = 0
    for index_first_dataframe_sequences in range(len(first_dataframe_sequences_indices_list)):
        first_dataframe_sequence_index = first_dataframe_sequences_indices_list[index_first_dataframe_sequences]
        first_dataframe_sequence_data = sequences_list[first_dataframe_sequence_index][1]
        first_dataframe_sequence_data_length = len(first_dataframe_sequence_data)
        if biggest_sequence_length_among_dataframes < first_dataframe_sequence_data_length:
            biggest_sequence_length_among_dataframes = first_dataframe_sequence_data_length
    for index_second_dataframe_sequences in range(len(second_dataframe_sequences_indices_list)):
        second_dataframe_sequence_index = second_dataframe_sequences_indices_list[index_second_dataframe_sequences]
        second_dataframe_sequence_data = sequences_list[second_dataframe_sequence_index][1]
        second_dataframe_sequence_data_length = len(second_dataframe_sequence_data)
        if biggest_sequence_length_among_dataframes < second_dataframe_sequence_data_length:
            biggest_sequence_length_among_dataframes = second_dataframe_sequence_data_length
    return biggest_sequence_length_among_dataframes


def generate_dataframe_schema_struct_list(sequences_list: list,
                                          dataframe_sequences_indices_list: list) -> list:
    dataframe_schema_struct_list = []
    dataframe_index_label = "Index"
    dataframe_schema_struct_list.append([dataframe_index_label,
                                         LongType(),
                                         False])
    for index_dataframe_sequences_indices_list in range(len(dataframe_sequences_indices_list)):
        dataframe_sequence_identification = \
            sequences_list[dataframe_sequences_indices_list[index_dataframe_sequences_indices_list]][0]
        if dataframe_sequence_identification != "Seq":
            dataframe_char_label = "Seq_" + dataframe_sequence_identification
        else:
            dataframe_char_label = "Seq_" + str(index_dataframe_sequences_indices_list + 1)
        dataframe_schema_struct_list.append([dataframe_char_label,
                                             StringType(),
                                             True])
    return dataframe_schema_struct_list


def create_dataframe_schema(dataframe_schema_struct_list: list) -> StructType:
    dataframe_schema = StructType()
    for index_dataframe_schema_struct_list in range(len(dataframe_schema_struct_list)):
        dataframe_schema.add(field=dataframe_schema_struct_list[index_dataframe_schema_struct_list][0],
                             data_type=dataframe_schema_struct_list[index_dataframe_schema_struct_list][1],
                             nullable=dataframe_schema_struct_list[index_dataframe_schema_struct_list][2])
    return dataframe_schema


def get_dataframe_schema_column_names(dataframe_schema: StructType) -> list:
    return dataframe_schema.names


def get_dataframe_sequences_data_list(sequences_list: list,
                                      dataframe_sequences_indices_list: list) -> list:
    dataframe_sequences_data_list = []
    for index_dataframe_sequences_indices_list in range(len(dataframe_sequences_indices_list)):
        sequence_data = sequences_list[dataframe_sequences_indices_list[index_dataframe_sequences_indices_list]][1]
        dataframe_sequences_data_list.append(sequence_data)
    return dataframe_sequences_data_list


def append_data_into_dataframe(dataframe_length: int,
                               dataframe_sequences_data_list: list) -> list:
    dataframe_data_list = []
    dataframe_data_aux_list = []
    for index_dataframe_length in range(dataframe_length):
        dataframe_data_aux_list.append(index_dataframe_length)
        for index_dataframe_sequences_data_list in range(len(dataframe_sequences_data_list)):
            try:
                sequence = dataframe_sequences_data_list[index_dataframe_sequences_data_list]
                nucleotide_letter = sequence[index_dataframe_length]
                if nucleotide_letter:
                    dataframe_data_aux_list.append(nucleotide_letter)
            except IndexError:
                # LENGTH OF BIGGEST SEQUENCE AMONG DATAFRAMES > LENGTH OF THIS DATAFRAME'S SEQUENCE
                # (APPEND NULL AS NUCLEOTIDE LETTER)
                dataframe_data_aux_list.append(None)
        dataframe_data_list.append(dataframe_data_aux_list)
        dataframe_data_aux_list = []
    return dataframe_data_list


def create_dataframe(spark_session: SparkSession,
                     dataframe_data: list,
                     dataframe_schema: StructType) -> DataFrame:
    return spark_session.createDataFrame(data=dataframe_data,
                                         schema=dataframe_schema,
                                         verifySchema=True)


def get_dataframe_partitions_number(dataframe: DataFrame) -> int:
    return dataframe.rdd.getNumPartitions()


def get_spark_recommended_tasks_per_cpu() -> int:
    return 3


def calculate_optimized_number_of_partitions_after_dataframe_creation(spark_context: SparkContext) -> int:
    # GET SPARK CORES MAX COUNT
    spark_cores_max_count = get_spark_cores_max_count(spark_context)

    # GET SPARK RECOMMENDED TASKS PER CPU (SPARK DOCS TUNING: LEVEL OF PARALLELISM)
    spark_recommended_tasks_per_cpu = get_spark_recommended_tasks_per_cpu()

    return spark_cores_max_count * spark_recommended_tasks_per_cpu


def repartition_dataframe(dataframe: DataFrame,
                          new_number_of_partitions: int) -> DataFrame:
    current_dataframe_num_partitions = get_dataframe_partitions_number(dataframe)
    if current_dataframe_num_partitions > new_number_of_partitions:
        # EXECUTE COALESCE (SPARK LESS-WIDE-SHUFFLE TRANSFORMATION) FUNCTION
        dataframe = dataframe.coalesce(new_number_of_partitions)
    if current_dataframe_num_partitions < new_number_of_partitions:
        # EXECUTE REPARTITION (SPARK WIDER-SHUFFLE TRANSFORMATION) FUNCTION
        dataframe = dataframe.repartition(new_number_of_partitions)
    return dataframe


def apply_customized_partitioning_after_dataframe_creation(enable_customized_partitioning: bool,
                                                           spark_context: SparkContext,
                                                           dataframe: DataFrame) -> DataFrame:
    if enable_customized_partitioning:
        dataframe_optimized_number_of_partitions = \
            calculate_optimized_number_of_partitions_after_dataframe_creation(spark_context)
        dataframe = repartition_dataframe(dataframe,
                                          dataframe_optimized_number_of_partitions)
    return dataframe


def assemble_join_conditions(first_dataframe: DataFrame,
                             first_dataframe_column_names: list,
                             second_dataframe: DataFrame,
                             second_dataframe_column_names: list):
    index_condition = first_dataframe["Index"] == second_dataframe["Index"]
    non_index_conditions_list = []
    for index_second_dataframe_column_names in range(len(second_dataframe_column_names)):
        second_dataframe_column_name_quoted = \
            "`" + second_dataframe_column_names[index_second_dataframe_column_names] + "`"
        second_dataframe_column_name_found = second_dataframe_column_name_quoted.find("Seq_") != -1
        for index_first_dataframe_column_names in range(len(first_dataframe_column_names)):
            first_dataframe_column_name_quoted = \
                "`" + first_dataframe_column_names[index_first_dataframe_column_names] + "`"
            first_dataframe_column_name_found = first_dataframe_column_name_quoted.find("Seq_") != -1
            if first_dataframe_column_name_found and second_dataframe_column_name_found:
                non_index_condition = \
                    first_dataframe[first_dataframe_column_name_quoted] != \
                    second_dataframe[second_dataframe_column_name_quoted]
                non_index_conditions_list.append(non_index_condition)
    return index_condition & reduce(lambda x, y: x | y, non_index_conditions_list)


def execute_dataframes_diff(first_dataframe: DataFrame,
                            second_dataframe: DataFrame,
                            join_conditions):
    # EXECUTE FULL OUTER JOIN (SPARK WIDER-SHUFFLE TRANSFORMATION),
    #         FILTER(SPARK NARROW TRANSFORMATION) AND
    #         DROP (SPARK NARROW TRANSFORMATION) FUNCTIONS
    d_r = first_dataframe \
        .join(second_dataframe, join_conditions, "fullouter") \
        .filter(first_dataframe["Index"].isNotNull() & second_dataframe["Index"].isNotNull()) \
        .drop(second_dataframe["Index"])
    return d_r


def substitute_equal_nucleotide_letters_on_d_r_for_diff_phase_opt(diff_phase_version: str,
                                                                  d_r: DataFrame,
                                                                  first_dataframe_column_names: list):
    if diff_phase_version == "opt":
        # UPDATE NON-DIFF LINE VALUES TO "=" CHARACTER (FOR BETTER VIEWING)
        first_dataframe_nucleotide_letter_column_quoted = "`" + first_dataframe_column_names[1] + "`"
        first_dataframe_nucleotide_letter_column_new_value = "="
        d_r_second_dataframe_columns_only_list = \
            [column for column in d_r.columns if column not in first_dataframe_column_names]
        for second_dataframe_column in d_r_second_dataframe_columns_only_list:
            second_dataframe_column_quoted = "`" + second_dataframe_column + "`"
            is_non_diff_column_comparison = col(second_dataframe_column_quoted) == \
                d_r[first_dataframe_nucleotide_letter_column_quoted]
            column_expression = when(is_non_diff_column_comparison,
                                     first_dataframe_nucleotide_letter_column_new_value) \
                .otherwise(col(second_dataframe_column_quoted))
            d_r = d_r.withColumn(second_dataframe_column,
                                 column_expression)
    return d_r


def estimate_highest_d_r_size_in_bytes(first_dataframe_schema: StructType,
                                       first_dataframe_num_rows: int,
                                       second_dataframe_schema: StructType,
                                       second_dataframe_num_rows: int) -> int:
    longtype_count = 0
    longtype_default_size = 8  # LongType(): 8 BYTES EACH
    stringtype_count = 0
    stringtype_default_size = 4  # StringType(): 4 BYTES EACH + (1 BYTE * STRING LENGTH)
    first_dataframe_schema_list = [[field.dataType, field.name] for field in first_dataframe_schema.fields]
    for schema_field_list in first_dataframe_schema_list:
        if schema_field_list[0] == LongType():
            longtype_count = longtype_count + 1
        elif schema_field_list[0] == StringType():
            stringtype_count = stringtype_count + 1
    second_dataframe_schema_list = [[field.dataType, field.name] for field in second_dataframe_schema.fields]
    for schema_field_list in second_dataframe_schema_list:
        if schema_field_list[0] == LongType():
            longtype_count = longtype_count + 1
        elif schema_field_list[0] == StringType():
            stringtype_count = stringtype_count + 1
    longtype_count = longtype_count - 1  # DISCOUNTING SECOND DATAFRAME'S INDEX (AS IT WILL BE DROPPED AFTER JOIN)
    minimum_dataframe_num_rows = min(first_dataframe_num_rows, second_dataframe_num_rows)
    longtype_size_one_row = longtype_count * longtype_default_size
    stringtype_size_one_row = stringtype_count * (stringtype_default_size + 1)
    return minimum_dataframe_num_rows * (longtype_size_one_row + stringtype_size_one_row)


def get_spark_maximum_recommended_partition_size_in_bytes() -> int:
    return 134217728  # 128 MB


def calculate_optimized_number_of_partitions_after_dataframe_shuffling(spark_context: SparkContext,
                                                                       dataframe_size_in_bytes: int) -> int:
    # GET SPARK MAXIMUM RECOMMENDED PARTITION SIZE IN BYTES (AFTER SHUFFLING OPERATIONS)
    spark_maximum_recommended_partition_size_in_bytes = get_spark_maximum_recommended_partition_size_in_bytes()

    # GET SPARK CORES MAX COUNT
    spark_cores_max_count = get_spark_cores_max_count(spark_context)

    # GET SPARK RECOMMENDED TASKS PER CPU (SPARK DOCS TUNING: LEVEL OF PARALLELISM)
    spark_recommended_tasks_per_cpu = get_spark_recommended_tasks_per_cpu()

    # SET INITIAL DIVIDER VARIABLE VALUE
    divider = spark_cores_max_count * spark_recommended_tasks_per_cpu

    # SEARCHING OPTIMIZED NUMBER OF PARTITIONS
    while True:
        if (dataframe_size_in_bytes / divider) <= spark_maximum_recommended_partition_size_in_bytes:
            return divider
        divider = divider + 1


def apply_customized_partitioning_after_dataframes_diff(enable_customized_partitioning: bool,
                                                        spark_context: SparkContext,
                                                        first_dataframe_schema: StructType,
                                                        first_dataframe_num_rows: int,
                                                        second_dataframe_schema: StructType,
                                                        second_dataframe_num_rows: int,
                                                        d_r: DataFrame) -> DataFrame:
    if enable_customized_partitioning:
        higher_estimate_d_r_size_in_bytes = estimate_highest_d_r_size_in_bytes(first_dataframe_schema,
                                                                               first_dataframe_num_rows,
                                                                               second_dataframe_schema,
                                                                               second_dataframe_num_rows)

        optimized_number_of_dataframe_partitions = \
            calculate_optimized_number_of_partitions_after_dataframe_shuffling(spark_context,
                                                                               higher_estimate_d_r_size_in_bytes)

        d_r = repartition_dataframe(d_r,
                                    optimized_number_of_dataframe_partitions)
    return d_r


def execute_diff_phase(diff_phase_version: str,
                       enable_customized_partitioning: bool,
                       spark_context: SparkContext,
                       first_dataframe_struct: DataFrameStruct,
                       second_dataframe_struct: DataFrameStruct) -> DataFrame:
    # GET FIRST DATAFRAME STRUCT'S VALUES
    first_dataframe = first_dataframe_struct.dataframe
    first_dataframe_schema = first_dataframe_struct.schema
    first_dataframe_column_names = first_dataframe_struct.column_names
    first_dataframe_num_rows = first_dataframe_struct.num_rows

    # GET SECOND DATAFRAME STRUCT'S VALUES
    second_dataframe = second_dataframe_struct.dataframe
    second_dataframe_schema = second_dataframe_struct.schema
    second_dataframe_column_names = second_dataframe_struct.column_names
    second_dataframe_num_rows = second_dataframe_struct.num_rows

    # ASSEMBLE JOIN CONDITIONS
    join_conditions = assemble_join_conditions(first_dataframe,
                                               first_dataframe_column_names,
                                               second_dataframe,
                                               second_dataframe_column_names)

    # EXECUTE DATAFRAMES DIFF (RESULTING DATAFRAME: d_r)
    d_r = execute_dataframes_diff(first_dataframe,
                                  second_dataframe,
                                  join_conditions)

    # SUBSTITUTE EQUAL NUCLEOTIDE LETTERS ON d_r (IF DIFF_opt)
    d_r = substitute_equal_nucleotide_letters_on_d_r_for_diff_phase_opt(diff_phase_version,
                                                                        d_r,
                                                                        first_dataframe_column_names)

    # APPLY CUSTOMIZED PARTITIONING ON d_r AFTER DIFF (IF ENABLED)
    d_r = apply_customized_partitioning_after_dataframes_diff(enable_customized_partitioning,
                                                              spark_context,
                                                              first_dataframe_schema,
                                                              first_dataframe_num_rows,
                                                              second_dataframe_schema,
                                                              second_dataframe_num_rows,
                                                              d_r)

    return d_r


def get_first_dataframe_first_sequence_index(first_dataframe_sequences_indices_list: list) -> int:
    return first_dataframe_sequences_indices_list[0]


def get_second_dataframe_first_sequence_index(second_dataframe_sequences_indices_list: list) -> int:
    return second_dataframe_sequences_indices_list[0]


def get_second_dataframe_last_sequence_index(second_dataframe_sequences_indices_list: list) -> int:
    return second_dataframe_sequences_indices_list[-1]


def get_collection_phase_destination_file_path(results_directory_path: Path,
                                               app_name: str,
                                               app_id: str,
                                               first_dataframe_first_sequence_index: int,
                                               second_dataframe_first_sequence_index: int,
                                               second_dataframe_last_sequence_index: int) -> Path:
    if second_dataframe_first_sequence_index != second_dataframe_last_sequence_index:
        destination_file_path = Path("{0}/{1}/{2}/sequence_{3}_diff_sequences_{4}_to_{5}.csv"
                                     .format(results_directory_path,
                                             app_name,
                                             app_id,
                                             str(first_dataframe_first_sequence_index),
                                             str(second_dataframe_first_sequence_index),
                                             str(second_dataframe_last_sequence_index)))
    else:
        destination_file_path = Path("{0}/{1}/{2}/sequence_{3}_diff_sequence_{4}.csv"
                                     .format(results_directory_path,
                                             app_name,
                                             app_id,
                                             str(first_dataframe_first_sequence_index),
                                             str(second_dataframe_last_sequence_index)))
    return destination_file_path


def show_dataframe(dataframe: DataFrame,
                   number_of_rows_to_show: int,
                   truncate_boolean: bool) -> None:
    # EXECUTE SORT (SPARK WIDER-SHUFFLE TRANSFORMATION) AND
    #         SHOW (SPARK ACTION) FUNCTIONS
    dataframe \
        .sort(dataframe["Index"].asc_nulls_last()) \
        .show(n=number_of_rows_to_show,
              truncate=truncate_boolean)


def write_dataframe_as_distributed_partial_multiple_csv_files(dataframe: DataFrame,
                                                              destination_file_path: Path,
                                                              header_boolean: bool,
                                                              write_mode: str) -> None:
    # EXECUTE SORT (SPARK WIDER-SHUFFLE TRANSFORMATION) AND
    #         WRITE CSV (SPARK ACTION) FUNCTIONS
    dataframe \
        .sort(dataframe["Index"].asc_nulls_last()) \
        .write \
        .csv(path=str(destination_file_path),
             header=header_boolean,
             mode=write_mode)


def write_dataframe_as_merged_single_csv_file(dataframe: DataFrame,
                                              destination_file_path: Path,
                                              header_boolean: bool,
                                              write_mode: str) -> None:
    # EXECUTE COALESCE (SPARK LESS-WIDE-SHUFFLE TRANSFORMATION),
    #         SORT (SPARK WIDER-SHUFFLE TRANSFORMATION) AND
    #         WRITE CSV (SPARK ACTION) FUNCTIONS
    dataframe \
        .coalesce(1) \
        .sort(dataframe["Index"].asc_nulls_last()) \
        .write \
        .csv(path=str(destination_file_path),
             header=header_boolean,
             mode=write_mode)


def execute_collection_phase(d_r: DataFrame,
                             collection_phase_version: str,
                             collection_phase_destination_file_path: Path) -> None:
    if collection_phase_version == "None":
        # DO NOT COLLECT DIFF PHASE RESULTING DATAFRAME (d_r)
        pass
    elif collection_phase_version == "ST":  # ST = SHOW AS TABLE
        # SHOW DIFF PHASE RESULTING DATAFRAME (d_r) AS TABLE FORMAT ON COMMAND-LINE TERMINAL
        show_dataframe(d_r,
                       d_r.count(),
                       False)
    elif collection_phase_version == "DW":  # DW = DISTRIBUTED WRITE
        # WRITE TO DISK DIFF PHASE RESULTING DATAFRAME (d_r) AS MULTIPLE PARTIAL CSV FILES
        # (EACH SPARK EXECUTOR WRITES ITS PARTITION'S DATA LOCALLY)
        write_dataframe_as_distributed_partial_multiple_csv_files(d_r,
                                                                  collection_phase_destination_file_path,
                                                                  True,
                                                                  "append")
    elif collection_phase_version == "MW":  # MW = MERGED WRITE
        # WRITE TO DISK DIFF PHASE RESULTING DATAFRAME (d_r) AS SINGLE CSV FILE
        # (EACH SPARK EXECUTOR SENDS ITS PARTITION'S DATA TO ONE EXECUTOR WHICH WILL MERGE AND WRITE THEM)
        write_dataframe_as_merged_single_csv_file(d_r,
                                                  collection_phase_destination_file_path,
                                                  True,
                                                  "append")


def log_sequences_comparison_time(app_name: str,
                                  first_dataframe_first_sequence_index: int,
                                  second_dataframe_first_sequence_index: int,
                                  second_dataframe_last_sequence_index: int,
                                  sequences_comparison_time_seconds: time,
                                  logger: Logger) -> None:
    if second_dataframe_first_sequence_index != second_dataframe_last_sequence_index:
        sequences_comparison_time_message = "({0}) Sequence {1} X Sequences [{2}, ..., {3}] " \
                                            "Comparison Time (DataFrames → Create, Diff & Collection): " \
                                            "{4} sec (≈ {5} min)" \
            .format(app_name,
                    str(first_dataframe_first_sequence_index),
                    str(second_dataframe_first_sequence_index),
                    str(second_dataframe_last_sequence_index),
                    str(round(sequences_comparison_time_seconds, 4)),
                    str(round((sequences_comparison_time_seconds / 60), 4)))
    else:
        sequences_comparison_time_message = "({0}) Sequence {1} X Sequence {2} " \
                                            "Comparison Time (DataFrames → Create, Diff & Collection): " \
                                            "{3} sec (≈ {4} min)" \
            .format(app_name,
                    str(first_dataframe_first_sequence_index),
                    str(second_dataframe_last_sequence_index),
                    str(round(sequences_comparison_time_seconds, 4)),
                    str(round((sequences_comparison_time_seconds / 60), 4)))
    print(sequences_comparison_time_message)
    logger.info(sequences_comparison_time_message)


def get_number_of_sequences_comparisons_left(actual_dp_a: int,
                                             sequences_comparisons_count: int) -> int:
    return actual_dp_a - sequences_comparisons_count


def get_average_sequences_comparison_time(sequences_comparisons_time_seconds: time,
                                          sequences_comparisons_count: int) -> time:
    return sequences_comparisons_time_seconds / sequences_comparisons_count


def estimate_time_left(number_of_sequences_comparisons_left: int,
                       average_sequences_comparison_time_seconds: time) -> time:
    return number_of_sequences_comparisons_left * average_sequences_comparison_time_seconds


def print_real_time_metrics(app_name: str,
                            sequences_comparisons_count: int,
                            number_of_sequences_comparisons_left: int,
                            average_sequences_comparison_time_seconds: time,
                            estimated_time_left_seconds: time) -> None:
    real_time_metrics_message = "({0}) Sequences Comparisons Done: {1} ({2} Left) | " \
                                "Average Sequences Comparison Time: {3} sec (≈ {4} min) | " \
                                "Estimated Time Left: {5} sec (≈ {6} min)" \
        .format(app_name,
                str(sequences_comparisons_count),
                str(number_of_sequences_comparisons_left),
                str(round(average_sequences_comparison_time_seconds, 4)),
                str(round((average_sequences_comparison_time_seconds / 60), 4)),
                str(round(estimated_time_left_seconds, 4)),
                str(round((estimated_time_left_seconds / 60), 4)))
    print(real_time_metrics_message)


def log_average_sequences_comparison_time(app_name: str,
                                          average_sequences_comparison_time_seconds: time,
                                          logger: Logger) -> None:
    average_sequences_comparison_time_message = \
        "({0}) Average Sequences Comparison Time (DataFrames → Create, Diff & Collection): {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(average_sequences_comparison_time_seconds, 4)),
                str(round((average_sequences_comparison_time_seconds / 60), 4)))
    logger.info(average_sequences_comparison_time_message)


def log_sequences_comparisons_count(app_name: str,
                                    sequences_comparisons_count: int,
                                    logger: Logger) -> None:
    sequences_comparisons_count_message = "({0}) Sequences Comparisons Count: {1}" \
        .format(app_name,
                str(sequences_comparisons_count))
    logger.info(sequences_comparisons_count_message)


def log_diff_phases_time(app_name: str,
                         diff_phase_version: str,
                         diff_phases_time_seconds: time,
                         logger: Logger) -> None:
    diff_phases_time_message = \
        "({0}) Diff Phase {1} Time (Transformations → Join, Filter & Drop): {2} sec (≈ {3} min)" \
        .format(app_name,
                diff_phase_version,
                str(round(diff_phases_time_seconds, 4)),
                str(round((diff_phases_time_seconds / 60), 4)))
    logger.info(diff_phases_time_message)


def log_collection_phases_time(app_name: str,
                               collection_phase_version: str,
                               collection_phases_time_seconds: time,
                               logger: Logger) -> None:
    collection_phase_description = None
    if collection_phase_version == "None":
        pass
    elif collection_phase_version == "ST":
        collection_phase_description = \
            "Collection Phase ST Time (Transformation → Sort | Action → Show)"
    elif collection_phase_version == "DW":
        collection_phase_description = \
            "Collection Phase DW Time (Transformation → Sort | Action → Write CSV)"
    elif collection_phase_version == "MW":
        collection_phase_description = \
            "Collection Phase MW Time (Transformations → Coalesce & Sort | Action → Write CSV)"
    if collection_phase_description:
        collection_phases_time_message = "({0}) {1}: {2} sec (≈ {3} min)" \
            .format(app_name,
                    collection_phase_description,
                    str(round(collection_phases_time_seconds, 4)),
                    str(round((collection_phases_time_seconds / 60), 4)))
        logger.info(collection_phases_time_message)


def log_spark_dataframes_partitions_count(app_name: str,
                                          spark_dataframes_partitions_count: int,
                                          logger: Logger) -> None:
    spark_dataframes_partitions_count_message = "({0}) Spark DataFrames Partitions Count: {1}" \
        .format(app_name,
                str(spark_dataframes_partitions_count))
    logger.info(spark_dataframes_partitions_count_message)


def execute_sequences_comparisons(dss: DiffSequencesSpark,
                                  dsp: DiffSequencesParameters,
                                  sequences_list: list,
                                  logger: Logger) -> None:
    # INITIALIZE METRICS VARIABLES
    sequences_comparisons_count = 0
    spark_dataframes_partitions_count = 0
    diff_phases_time_seconds = 0
    collection_phases_time_seconds = 0
    average_sequences_comparison_time_seconds = 0
    sequences_comparisons_time_seconds = 0

    # ESTIMATE DIFF PHASES AMOUNT (dp_a)
    estimated_dp_a = estimate_diff_phases_amount(dsp.diff_phase_version,
                                                 dsp.n,
                                                 dsp.max_d)

    # LOG ESTIMATED DIFF PHASES AMOUNT (dp_a)
    log_estimated_diff_phases_amount(dss.app_name,
                                     estimated_dp_a,
                                     logger)

    # GENERATE SEQUENCES INDICES DATAFRAMES LIST
    sequences_indices_dataframes_list = generate_sequences_indices_dataframes_list(dsp.diff_phase_version,
                                                                                   dsp.n,
                                                                                   dsp.max_d)

    # GET ACTUAL DIFF PHASES AMOUNT
    actual_dp_a = get_actual_diff_phases_amount(sequences_indices_dataframes_list)

    # LOG ACTUAL DIFF PHASES AMOUNT
    log_actual_diff_phases_amount(dss.app_name,
                                  actual_dp_a,
                                  logger)

    # CALCULATE DIFF PHASES AMOUNT (dp_a) ESTIMATION ABSOLUTE ERROR
    dp_a_estimation_absolute_error = calculate_diff_phases_amount_estimation_absolute_error(estimated_dp_a,
                                                                                            actual_dp_a)

    # CALCULATE DIFF PHASES AMOUNT (dp_a) ESTIMATION PERCENT ERROR
    dp_a_estimation_percent_error = calculate_diff_phases_amount_estimation_percent_error(estimated_dp_a,
                                                                                          actual_dp_a)

    # LOG DIFF PHASES AMOUNT (dp_a) ESTIMATION ABSOLUTE AND PERCENT ERRORS
    log_dp_a_estimation_error(dss.app_name,
                              dp_a_estimation_absolute_error,
                              dp_a_estimation_percent_error,
                              logger)

    # ITERATE THROUGH SEQUENCES INDICES DATAFRAMES LIST
    for index_sequences_indices_dataframes_list in range(len(sequences_indices_dataframes_list)):
        # SEQUENCES COMPARISON START TIME
        sequences_comparison_start_time_seconds = time.time()

        # GET FIRST DATAFRAME SEQUENCES INDICES LIST
        first_dataframe_sequences_indices_list = \
            get_first_dataframe_sequences_indices_list(sequences_indices_dataframes_list,
                                                       index_sequences_indices_dataframes_list)

        # GET SECOND DATAFRAME SEQUENCES INDICES LIST
        second_dataframe_sequences_indices_list = \
            get_second_dataframe_sequences_indices_list(sequences_indices_dataframes_list,
                                                        index_sequences_indices_dataframes_list)

        # GET BIGGEST SEQUENCE LENGTH AMONG DATAFRAMES
        biggest_sequence_length_among_dataframes = \
            get_biggest_sequence_length_among_dataframes(sequences_list,
                                                         first_dataframe_sequences_indices_list,
                                                         second_dataframe_sequences_indices_list)

        # SET FIRST DATAFRAME'S LENGTH
        first_dataframe_length = biggest_sequence_length_among_dataframes

        # SET SECOND DATAFRAME'S LENGTH
        second_dataframe_length = biggest_sequence_length_among_dataframes

        # GENERATE FIRST DATAFRAME SCHEMA STRUCT LIST
        first_dataframe_schema_struct_list = \
            generate_dataframe_schema_struct_list(sequences_list,
                                                  first_dataframe_sequences_indices_list)

        # CREATE FIRST DATAFRAME SCHEMA
        first_dataframe_schema = create_dataframe_schema(first_dataframe_schema_struct_list)

        # GET FIRST DATAFRAME SCHEMA'S COLUMN NAMES
        first_dataframe_schema_column_names = get_dataframe_schema_column_names(first_dataframe_schema)

        # GET FIRST DATAFRAME'S SEQUENCES DATA LIST
        first_dataframe_sequences_data_list = \
            get_dataframe_sequences_data_list(sequences_list,
                                              first_dataframe_sequences_indices_list)

        # APPEND DATA INTO FIRST DATAFRAME
        first_dataframe_data = append_data_into_dataframe(first_dataframe_length,
                                                          first_dataframe_sequences_data_list)

        # CREATE FIRST DATAFRAME
        first_dataframe = create_dataframe(dss.spark_session,
                                           first_dataframe_data,
                                           first_dataframe_schema)

        # APPLY CUSTOMIZED PARTITIONING ON FIRST DATAFRAME AFTER CREATION (IF ENABLED)
        first_dataframe = apply_customized_partitioning_after_dataframe_creation(dsp.enable_customized_partitioning,
                                                                                 dss.spark_context,
                                                                                 first_dataframe)

        # GET FIRST DATAFRAME'S PARTITIONS NUMBER
        first_dataframe_partitions_number = get_dataframe_partitions_number(first_dataframe)

        # INCREASE SPARK DATAFRAMES PARTITIONS COUNT
        spark_dataframes_partitions_count = spark_dataframes_partitions_count + first_dataframe_partitions_number

        # CREATE FIRST DATAFRAME'S STRUCT
        first_dataframe_struct = DataFrameStruct(first_dataframe,
                                                 first_dataframe_schema,
                                                 first_dataframe_schema_column_names,
                                                 first_dataframe_length)

        # GENERATE SECOND DATAFRAME SCHEMA STRUCT LIST
        second_dataframe_schema_struct_list = \
            generate_dataframe_schema_struct_list(sequences_list,
                                                  second_dataframe_sequences_indices_list)

        # CREATE SECOND DATAFRAME SCHEMA
        second_dataframe_schema = create_dataframe_schema(second_dataframe_schema_struct_list)

        # GET SECOND DATAFRAME SCHEMA'S COLUMN NAMES
        second_dataframe_schema_column_names = get_dataframe_schema_column_names(second_dataframe_schema)

        # GET SECOND DATAFRAME'S SEQUENCES DATA LIST
        second_dataframe_sequences_data_list = \
            get_dataframe_sequences_data_list(sequences_list,
                                              second_dataframe_sequences_indices_list)

        # APPEND DATA INTO SECOND DATAFRAME
        second_dataframe_data = append_data_into_dataframe(second_dataframe_length,
                                                           second_dataframe_sequences_data_list)

        # CREATE SECOND DATAFRAME
        second_dataframe = create_dataframe(dss.spark_session,
                                            second_dataframe_data,
                                            second_dataframe_schema)

        # APPLY CUSTOMIZED PARTITIONING ON SECOND DATAFRAME AFTER CREATION (IF ENABLED)
        second_dataframe = apply_customized_partitioning_after_dataframe_creation(dsp.enable_customized_partitioning,
                                                                                  dss.spark_context,
                                                                                  second_dataframe)

        # GET SECOND DATAFRAME'S PARTITIONS NUMBER
        second_dataframe_partitions_number = get_dataframe_partitions_number(second_dataframe)

        # INCREASE SPARK DATAFRAMES PARTITIONS COUNT
        spark_dataframes_partitions_count = spark_dataframes_partitions_count + second_dataframe_partitions_number

        # CREATE SECOND DATAFRAME'S STRUCT
        second_dataframe_struct = DataFrameStruct(second_dataframe,
                                                  second_dataframe_schema,
                                                  second_dataframe_schema_column_names,
                                                  second_dataframe_length)

        # DIFF PHASE START TIME
        diff_phase_start_time = time.time()

        # EXECUTE DIFF PHASE
        d_r = execute_diff_phase(dsp.diff_phase_version,
                                 dsp.enable_customized_partitioning,
                                 dss.spark_context,
                                 first_dataframe_struct,
                                 second_dataframe_struct)

        # DIFF PHASE END TIME
        diff_phase_end_time = time.time() - diff_phase_start_time

        # INCREASE DIFF PHASES TIME
        diff_phases_time_seconds = diff_phases_time_seconds + diff_phase_end_time

        # INCREASE SEQUENCES COMPARISONS COUNT
        sequences_comparisons_count = sequences_comparisons_count + 1

        # GET DIFF PHASE RESULTING DATAFRAME'S (d_r) PARTITIONS NUMBER
        d_r_partitions_number = get_dataframe_partitions_number(d_r)

        # INCREASE SPARK DATAFRAMES PARTITIONS COUNT
        spark_dataframes_partitions_count = spark_dataframes_partitions_count + d_r_partitions_number

        # GET FIRST DATAFRAME'S FIRST SEQUENCE INDEX
        first_dataframe_first_sequence_index = \
            get_first_dataframe_first_sequence_index(first_dataframe_sequences_indices_list)

        # GET SECOND DATAFRAME'S FIRST SEQUENCE INDEX
        second_dataframe_first_sequence_index = \
            get_second_dataframe_first_sequence_index(second_dataframe_sequences_indices_list)

        # GET SECOND DATAFRAME'S LAST SEQUENCE INDEX
        second_dataframe_last_sequence_index = \
            get_second_dataframe_last_sequence_index(second_dataframe_sequences_indices_list)

        # GET COLLECTION PHASE'S DESTINATION FILE PATH
        collection_phase_destination_file_path = \
            get_collection_phase_destination_file_path(dsp.results_directory_path,
                                                       dss.app_name,
                                                       dss.app_id,
                                                       first_dataframe_first_sequence_index,
                                                       second_dataframe_first_sequence_index,
                                                       second_dataframe_last_sequence_index)

        # COLLECTION PHASE START TIME
        collection_phase_start_time = time.time()

        # EXECUTE COLLECTION PHASE
        execute_collection_phase(d_r,
                                 dsp.collection_phase_version,
                                 collection_phase_destination_file_path)

        # COLLECTION PHASE END TIME
        collection_phase_end_time = time.time() - collection_phase_start_time

        # INCREASE COLLECTION PHASES TIME
        collection_phases_time_seconds = collection_phases_time_seconds + collection_phase_end_time

        # SEQUENCES COMPARISON END TIME
        sequences_comparison_time_seconds = time.time() - sequences_comparison_start_time_seconds

        # INCREASE SEQUENCES COMPARISONS TIME
        sequences_comparisons_time_seconds = sequences_comparisons_time_seconds + sequences_comparison_time_seconds

        # LOG SEQUENCES COMPARISON TIME
        log_sequences_comparison_time(dss.app_name,
                                      first_dataframe_first_sequence_index,
                                      second_dataframe_first_sequence_index,
                                      second_dataframe_last_sequence_index,
                                      sequences_comparison_time_seconds,
                                      logger)

        # GET NUMBER OF SEQUENCES COMPARISONS LEFT
        number_of_sequences_comparisons_left = get_number_of_sequences_comparisons_left(actual_dp_a,
                                                                                        sequences_comparisons_count)

        # GET AVERAGE SEQUENCES COMPARISON TIME
        average_sequences_comparison_time_seconds = \
            get_average_sequences_comparison_time(sequences_comparisons_time_seconds,
                                                  sequences_comparisons_count)

        # ESTIMATE TIME LEFT
        estimated_time_left_seconds = estimate_time_left(number_of_sequences_comparisons_left,
                                                         average_sequences_comparison_time_seconds)

        # PRINT REAL TIME METRICS
        print_real_time_metrics(dss.app_name,
                                sequences_comparisons_count,
                                number_of_sequences_comparisons_left,
                                average_sequences_comparison_time_seconds,
                                estimated_time_left_seconds)

    # LOG AVERAGE SEQUENCES COMPARISON TIME
    log_average_sequences_comparison_time(dss.app_name,
                                          average_sequences_comparison_time_seconds,
                                          logger)

    # LOG SEQUENCES COMPARISONS COUNT
    log_sequences_comparisons_count(dss.app_name,
                                    sequences_comparisons_count,
                                    logger)

    # LOG DIFF PHASES TIME
    log_diff_phases_time(dss.app_name,
                         dsp.diff_phase_version,
                         diff_phases_time_seconds,
                         logger)

    # LOG COLLECTION PHASES TIME
    log_collection_phases_time(dss.app_name,
                               dsp.collection_phase_version,
                               collection_phases_time_seconds,
                               logger)

    # LOG SPARK DATAFRAMES PARTITIONS COUNT
    log_spark_dataframes_partitions_count(dss.app_name,
                                          spark_dataframes_partitions_count,
                                          logger)


def get_spark_driver_host(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.driver.host")


def get_spark_ui_port(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.ui.port")


def log_collected_spark_app_numerical_metrics_list(spark_app_numerical_metrics_list: list,
                                                   app_name: str,
                                                   logger: Logger) -> None:
    # LOG SPARK JOBS NUMERICAL METRICS
    spark_jobs_numerical_metrics_list = spark_app_numerical_metrics_list[0]
    total_jobs = spark_jobs_numerical_metrics_list[0][1]
    succeeded_jobs = spark_jobs_numerical_metrics_list[1][1]
    running_jobs = spark_jobs_numerical_metrics_list[2][1]
    failed_jobs = spark_jobs_numerical_metrics_list[3][1]
    jobs_metrics_message = \
        "({0}) Spark Jobs Count: {1} " \
        "(Succeeded: {2} | Running: {3} | Failed: {4})" \
        .format(app_name,
                total_jobs,
                succeeded_jobs,
                running_jobs,
                failed_jobs)
    logger.info(jobs_metrics_message)

    # LOG SPARK TASKS NUMERICAL METRICS
    spark_tasks_numerical_metrics_list = spark_app_numerical_metrics_list[1]
    total_tasks = spark_tasks_numerical_metrics_list[0][1]
    completed_tasks = spark_tasks_numerical_metrics_list[1][1]
    skipped_tasks = spark_tasks_numerical_metrics_list[2][1]
    active_tasks = spark_tasks_numerical_metrics_list[3][1]
    failed_tasks = spark_tasks_numerical_metrics_list[4][1]
    killed_tasks = spark_tasks_numerical_metrics_list[5][1]
    tasks_metrics_message = \
        "({0}) Spark Tasks Count: {1} " \
        "(Completed: {2} | Skipped: {3} | Active: {4} | Failed: {5} | Killed: {6})" \
        .format(app_name,
                total_tasks,
                completed_tasks,
                skipped_tasks,
                active_tasks,
                failed_tasks,
                killed_tasks)
    logger.info(tasks_metrics_message)

    # LOG SPARK STAGES NUMERICAL METRICS
    spark_stages_numerical_metrics_list = spark_app_numerical_metrics_list[2]
    total_stages = spark_stages_numerical_metrics_list[0][1]
    complete_stages = spark_stages_numerical_metrics_list[1][1]
    skipped_stages = spark_stages_numerical_metrics_list[2][1]
    active_stages = spark_stages_numerical_metrics_list[3][1]
    pending_stages = spark_stages_numerical_metrics_list[4][1]
    failed_stages = spark_stages_numerical_metrics_list[5][1]
    stages_metrics_message = \
        "({0}) Spark Stages Count: {1} " \
        "(Complete: {2} | Skipped: {3} | Active: {4} | Pending: {5} | Failed: {6})" \
        .format(app_name,
                total_stages,
                complete_stages,
                skipped_stages,
                active_stages,
                pending_stages,
                failed_stages)
    logger.info(stages_metrics_message)


def log_spark_app_numerical_metrics_list_time(app_name: str,
                                              spark_app_numerical_metrics_list_time_seconds: time,
                                              logger: Logger) -> None:
    spark_job_metrics_collect_and_parse_time_message = \
        "({0}) Spark Job Metrics' Collect and Parse Time: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(spark_app_numerical_metrics_list_time_seconds, 4)),
                str(round((spark_app_numerical_metrics_list_time_seconds / 60), 4)))
    logger.info(spark_job_metrics_collect_and_parse_time_message)


def stop_spark_session(dss: DiffSequencesSpark) -> None:
    dss.spark_session.stop()


def log_spark_session_stop_time(app_name: str,
                                spark_session_stop_time_seconds: time,
                                logger: Logger) -> None:
    spark_session_stop_time_message = "({0}) Spark Session Stop Time: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(spark_session_stop_time_seconds, 4)),
                str(round((spark_session_stop_time_seconds / 60), 4)))
    logger.info(spark_session_stop_time_message)


def log_application_total_elapsed_time(app_name: str,
                                       application_total_elapsed_time_seconds: time,
                                       logger: Logger) -> None:
    total_elapsed_time_message = "({0}) Total Elapsed Time: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(application_total_elapsed_time_seconds, 4)),
                str(round((application_total_elapsed_time_seconds / 60), 4)))
    logger.info(total_elapsed_time_message)


def diff(argv: list) -> None:
    # BEGIN

    # APPLICATION START TIME
    app_start_time = time.time()

    # PRINT APPLICATION START NOTICE
    print("Application Started!")

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

    # INSTANTIATE DIFF SEQUENCES PARAMETERS
    dsp = DiffSequencesParameters()

    # LOAD DIFF SEQUENCES PARAMETERS
    load_diff_sequences_parameters(dsp,
                                   parsed_parameters_dictionary)

    # VALIDATE DIFF SEQUENCES PARAMETERS
    validate_diff_sequences_parameters(dsp)

    # INSTANTIATE DIFF SEQUENCES SPARK
    dss = DiffSequencesSpark()

    # CREATE SPARK SESSION START TIME
    create_spark_session_start_time = time.time()

    # LOAD DIFF SEQUENCES SPARK PROPERTIES
    load_diff_sequences_spark_properties(dss,
                                         parsed_parameters_dictionary)

    # CREATE SPARK SESSION TIME
    create_spark_session_time_seconds = time.time() - create_spark_session_start_time

    # SET LOGGER BASIC CONFIG
    set_logger_basic_config(dsp.logging_directory_path,
                            dss.app_name,
                            dss.app_id)

    # INSTANTIATE LOGGER
    logger = getLogger()

    # LOG SPARK SESSION CREATE TIME
    log_create_spark_session_time(dss.app_name,
                                  create_spark_session_time_seconds,
                                  logger)

    # LOG DIFF SEQUENCES SPARK
    log_diff_sequences_spark(dss,
                             logger)

    # CREATE INTERVAL TIMER THREAD (DAEMON MODE)
    interval_timer_thread = threading.Thread(target=interval_timer_function,
                                             args=(dss.app_name, logger),
                                             daemon=True)

    # START INTERVAL TIMER THREAD
    interval_timer_thread.start()

    # GENERATE SEQUENCES LIST START TIME
    generate_sequences_list_start_time = time.time()

    # GENERATE SEQUENCES LIST
    sequences_list = generate_sequences_list(dsp.sequences_list_text_file_path)

    # GENERATE SEQUENCES LIST END TIME
    generate_sequences_list_time_seconds = time.time() - generate_sequences_list_start_time

    # LOG GENERATE SEQUENCES LIST END TIME
    log_generate_sequences_list_time(dss.app_name,
                                     generate_sequences_list_time_seconds,
                                     logger)

    # SET NUMBER OF SEQUENCES TO DIFF (N)
    set_n(dsp,
          len(sequences_list))

    # LOG NUMBER OF SEQUENCES TO DIFF (N)
    log_n(dss.app_name,
          dsp.n,
          logger)

    # SET MAXIMUM SEQUENCES PER DATAFRAME (max_d)
    set_max_d(dsp)

    # LOG MAXIMUM SEQUENCES PER DATAFRAME (max_d)
    log_max_d(dss.app_name,
              dsp.max_d,
              logger)

    # EXECUTE SEQUENCES COMPARISONS
    execute_sequences_comparisons(dss,
                                  dsp,
                                  sequences_list,
                                  logger)

    # SPARK APP NUMERICAL METRICS LIST COLLECT AND LOG START TIME
    spark_app_numerical_metrics_list_start_time = time.time()

    # GET SPARK DRIVER HOST
    spark_driver_host = get_spark_driver_host(dss.spark_context)

    # GET SPARK APP ID
    spark_app_id = get_spark_app_id(dss.spark_context)

    # GET SPARK UI PORT
    spark_ui_port = get_spark_ui_port(dss.spark_context)

    # GET SPARK APP NUMERICAL METRICS LIST
    spark_app_numerical_metrics_list = get_spark_app_numerical_metrics_list(spark_driver_host,
                                                                            dss.app_name,
                                                                            spark_app_id,
                                                                            spark_ui_port,
                                                                            dsp.metrics_directory_path)

    # LOG COLLECTED SPARK APP NUMERICAL METRICS LIST
    log_collected_spark_app_numerical_metrics_list(spark_app_numerical_metrics_list,
                                                   dss.app_name,
                                                   logger)

    # SPARK APP NUMERICAL METRICS LIST COLLECT AND LOG TIME
    spark_app_numerical_metrics_list_time_seconds = time.time() - spark_app_numerical_metrics_list_start_time

    # LOG SPARK APP NUMERICAL METRICS LIST COLLECT AND LOG TIME
    log_spark_app_numerical_metrics_list_time(dss.app_name,
                                              spark_app_numerical_metrics_list_time_seconds,
                                              logger)

    # STOP SPARK SESSION START TIME
    stop_spark_session_start_time = time.time()

    # STOP SPARK SESSION
    stop_spark_session(dss)

    # SPARK SESSION STOP TIME
    spark_session_stop_time_seconds = time.time() - stop_spark_session_start_time

    # LOG SPARK SESSION STOP TIME
    log_spark_session_stop_time(dss.app_name,
                                spark_session_stop_time_seconds,
                                logger)

    # APPLICATION TOTAL ELAPSED TIME
    application_total_elapsed_time_seconds = time.time() - app_start_time

    # LOG APPLICATION TOTAL ELAPSED TIME
    log_application_total_elapsed_time(dss.app_name,
                                       application_total_elapsed_time_seconds,
                                       logger)

    # PRINT APPLICATION END NOTICE
    print("Application Finished Successfully!")

    # END
    sys.exit(0)


if __name__ == "__main__":
    diff(sys.argv)
