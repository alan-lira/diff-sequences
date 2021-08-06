from configparser import ConfigParser
from differentiator_exceptions import *
from differentiator_job_metrics import get_spark_job_metrics_counts_list
from functools import reduce
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType, StringType, StructType
import ast
import sys
import time


class DiffSequencesSpark:

    def __init__(self) -> None:
        self.spark_conf = None
        self.spark_session = None
        self.spark_context = None
        self.app_name = None
        self.app_id = None
        self.ui_port = None
        self.executors_count = None
        self.executor_memory = None
        self.total_cores_count = None
        self.cores_per_executor = None


class DiffSequencesParameters:

    def __init__(self) -> None:
        self.logging_file_path = None
        self.sequences_path_list_text_file_path = None
        self.diff_approach = None
        self.collect_approach = None


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("differentiator.dict", number_of_arguments_provided - 1)
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
    # READ LOGGING FILE PATH
    dsp.logging_file_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["logging_file_path"]))

    # READ FASTA SEQUENCES PATH LIST TEXT FILE PATH
    dsp.sequences_path_list_text_file_path = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["sequences_path_list_text_file_path"]))

    # READ DIFF APPROACH
    dsp.diff_approach = int(parsed_parameters_dictionary["DiffSequencesParameters"]["diff_approach"])

    # READ COLLECT APPROACH
    dsp.collect_approach = str(parsed_parameters_dictionary["DiffSequencesParameters"]["collect_approach"])


def validate_logging_file_path(logging_file_path: Path) -> None:
    if not logging_file_path.exists():
        invalid_logging_file_path_message = \
            "'{0}' is a invalid path for placing logging file (not exists)." \
            .format(str(logging_file_path))
        raise InvalidLoggingFilePathError(invalid_logging_file_path_message)


def validate_sequence_file_path_and_interval_list_length(sequence_file_path_and_interval_list_length: int) -> None:
    if sequence_file_path_and_interval_list_length != 3:
        invalid_sequences_path_list_text_file_path_message = \
            "Sequences path list text file lines must be formatted as follow: {0} " \
            "(e.g., {1}; [0,N] interval stands for entire sequence length)." \
            .format("sequence_file_path,start_position,end_position", "sequence.fasta,0,N")
        raise InvalidSequencesPathListTextFileError(invalid_sequences_path_list_text_file_path_message)


def validate_sequences_path_list_count(sequences_path_list_count: int) -> None:
    if sequences_path_list_count < 2:
        invalid_sequences_path_list_text_file_path_message = \
            "Sequences path list text file must have at least {0} lines." \
            .format("2")
        raise InvalidSequencesPathListTextFileError(invalid_sequences_path_list_text_file_path_message)


def validate_sequences_path_list_text_file(sequences_path_list_text_file_path: Path) -> None:
    sequences_path_list_count = 0
    with open(sequences_path_list_text_file_path, mode="r") as sequences_path_list_text_file:
        for sequence_file_path_and_interval in sequences_path_list_text_file:
            sequences_path_list_count = sequences_path_list_count + 1
            sequence_file_path_and_interval_list = sequence_file_path_and_interval.strip().split(",")
            sequence_file_path_and_interval_list_length = len(sequence_file_path_and_interval_list)
            validate_sequence_file_path_and_interval_list_length(sequence_file_path_and_interval_list_length)
    validate_sequences_path_list_count(sequences_path_list_count)


def get_supported_diff_approaches_list() -> list:
    return [1, 2]


def validate_diff_approach(diff_approach: int) -> None:
    supported_diff_approaches_list = get_supported_diff_approaches_list()
    if diff_approach not in supported_diff_approaches_list:
        invalid_diff_approach_message = "Supported Diff Approaches: {0}." \
            .format(", ".join(supported_diff_approaches_list))
        raise InvalidDiffApproachError(invalid_diff_approach_message)


def get_supported_collect_approaches_list() -> list:
    return ["None", "ST", "DW", "MW"]


def validate_collect_approach(collect_approach: str) -> None:
    supported_collect_approaches_list = get_supported_collect_approaches_list()
    if collect_approach not in supported_collect_approaches_list:
        invalid_collect_approach_message = "Supported Collect Approaches: {0}." \
            .format(", ".join(supported_collect_approaches_list))
        raise InvalidCollectApproachError(invalid_collect_approach_message)


def validate_diff_sequences_parameters(dsp: DiffSequencesParameters) -> None:
    # VALIDATE LOGGING FILE PATH
    validate_logging_file_path(dsp.logging_file_path)

    # VALIDATE SEQUENCES PATH LIST TEXT FILE
    validate_sequences_path_list_text_file(dsp.sequences_path_list_text_file_path)

    # VALIDATE DIFF APPROACH
    validate_diff_approach(dsp.diff_approach)

    # VALIDATE COLLECT APPROACH
    validate_collect_approach(dsp.collect_approach)


def set_logger_basic_config(logging_file_path: Path) -> None:
    basicConfig(filename=logging_file_path.joinpath("logging.log"),
                format="%(asctime)s %(message)s",
                level=INFO)


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


def get_spark_driver_host(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.driver.host")


def get_spark_ui_port(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.ui.port")


def get_spark_cores_max_count(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.cores.max"))


def get_spark_executors_count(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.executor.instances"))


def get_spark_cores_per_executor(spark_context: SparkContext) -> int:
    return int(get_spark_cores_max_count(spark_context) / get_spark_executors_count(spark_context))


def get_spark_executor_memory(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.executor.memory")


def get_spark_max_partition_size_in_bytes(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.sql.files.maxPartitionBytes"))


def start_diff_sequences_spark(dss: DiffSequencesSpark,
                               parsed_parameters_dictionary: dict,
                               logger: Logger) -> None:
    # CREATE SPARK CONF
    dss.spark_conf = create_spark_conf(parsed_parameters_dictionary)

    # GET OR CREATE SPARK SESSION
    create_spark_session_start = time.time()
    dss.spark_session = get_or_create_spark_session(dss.spark_conf)
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


def parse_sequences_list(sequences_path_list_text_file_path: Path) -> list:
    parsed_sequences_list = []
    with open(sequences_path_list_text_file_path, mode="r") as sequences_path_list_text_file:
        for sequence_file_path_and_interval in sequences_path_list_text_file:
            sequence_file_path_and_interval_list = sequence_file_path_and_interval.strip().split(",")
            sequence_file_path = Path(sequence_file_path_and_interval_list[0])
            sequence_file_start_position = sequence_file_path_and_interval_list[1]
            sequence_file_end_position = sequence_file_path_and_interval_list[2]
            parsed_sequence_file = parse_sequence_file(sequence_file_path,
                                                       sequence_file_start_position,
                                                       sequence_file_end_position)
            parsed_sequences_list.append(parsed_sequence_file)
    return parsed_sequences_list


def generate_sequences_list(sequences_path_list_text_file_path: Path,
                            app_name: str,
                            logger: Logger) -> list:
    # GENERATE SEQUENCES LIST
    read_sequences_start = time.time()
    parsed_sequences_list = parse_sequences_list(sequences_path_list_text_file_path)
    read_sequences_end = time.time()
    read_sequences_seconds = read_sequences_end - read_sequences_start
    read_sequences_minutes = read_sequences_seconds / 60
    generate_sequences_list_duration_message = "({0}) Generate Sequences List Duration: {1} sec (≈ {2} min)" \
        .format(app_name, str(round(read_sequences_seconds, 4)), str(round(read_sequences_minutes, 4)))
    logger.info(generate_sequences_list_duration_message)
    return parsed_sequences_list


def estimate_dataframe_size_in_bytes(sequence_length: int,
                                     dataframe_schema: StructType) -> int:
    longtype_count = 0
    longtype_default_size = 8  # LongType(): 8 Bytes
    stringtype_count = 0
    stringtype_default_size = 4  # StringType(): 4 Bytes + (String Length * 1 Byte)
    dataframe_schema_datatypes_list = [field.dataType for field in dataframe_schema.fields]
    for datatype in dataframe_schema_datatypes_list:
        if datatype == LongType():
            longtype_count = longtype_count + 1
        elif datatype == StringType():
            stringtype_count = stringtype_count + 1
    longtype_size_one_row = longtype_count * longtype_default_size
    stringtype_size_one_row = stringtype_count * (stringtype_default_size + 1)
    estimated_dataframe_size_in_bytes = sequence_length * (longtype_size_one_row + stringtype_size_one_row)
    return estimated_dataframe_size_in_bytes


def calculate_number_of_dataframe_partitions(spark_context: SparkContext,
                                             dataframe_size_in_bytes: int) -> int:
    max_partition_size = get_spark_max_partition_size_in_bytes(spark_context)
    total_cores_count = get_spark_cores_max_count(spark_context)
    recommended_tasks_per_cpu = 3  # SPARK DOCS TUNING (LEVEL OF PARALLELISM)
    divider = total_cores_count * recommended_tasks_per_cpu
    while True:
        partitions_size = dataframe_size_in_bytes / divider
        if partitions_size < max_partition_size:
            number_of_dataframe_partitions = divider
            break
        divider = divider + 1
    return number_of_dataframe_partitions


def get_dataframe_num_partitions(dataframe: DataFrame) -> int:
    return dataframe.rdd.getNumPartitions()


# TODO: REFACTOR
def execute_first_implementation_diff_operation(first_dataframe: DataFrame,
                                                second_dataframe: DataFrame) -> DataFrame:
    # GET FIRST DATAFRAME'S SCHEMA AND COLUMN NAMES
    first_dataframe_schema = first_dataframe.schema
    first_dataframe_column_names = first_dataframe_schema.names

    # GET SECOND DATAFRAME'S SCHEMA AND COLUMN NAMES
    second_dataframe_schema = second_dataframe.schema
    second_dataframe_column_names = second_dataframe_schema.names

    # ASSEMBLE JOIN CONDITIONS
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
    join_conditions = index_condition & reduce(lambda x, y: x | y, non_index_conditions_list)

    # EXECUTE DIFF OPERATION (FULL OUTER JOIN FUNCTION)
    diff_operation_resulting_dataframe = first_dataframe.join(second_dataframe, join_conditions, "fullouter") \
        .sort(first_dataframe["Index"].asc_nulls_last(), second_dataframe["Index"].asc_nulls_last()) \
        .filter(first_dataframe["Index"].isNotNull() & second_dataframe["Index"].isNotNull()) \
        .drop(second_dataframe["Index"])

    # RETURN DIFF OPERATION RESULTING DATAFRAME
    return diff_operation_resulting_dataframe


# TODO: REFACTOR
def execute_first_implementation(spark_session: SparkSession,
                                 spark_context: SparkContext,
                                 app_id: str,
                                 app_name: str,
                                 collect_approach: str,
                                 sequences_list: list,
                                 logger: Logger) -> None:
    # INITIALIZE METRICS VARIABLES
    diff_operations_count = 0
    resulting_dataframes_partitions_count = 0
    repartitioning_dataframes_duration_time_seconds = 0
    diff_operation_duration_time_seconds = 0
    collect_operation_duration_time_seconds = 0

    # ITERATE THROUGH SEQUENCES LIST
    for first_sequence_index in range(0, len(sequences_list) - 1):

        # GET FIRST DATAFRAME'S INDEX
        first_dataframe_index = first_sequence_index

        # INITIALIZE FIRST DATAFRAME DATA LIST
        first_dataframe_data_list = []

        # SET FIRST DATAFRAME STRUCT LABELS (INDEX + NUCLEOTIDE)
        first_dataframe_index_label = "Index"
        first_dataframe_sequence_identification = sequences_list[first_dataframe_index][0]
        if first_dataframe_sequence_identification != "Seq":
            first_dataframe_char_label = "Seq_" + first_dataframe_sequence_identification
        else:
            first_dataframe_char_label = "Seq_" + str(first_dataframe_index)

        # CREATE FIRST DATAFRAME SCHEMA (COMPLETE)
        first_dataframe_schema = StructType() \
            .add(first_dataframe_index_label, LongType(), nullable=False) \
            .add(first_dataframe_char_label, StringType(), nullable=True)

        # GET FIRST DATAFRAME'S SEQUENCE DATA
        first_sequence_data = sequences_list[first_dataframe_index][1]

        # GET FIRST DATAFRAME'S LENGTH
        first_dataframe_length = len(first_sequence_data)

        # APPEND FIRST SEQUENCE DATA INTO FIRST DATAFRAME
        for index_first_dataframe in range(first_dataframe_length):
            first_dataframe_data_list.append((index_first_dataframe,
                                              first_sequence_data[index_first_dataframe]))

        # ESTIMATE FIRST DATAFRAME'S SIZE IN BYTES
        estimated_first_dataframe_size_in_bytes = estimate_dataframe_size_in_bytes(first_dataframe_length,
                                                                                   first_dataframe_schema)

        # CREATE FIRST DATAFRAME
        first_dataframe = spark_session.createDataFrame(data=first_dataframe_data_list,
                                                        schema=first_dataframe_schema,
                                                        verifySchema=True)

        # CALCULATE FIRST DATAFRAME'S OPTIMIZED NUMBER OF PARTITIONS
        first_dataframe_number_of_partitions = \
            calculate_number_of_dataframe_partitions(spark_context, estimated_first_dataframe_size_in_bytes)

        # SET FIRST DATAFRAME'S CUSTOM REPARTITIONING
        repartitioning_first_dataframe_start_time = time.time()
        first_dataframe = first_dataframe.coalesce(first_dataframe_number_of_partitions)
        repartitioning_first_dataframe_end_time = time.time() - repartitioning_first_dataframe_start_time
        repartitioning_dataframes_duration_time_seconds = \
            repartitioning_dataframes_duration_time_seconds + repartitioning_first_dataframe_end_time

        for second_sequence_index in range(first_sequence_index + 1, len(sequences_list)):
            # INITIALIZE SECOND DATAFRAME DATA LIST
            second_dataframe_data_list = []

            # GET SECOND DATAFRAME'S INDEX
            second_dataframe_index = second_sequence_index

            # SET SECOND DATAFRAME STRUCT LABELS (INDEX + NUCLEOTIDE)
            second_dataframe_index_label = "Index"
            second_dataframe_sequence_identification = sequences_list[second_dataframe_index][0]
            if second_dataframe_sequence_identification != "Seq":
                second_dataframe_char_label = "Seq_" + second_dataframe_sequence_identification
            else:
                second_dataframe_char_label = "Seq_" + str(second_dataframe_index)

            # CREATE SECOND DATAFRAME SCHEMA (COMPLETE)
            second_dataframe_schema = StructType() \
                .add(second_dataframe_index_label, LongType(), nullable=False) \
                .add(second_dataframe_char_label, StringType(), nullable=True)

            # GET SECOND DATAFRAME'S SEQUENCE DATA
            second_sequence_data = sequences_list[second_dataframe_index][1]

            # GET SECOND DATAFRAME'S LENGTH
            second_dataframe_length = len(second_sequence_data)

            # APPEND SECOND SEQUENCE DATA INTO SECOND DATAFRAME
            for index_second_dataframe in range(second_dataframe_length):
                second_dataframe_data_list.append((index_second_dataframe,
                                                   second_sequence_data[index_second_dataframe]))

            # ESTIMATE SECOND DATAFRAME'S SIZE IN BYTES
            estimated_second_dataframe_size_in_bytes = estimate_dataframe_size_in_bytes(second_dataframe_length,
                                                                                        second_dataframe_schema)

            # CREATE SECOND DATAFRAME
            second_dataframe = spark_session.createDataFrame(data=second_dataframe_data_list,
                                                             schema=second_dataframe_schema,
                                                             verifySchema=True)

            # CALCULATE SECOND DATAFRAME'S OPTIMIZED NUMBER OF PARTITIONS
            second_dataframe_number_of_partitions = \
                calculate_number_of_dataframe_partitions(spark_context, estimated_second_dataframe_size_in_bytes)

            # SET SECOND DATAFRAME'S CUSTOM REPARTITIONING
            repartitioning_second_dataframe_start_time = time.time()
            second_dataframe = second_dataframe.coalesce(second_dataframe_number_of_partitions)
            repartitioning_second_dataframe_end_time = time.time() - repartitioning_second_dataframe_start_time
            repartitioning_dataframes_duration_time_seconds = \
                repartitioning_dataframes_duration_time_seconds + repartitioning_second_dataframe_end_time

            # EXECUTE FIRST IMPLEMENTATION'S DIFF OPERATION
            diff_start_time = time.time()
            diff_operation_resulting_dataframe = execute_first_implementation_diff_operation(first_dataframe,
                                                                                             second_dataframe)
            diff_end_time = time.time() - diff_start_time
            diff_operation_duration_time_seconds = diff_operation_duration_time_seconds + diff_end_time

            # INCREASE DIFF OPERATIONS COUNT
            diff_operations_count = diff_operations_count + 1

            # ESTIMATE DIFF OPERATION RESULTING DATAFRAME SIZE IN BYTES (HIGHEST SIZE POSSIBLE)
            highest_estimated_diff_operation_resulting_dataframe_size_in_bytes = \
                estimate_highest_diff_operation_resulting_dataframe_size_in_bytes(first_dataframe_length,
                                                                                  first_dataframe_schema,
                                                                                  second_dataframe_length,
                                                                                  second_dataframe_schema)

            # CALCULATE DIFF OPERATION RESULTING DATAFRAME OPTIMIZED NUMBER OF PARTITIONS
            optimized_number_of_dataframe_partitions = \
                calculate_number_of_dataframe_partitions(spark_context,
                                                         highest_estimated_diff_operation_resulting_dataframe_size_in_bytes)

            # SET DIFF OPERATION RESULTING DATAFRAME'S CUSTOM REPARTITIONING
            repartitioning_diff_operation_resulting_dataframe_start_time = time.time()
            diff_operation_resulting_dataframe = \
                diff_operation_resulting_dataframe.coalesce(optimized_number_of_dataframe_partitions)
            repartitioning_diff_operation_resulting_dataframe_end_time = \
                time.time() - repartitioning_diff_operation_resulting_dataframe_start_time
            repartitioning_dataframes_duration_time_seconds = \
                repartitioning_dataframes_duration_time_seconds + repartitioning_diff_operation_resulting_dataframe_end_time

            # GET DIFF OPERATION RESULTING DATAFRAME'S NUMBER OF PARTITIONS
            diff_operation_resulting_dataframe_num_partitions = \
                get_dataframe_num_partitions(diff_operation_resulting_dataframe)

            # INCREASE RESULTING DATAFRAMES PARTITIONS COUNT
            resulting_dataframes_partitions_count = \
                resulting_dataframes_partitions_count + diff_operation_resulting_dataframe_num_partitions

            # COLLECT DIFF OPERATION RESULTING DATAFRAME (COLLECT OPERATION)
            collect_start_time = time.time()
            destination_file_path = \
                Path("{0}Result/{1}/Sequence_{2}_Diff_Sequence_{3}.csv"
                     .format(app_name, app_id, str(first_dataframe_index), str(second_dataframe_index)))
            collect_diff_operation_resulting_dataframe(diff_operation_resulting_dataframe,
                                                       collect_approach,
                                                       destination_file_path)
            collect_end_time = time.time() - collect_start_time
            collect_operation_duration_time_seconds = collect_operation_duration_time_seconds + collect_end_time

    # LOG TOTAL NUMBER OF DIFF OPERATIONS
    total_number_of_diff_operations_message = "({0}) Total Number of Diff Operations: {1}" \
        .format(app_name, str(diff_operations_count))
    logger.info(total_number_of_diff_operations_message)

    # LOG TOTAL NUMBER OF SPARK PARTITIONS
    total_number_of_spark_partitions_message = "({0}) Total Number of Spark Partitions: {1}" \
        .format(app_name, str(resulting_dataframes_partitions_count))
    logger.info(total_number_of_spark_partitions_message)

    # LOG REPARTITIONING DATAFRAMES DURATION TIME
    repartitioning_dataframes_duration_time_minutes = repartitioning_dataframes_duration_time_seconds / 60
    repartitioning_dataframes_duration_time__message = \
        "({0}) Repartitioning Dataframes Duration: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(repartitioning_dataframes_duration_time_seconds, 4)),
                str(round(repartitioning_dataframes_duration_time_minutes, 4)))
    logger.info(repartitioning_dataframes_duration_time__message)

    # LOG DIFF OPERATION DURATION TIME
    diff_operation_duration_time_minutes = diff_operation_duration_time_seconds / 60
    diff_operation_duration_time_message = \
        "({0}) Diff Dataframes Operation Duration (Transformation: Join): {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(diff_operation_duration_time_seconds, 4)),
                str(round(diff_operation_duration_time_minutes, 4)))
    logger.info(diff_operation_duration_time_message)

    # LOG COLLECT OPERATION DURATION TIME
    collect_description = None
    if collect_approach == "None":
        pass
    elif collect_approach == "ST":
        collect_description = "Show as Table Format Duration (Action: Show as Table Format)"
    elif collect_approach == "DW":
        collect_description = "Distributed Write Operation Duration (Action: Save as Multiple CSV Files)"
    elif collect_approach == "MW":
        collect_description = "Merged Write Operation Duration (Action: Save as Single CSV File)"
    collect_operation_duration_time_minutes = collect_operation_duration_time_seconds / 60
    if collect_description:
        collect_operation_duration_time_message = "({0}) {1}: {2} sec (≈ {3} min)" \
            .format(app_name,
                    collect_description,
                    str(round(collect_operation_duration_time_seconds, 4)),
                    str(round(collect_operation_duration_time_minutes, 4)))
        logger.info(collect_operation_duration_time_message)


def generate_sequences_indices_blocks_list(sequences_list_length: int) -> list:
    sequences_indices_blocks_list = []
    first_block_sequences_indices_list = []
    second_block_sequences_indices_list = []
    for first_block_sequence_index in range(0, sequences_list_length - 1):
        first_block_sequences_indices_list.append(first_block_sequence_index)
        for second_block_sequence_index in range(first_block_sequence_index + 1, sequences_list_length):
            second_block_sequences_indices_list.extend([second_block_sequence_index])
        if len(first_block_sequences_indices_list) > 0 and len(second_block_sequences_indices_list) > 0:
            sequences_indices_blocks_list.append([first_block_sequences_indices_list,
                                                  second_block_sequences_indices_list])
        first_block_sequences_indices_list = []
        second_block_sequences_indices_list = []
    return sequences_indices_blocks_list


def get_biggest_sequence_length_among_blocks(sequences_list: list,
                                             first_block_sequences_indices_list: list,
                                             second_block_sequences_indices_list: list) -> int:
    biggest_sequence_length_among_blocks = 0
    first_block_sequence_length = len(sequences_list[first_block_sequences_indices_list[0]][1])
    if biggest_sequence_length_among_blocks < first_block_sequence_length:
        biggest_sequence_length_among_blocks = first_block_sequence_length
    for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):
        second_block_index_sequence_length = \
            len(sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1])
        if biggest_sequence_length_among_blocks < second_block_index_sequence_length:
            biggest_sequence_length_among_blocks = second_block_index_sequence_length
    return biggest_sequence_length_among_blocks


def execute_second_implementation_diff_operation(first_dataframe: DataFrame,
                                                 second_dataframe: DataFrame) -> DataFrame:
    # GET FIRST DATAFRAME'S SCHEMA AND COLUMN NAMES
    first_dataframe_schema = first_dataframe.schema
    first_dataframe_column_names = first_dataframe_schema.names

    # GET SECOND DATAFRAME'S SCHEMA AND COLUMN NAMES
    second_dataframe_schema = second_dataframe.schema
    second_dataframe_column_names = second_dataframe_schema.names

    # ASSEMBLE JOIN CONDITIONS
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
    join_conditions = index_condition & reduce(lambda x, y: x | y, non_index_conditions_list)

    # EXECUTE DIFF OPERATION (FULL OUTER JOIN FUNCTION)
    diff_operation_resulting_dataframe = first_dataframe.join(second_dataframe, join_conditions, "fullouter") \
        .sort(first_dataframe["Index"].asc_nulls_last(), second_dataframe["Index"].asc_nulls_last()) \
        .filter(first_dataframe["Index"].isNotNull() & second_dataframe["Index"].isNotNull()) \
        .drop(second_dataframe["Index"])

    # UPDATE RESULTING DATAFRAME'S NON-DIFF LINE VALUES TO "=" CHARACTER (FOR BETTER VIEWING)
    first_dataframe_nucleotide_letter_column_quoted = "`" + first_dataframe_column_names[1] + "`"
    first_dataframe_nucleotide_letter_column_new_value = "="
    diff_operation_resulting_dataframe_second_dataframe_columns_only_list = \
        [column for column in diff_operation_resulting_dataframe.columns if column not in first_dataframe_column_names]
    for second_dataframe_column in diff_operation_resulting_dataframe_second_dataframe_columns_only_list:
        second_dataframe_column_quoted = "`" + second_dataframe_column + "`"
        is_non_diff_column_comparison = col(second_dataframe_column_quoted) == \
            diff_operation_resulting_dataframe[first_dataframe_nucleotide_letter_column_quoted]
        column_expression = when(is_non_diff_column_comparison, first_dataframe_nucleotide_letter_column_new_value) \
            .otherwise(col(second_dataframe_column_quoted))
        diff_operation_resulting_dataframe = \
            diff_operation_resulting_dataframe.withColumn(second_dataframe_column, column_expression)

    # RETURN DIFF OPERATION RESULTING DATAFRAME
    return diff_operation_resulting_dataframe


def estimate_highest_diff_operation_resulting_dataframe_size_in_bytes(first_dataframe_length: int,
                                                                      first_dataframe_schema: StructType,
                                                                      second_dataframe_length: int,
                                                                      second_dataframe_schema: StructType) -> int:
    longtype_count = 0
    longtype_default_size = 8  # LongType(): 8 Bytes
    stringtype_count = 0
    stringtype_default_size = 4  # StringType(): 4 Bytes + (1 Byte * String Length)
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
    longtype_count = longtype_count - 1  # Removing second_dataframe.Index from count (will be dropped after join)
    minimum_dataframe_length = min(first_dataframe_length, second_dataframe_length)
    longtype_size_one_row = longtype_count * longtype_default_size
    stringtype_size_one_row = stringtype_count * (stringtype_default_size + 1)
    return minimum_dataframe_length * (longtype_size_one_row + stringtype_size_one_row)


# TODO: REFACTOR
def execute_second_implementation(spark_session: SparkSession,
                                  spark_context: SparkContext,
                                  app_id: str,
                                  app_name: str,
                                  collect_approach: str,
                                  sequences_list: list,
                                  logger: Logger) -> None:
    # INITIALIZE METRICS VARIABLES
    diff_operations_count = 0
    resulting_dataframes_partitions_count = 0
    repartitioning_dataframes_duration_time_seconds = 0
    diff_operation_duration_time_seconds = 0
    collect_operation_duration_time_seconds = 0

    # GENERATE SEQUENCES INDICES BLOCKS LIST
    sequences_indices_blocks_list = generate_sequences_indices_blocks_list(len(sequences_list))

    # ITERATE THROUGH SEQUENCES INDICES BLOCKS LIST
    for index_sequences_indices_blocks_list in range(len(sequences_indices_blocks_list)):

        # GET FIRST BLOCK SEQUENCES INDICES LIST
        first_block_sequences_indices_list = sequences_indices_blocks_list[index_sequences_indices_blocks_list][0]

        # GET SECOND BLOCK SEQUENCES INDICES LIST
        second_block_sequences_indices_list = sequences_indices_blocks_list[index_sequences_indices_blocks_list][1]

        # GET BIGGEST SEQUENCE LENGTH AMONG BLOCKS
        biggest_sequence_length_from_blocks = \
            get_biggest_sequence_length_among_blocks(sequences_list,
                                                     first_block_sequences_indices_list,
                                                     second_block_sequences_indices_list)

        # INITIALIZE FIRST DATAFRAME DATA LIST
        first_dataframe_data_list = []

        # SET FIRST DATAFRAME STRUCT LABELS (INDEX + NUCLEOTIDE)
        first_dataframe_index_label = "Index"
        first_block_sequence_identification = sequences_list[first_block_sequences_indices_list[0]][0]
        if first_block_sequence_identification != "Seq":
            first_dataframe_char_label = "Seq_" + first_block_sequence_identification
        else:
            first_dataframe_char_label = "Seq_" + "0"

        # CREATE FIRST DATAFRAME SCHEMA (COMPLETE)
        first_dataframe_schema = StructType() \
            .add(first_dataframe_index_label, LongType(), nullable=False) \
            .add(first_dataframe_char_label, StringType(), nullable=True)

        # INITIALIZE SECOND DATAFRAME DATA LIST (AND DATA AUX LIST)
        second_dataframe_data_list = []
        second_dataframe_data_aux_list = []

        # SET SECOND DATAFRAME STRUCT INDEX LABEL
        second_dataframe_index_label = "Index"

        # CREATE SECOND DATAFRAME SCHEMA (PARTIAL)
        second_dataframe_schema = StructType() \
            .add(second_dataframe_index_label, LongType(), nullable=False)

        # ITERATE THROUGH SECOND BLOCK SEQUENCES INDICES LIST (TO COMPLETE SECOND DATAFRAME SCHEMA)
        for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):

            # SET SECOND DATAFRAME STRUCT NUCLEOTIDE LABEL
            second_block_sequence_identification = \
                sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][0]
            if second_block_sequence_identification != "Seq":
                second_dataframe_char_label = "Seq_" + second_block_sequence_identification
            else:
                second_dataframe_char_label = "Seq_" + str(index_second_block_sequences_indices_list + 1)

            # ADD NUCLEOTIDE LABEL TO SECOND DATAFRAME SCHEMA
            second_dataframe_schema.add(second_dataframe_char_label, StringType(), nullable=True)

        # ITERATE THROUGH BIGGEST SEQUENCE LENGTH TO OBTAIN BOTH DATAFRAMES DATA
        for index_biggest_sequence_length_from_blocks in range(biggest_sequence_length_from_blocks):

            # APPEND FIRST BLOCK SEQUENCE DATA INTO FIRST DATAFRAME
            try:
                if sequences_list[first_block_sequences_indices_list[0]][1][index_biggest_sequence_length_from_blocks]:
                    first_dataframe_data_list.append((index_biggest_sequence_length_from_blocks, sequences_list[first_block_sequences_indices_list[0]][1][index_biggest_sequence_length_from_blocks]))
            except IndexError:  # BIGGEST SEQUENCE LENGTH > FIRST BLOCK SEQUENCE LENGTH (APPEND NULL DATA)
                first_dataframe_data_list.append((index_biggest_sequence_length_from_blocks, None))

            # APPEND INDEX INTO SECOND DATAFRAME DATA AUX LIST
            second_dataframe_data_aux_list.append(index_biggest_sequence_length_from_blocks)

            # ITERATE THROUGH SECOND BLOCK SEQUENCES INDICES LIST TO OBTAIN SECOND DATAFRAME DATA (ALL SEQUENCES)
            for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):

                # APPEND SECOND BLOCK SEQUENCES DATA INTO SECOND DATAFRAME DATA AUX
                try:
                    if sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1][index_biggest_sequence_length_from_blocks]:
                        second_dataframe_data_aux_list.append((sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1][index_biggest_sequence_length_from_blocks]))
                except IndexError:  # BIGGEST SEQUENCE LENGTH > SECOND BLOCK SEQUENCE LENGTH (APPEND NULL DATA)
                    second_dataframe_data_aux_list.append(None)

            # APPEND SECOND BLOCK SEQUENCE DATA INTO SECOND DATAFRAME
            second_dataframe_data_list.append(second_dataframe_data_aux_list)

            # CLEAR SECOND DATAFRAME DATA AUX LIST
            second_dataframe_data_aux_list = []

        # ESTIMATE FIRST DATAFRAME'S SIZE IN BYTES
        estimated_first_dataframe_size_in_bytes = estimate_dataframe_size_in_bytes(biggest_sequence_length_from_blocks,
                                                                                   first_dataframe_schema)

        # CREATE FIRST DATAFRAME
        first_dataframe = spark_session.createDataFrame(data=first_dataframe_data_list,
                                                        schema=first_dataframe_schema,
                                                        verifySchema=True)

        # CALCULATE FIRST DATAFRAME'S OPTIMIZED NUMBER OF PARTITIONS
        first_dataframe_number_of_partitions = \
            calculate_number_of_dataframe_partitions(spark_context, estimated_first_dataframe_size_in_bytes)

        # SET FIRST DATAFRAME'S CUSTOM REPARTITIONING
        repartitioning_first_dataframe_start_time = time.time()
        first_dataframe = first_dataframe.coalesce(first_dataframe_number_of_partitions)
        repartitioning_first_dataframe_end_time = time.time() - repartitioning_first_dataframe_start_time
        repartitioning_dataframes_duration_time_seconds = \
            repartitioning_dataframes_duration_time_seconds + repartitioning_first_dataframe_end_time

        # GET FIRST DATAFRAME'S INDEX
        first_dataframe_index = index_sequences_indices_blocks_list

        # GET FIRST DATAFRAME'S LENGTH
        first_dataframe_length = biggest_sequence_length_from_blocks

        # ESTIMATE SECOND DATAFRAME'S SIZE IN BYTES
        estimated_second_dataframe_size_in_bytes = estimate_dataframe_size_in_bytes(biggest_sequence_length_from_blocks,
                                                                                    second_dataframe_schema)

        # CREATE SECOND DATAFRAME
        second_dataframe = spark_session.createDataFrame(data=second_dataframe_data_list,
                                                         schema=second_dataframe_schema,
                                                         verifySchema=True)

        # CALCULATE SECOND DATAFRAME'S OPTIMIZED NUMBER OF PARTITIONS
        second_dataframe_number_of_partitions = \
            calculate_number_of_dataframe_partitions(spark_context, estimated_second_dataframe_size_in_bytes)

        # SET SECOND DATAFRAME'S CUSTOM REPARTITIONING
        repartitioning_second_dataframe_start_time = time.time()
        second_dataframe = second_dataframe.coalesce(second_dataframe_number_of_partitions)
        repartitioning_second_dataframe_end_time = time.time() - repartitioning_second_dataframe_start_time
        repartitioning_dataframes_duration_time_seconds = \
            repartitioning_dataframes_duration_time_seconds + repartitioning_second_dataframe_end_time

        # GET SECOND DATAFRAME'S INDEX
        second_dataframe_index = first_dataframe_index + 1

        # GET SECOND DATAFRAME'S LENGTH
        second_dataframe_length = biggest_sequence_length_from_blocks

        # EXECUTE SECOND IMPLEMENTATION'S DIFF OPERATION
        diff_start_time = time.time()
        diff_operation_resulting_dataframe = execute_second_implementation_diff_operation(first_dataframe,
                                                                                          second_dataframe)
        diff_end_time = time.time() - diff_start_time
        diff_operation_duration_time_seconds = diff_operation_duration_time_seconds + diff_end_time

        # INCREASE DIFF OPERATIONS COUNT
        diff_operations_count = diff_operations_count + 1

        # ESTIMATE DIFF OPERATION RESULTING DATAFRAME SIZE IN BYTES (HIGHEST SIZE POSSIBLE)
        highest_estimated_diff_operation_resulting_dataframe_size_in_bytes = \
            estimate_highest_diff_operation_resulting_dataframe_size_in_bytes(first_dataframe_length,
                                                                              first_dataframe_schema,
                                                                              second_dataframe_length,
                                                                              second_dataframe_schema)

        # CALCULATE DIFF OPERATION RESULTING DATAFRAME OPTIMIZED NUMBER OF PARTITIONS
        optimized_number_of_dataframe_partitions = \
            calculate_number_of_dataframe_partitions(spark_context,
                                                     highest_estimated_diff_operation_resulting_dataframe_size_in_bytes)

        # SET DIFF OPERATION RESULTING DATAFRAME'S CUSTOM REPARTITIONING
        repartitioning_diff_operation_resulting_dataframe_start_time = time.time()
        diff_operation_resulting_dataframe = \
            diff_operation_resulting_dataframe.coalesce(optimized_number_of_dataframe_partitions)
        repartitioning_diff_operation_resulting_dataframe_end_time = \
            time.time() - repartitioning_diff_operation_resulting_dataframe_start_time
        repartitioning_dataframes_duration_time_seconds = \
            repartitioning_dataframes_duration_time_seconds + repartitioning_diff_operation_resulting_dataframe_end_time

        # GET DIFF OPERATION RESULTING DATAFRAME'S NUMBER OF PARTITIONS
        diff_operation_resulting_dataframe_num_partitions = \
            get_dataframe_num_partitions(diff_operation_resulting_dataframe)

        # INCREASE RESULTING DATAFRAMES PARTITIONS COUNT
        resulting_dataframes_partitions_count = \
            resulting_dataframes_partitions_count + diff_operation_resulting_dataframe_num_partitions

        # COLLECT DIFF OPERATION RESULTING DATAFRAME (COLLECT OPERATION)
        collect_start_time = time.time()
        destination_file_path = \
            Path("{0}Result/{1}/Block_{2}_Diff_Block_{3}.csv"
                 .format(app_name, app_id, str(first_dataframe_index), str(second_dataframe_index)))
        collect_diff_operation_resulting_dataframe(diff_operation_resulting_dataframe,
                                                   collect_approach,
                                                   destination_file_path)
        collect_end_time = time.time() - collect_start_time
        collect_operation_duration_time_seconds = collect_operation_duration_time_seconds + collect_end_time

    # LOG TOTAL NUMBER OF DIFF OPERATIONS
    total_number_of_diff_operations_message = "({0}) Total Number of Diff Operations: {1}" \
        .format(app_name, str(diff_operations_count))
    logger.info(total_number_of_diff_operations_message)

    # LOG TOTAL NUMBER OF SPARK PARTITIONS
    total_number_of_spark_partitions_message = "({0}) Total Number of Spark Partitions: {1}" \
        .format(app_name, str(resulting_dataframes_partitions_count))
    logger.info(total_number_of_spark_partitions_message)

    # LOG REPARTITIONING DATAFRAMES DURATION TIME
    repartitioning_dataframes_duration_time_minutes = repartitioning_dataframes_duration_time_seconds / 60
    repartitioning_dataframes_duration_time__message = \
        "({0}) Repartitioning Dataframes Duration: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(repartitioning_dataframes_duration_time_seconds, 4)),
                str(round(repartitioning_dataframes_duration_time_minutes, 4)))
    logger.info(repartitioning_dataframes_duration_time__message)

    # LOG DIFF OPERATION DURATION TIME
    diff_operation_duration_time_minutes = diff_operation_duration_time_seconds / 60
    diff_operation_duration_time_message = \
        "({0}) Diff Dataframes Operation Duration (Transformation: Join): {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(diff_operation_duration_time_seconds, 4)),
                str(round(diff_operation_duration_time_minutes, 4)))
    logger.info(diff_operation_duration_time_message)

    # LOG COLLECT OPERATION DURATION TIME
    collect_description = None
    if collect_approach == "None":
        pass
    elif collect_approach == "ST":
        collect_description = "Show as Table Format Duration (Action: Show as Table Format)"
    elif collect_approach == "DW":
        collect_description = "Distributed Write Operation Duration (Action: Save as Multiple CSV Files)"
    elif collect_approach == "MW":
        collect_description = "Merged Write Operation Duration (Action: Save as Single CSV File)"
    collect_operation_duration_time_minutes = collect_operation_duration_time_seconds / 60
    if collect_description:
        collect_operation_duration_time_message = "({0}) {1}: {2} sec (≈ {3} min)" \
            .format(app_name,
                    collect_description,
                    str(round(collect_operation_duration_time_seconds, 4)),
                    str(round(collect_operation_duration_time_minutes, 4)))
        logger.info(collect_operation_duration_time_message)


# TODO: REFACTOR
def execute_diff_approach_1(spark_context: SparkContext,
                            dataframes_list: list,
                            app_id: str,
                            app_name: str,
                            logger: Logger) -> list:
    diff_result_list = []
    diff_operations_count = 0
    partitions_count = 0
    for first_dataframe_index in range(0, len(dataframes_list) - 1):
        df1 = dataframes_list[first_dataframe_index][0]
        df1_length = dataframes_list[first_dataframe_index][1]
        df1_schema = df1.schema
        df1_column_names = df1_schema.names
        for second_dataframe_index in range(first_dataframe_index + 1, len(dataframes_list)):
            destination_file = "{0}Result/{1}/Sequence_{2}_Diff_Sequence_{3}.csv" \
                .format(app_name, app_id, str(first_dataframe_index), str(second_dataframe_index))
            df2 = dataframes_list[second_dataframe_index][0]
            df2_length = dataframes_list[second_dataframe_index][1]
            df2_schema = df2.schema
            df2_column_names = df2_schema.names
            join_index_condition = df1["Index"] == df2["Index"]
            join_conditions_list = []
            for index in range(len(df2_column_names)):
                df1_column_name = "`" + df1_column_names[index] + "`"
                df2_column_name = "`" + df2_column_names[index] + "`"
                if df1_column_name.find("Seq_") != -1 and df2_column_name.find("Seq_") != -1:
                    join_conditions_list.append(df1[df1_column_name] != df2[df2_column_name])
            join_conditions = join_index_condition & reduce(lambda x, y: x | y, join_conditions_list)
            diff_result = df1.join(df2, join_conditions, "fullouter") \
                .sort(df1["Index"].asc_nulls_last(), df2["Index"].asc_nulls_last()) \
                .filter(df1["Index"].isNotNull() & df2["Index"].isNotNull()) \
                .drop(df2["Index"])
            # HIGHEST ESTIMATE DIFF RESULTING DATAFRAME SIZE
            highest_estimate_resulting_dataframe_after_diff_size_in_bytes = \
                estimate_highest_diff_operation_resulting_dataframe_size_in_bytes(df1_length,
                                                                                  df1_schema,
                                                                                  df2_length,
                                                                                  df2_schema)
            number_of_dataframe_partitions = \
                calculate_number_of_dataframe_partitions(spark_context,
                                                         highest_estimate_resulting_dataframe_after_diff_size_in_bytes)
            diff_result = diff_result.coalesce(number_of_dataframe_partitions)
            diff_result_number_of_spark_partitions_message = "({0}) {1} Diff {2}: {3} Partition(s)" \
                .format(app_name,
                        first_dataframe_index,
                        second_dataframe_index,
                        str(diff_result.rdd.getNumPartitions()))
            logger.info(diff_result_number_of_spark_partitions_message)
            partitions_count = partitions_count + diff_result.rdd.getNumPartitions()
            diff_result_list.append((diff_result, destination_file))
            diff_operations_count = diff_operations_count + 1
    number_of_diff_operations_message = "({0}) Total Number of Diff Operations: {1}" \
        .format(app_name, str(diff_operations_count))
    logger.info(number_of_diff_operations_message)
    number_of_spark_partitions_message = "({0}) Total Number of Spark Partitions: {1}" \
        .format(app_name, str(partitions_count))
    logger.info(number_of_spark_partitions_message)
    return diff_result_list


def show_dataframe(dataframe: DataFrame,
                   number_of_rows_to_show: int,
                   truncate_boolean: bool) -> None:
    dataframe.show(n=number_of_rows_to_show,
                   truncate=truncate_boolean)


def write_dataframe_as_distributed_partial_multiple_csv_files(dataframe: DataFrame,
                                                              destination_file_path: Path,
                                                              header_boolean: bool,
                                                              write_mode: str) -> None:
    dataframe.write.csv(path=str(destination_file_path),
                        header=header_boolean,
                        mode=write_mode)


def write_dataframe_as_merged_complete_single_csv_file(dataframe: DataFrame,
                                                       destination_file_path: Path,
                                                       header_boolean: bool,
                                                       write_mode: str) -> None:
    dataframe.coalesce(1).write.csv(path=str(destination_file_path),
                                    header=header_boolean,
                                    mode=write_mode)


def collect_diff_operation_resulting_dataframe(diff_operation_resulting_dataframe: DataFrame,
                                               collect_approach: str,
                                               destination_file_path: Path) -> None:
    if collect_approach == "None":
        # DO NOT COLLECT DIFF OPERATION RESULTING DATAFRAME
        pass
    elif collect_approach == "ST":  # ST = SHOW AS TABLE
        # COLLECT DIFF OPERATION RESULTING DATAFRAME AND SHOW AS TABLE FORMAT ON TERMINAL
        show_dataframe(diff_operation_resulting_dataframe,
                       diff_operation_resulting_dataframe.count(),
                       False)
    elif collect_approach == "DW":  # DW = DISTRIBUTED WRITE
        # COLLECT DIFF OPERATION RESULTING DATAFRAME AND WRITE TO DISK MULTIPLE CSV FILES (DISTRIBUTED PARTITIONS DATA)
        write_dataframe_as_distributed_partial_multiple_csv_files(diff_operation_resulting_dataframe,
                                                                  destination_file_path,
                                                                  True,
                                                                  "append")
    elif collect_approach == "MW":  # MW = MERGED WRITE
        # COLLECT DIFF OPERATION RESULTING DATAFRAME AND WRITE TO DISK SINGLE CSV FILE (MERGE DATA FROM ALL PARTITIONS)
        write_dataframe_as_merged_complete_single_csv_file(diff_operation_resulting_dataframe,
                                                           destination_file_path,
                                                           True,
                                                           "append")


def parse_collected_spark_job_metrics_counts_list(spark_job_metrics_counts_list: list,
                                                  app_name: str,
                                                  logger: Logger) -> None:
    # JOBS METRICS COUNTS
    jobs_metrics_counts_list = spark_job_metrics_counts_list[0]
    total_jobs = jobs_metrics_counts_list[0][1]
    succeeded_jobs = jobs_metrics_counts_list[1][1]
    running_jobs = jobs_metrics_counts_list[2][1]
    failed_jobs = jobs_metrics_counts_list[3][1]
    jobs_metrics_message = \
        "({0}) Total Number of Spark Jobs: {1} " \
        "(Succeeded: {2} | Running: {3} | Failed: {4})" \
        .format(app_name, total_jobs, succeeded_jobs, running_jobs, failed_jobs)
    logger.info(jobs_metrics_message)

    # TASKS METRICS COUNTS
    tasks_metrics_counts_list = spark_job_metrics_counts_list[1]
    total_tasks = tasks_metrics_counts_list[0][1]
    completed_tasks = tasks_metrics_counts_list[1][1]
    skipped_tasks = tasks_metrics_counts_list[2][1]
    active_tasks = tasks_metrics_counts_list[3][1]
    failed_tasks = tasks_metrics_counts_list[4][1]
    killed_tasks = tasks_metrics_counts_list[5][1]
    tasks_metrics_message = \
        "({0}) Total Number of Spark Tasks: {1} " \
        "(Completed: {2} | Skipped: {3} | Active: {4} | Failed: {5} | Killed: {6})" \
        .format(app_name, total_tasks, completed_tasks, skipped_tasks, active_tasks, failed_tasks, killed_tasks)
    logger.info(tasks_metrics_message)

    # STAGES METRICS COUNTS
    stages_metrics_counts_list = spark_job_metrics_counts_list[2]
    total_stages = stages_metrics_counts_list[0][1]
    complete_stages = stages_metrics_counts_list[1][1]
    skipped_stages = stages_metrics_counts_list[2][1]
    active_stages = stages_metrics_counts_list[3][1]
    pending_stages = stages_metrics_counts_list[4][1]
    failed_stages = stages_metrics_counts_list[5][1]
    stages_metrics_message = \
        "({0}) Total Number of Spark Stages: {1} " \
        "(Complete: {2} | Skipped: {3} | Active: {4} | Pending: {5} | Failed: {6})" \
        .format(app_name, total_stages, complete_stages, skipped_stages, active_stages, pending_stages, failed_stages)
    logger.info(stages_metrics_message)


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
    load_diff_sequences_parameters(dsp, parsed_parameters_dictionary)

    # VALIDATE DIFF SEQUENCES PARAMETERS
    validate_diff_sequences_parameters(dsp)

    # CONFIGURE LOGGING
    set_logger_basic_config(dsp.logging_file_path)
    logger = getLogger()

    # START DIFF SEQUENCES SPARK
    dss = DiffSequencesSpark()
    start_diff_sequences_spark(dss, parsed_parameters_dictionary, logger)

    # GENERATE SEQUENCES LIST
    sequences_list = generate_sequences_list(dsp.sequences_path_list_text_file_path,
                                             dss.app_name,
                                             logger)

    # EXECUTE DIFF SEQUENCES IMPLEMENTATION
    if dsp.diff_approach == 1:  # EXECUTE FIRST IMPLEMENTATION
        execute_first_implementation(dss.spark_session,
                                     dss.spark_context,
                                     dss.app_id,
                                     dss.app_name,
                                     dsp.collect_approach,
                                     sequences_list,
                                     logger)
    elif dsp.diff_approach == 2:  # EXECUTE SECOND IMPLEMENTATION
        execute_second_implementation(dss.spark_session,
                                      dss.spark_context,
                                      dss.app_id,
                                      dss.app_name,
                                      dsp.collect_approach,
                                      sequences_list,
                                      logger)

    # COLLECT SPARK JOB METRICS COUNTS LIST
    collect_metrics_start_time = time.time()
    spark_driver_host = get_spark_driver_host(dss.spark_context)
    spark_app_id = get_spark_app_id(dss.spark_context)
    spark_ui_port = get_spark_ui_port(dss.spark_context)
    spark_job_metrics_counts_list = get_spark_job_metrics_counts_list(spark_driver_host,
                                                                      dss.app_name,
                                                                      spark_app_id,
                                                                      spark_ui_port)

    # PARSE COLLECTED SPARK JOB METRICS COUNTS LIST
    parse_collected_spark_job_metrics_counts_list(spark_job_metrics_counts_list,
                                                  dss.app_name,
                                                  logger)

    # LOG COLLECT SPARK JOB METRICS DURATION TIME
    collect_metrics_duration_time_seconds = time.time() - collect_metrics_start_time
    collect_metrics_duration_time_minutes = collect_metrics_duration_time_seconds / 60
    collect_metrics_duration_time_message = \
        "({0}) Collect Spark Job Metrics Duration: {1} sec (≈ {2} min)" \
        .format(dss.app_name,
                str(round(collect_metrics_duration_time_seconds, 4)),
                str(round(collect_metrics_duration_time_minutes, 4)))
    logger.info(collect_metrics_duration_time_message)

    # STOP DIFF SEQUENCES SPARK
    stop_diff_sequences_spark(dss, logger)

    # END
    app_end_time = time.time()
    get_total_elapsed_time(dss.app_name, app_start_time, app_end_time, logger)
    print("Application Finished Successfully!")
    sys.exit(0)


if __name__ == "__main__":
    diff(sys.argv)
