from configparser import ConfigParser
from differentiator_exceptions import *
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructType
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
        self.diff_approach = None


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

    # READ DIFF APPROACH
    dsp.diff_approach = int(parsed_parameters_dictionary["DiffSequences"]["diff_approach"])


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


def get_spark_max_partition_size_in_bytes(spark_context: SparkContext) -> int:
    is_running_locally_non_cluster = check_if_is_running_locally_non_cluster(spark_context)
    if is_running_locally_non_cluster:
        return 134217728  # (128 MB)
    else:
        return int(spark_context.getConf().get("spark.sql.files.maxPartitionBytes"))


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
    parsed_sequences_list = parse_sequences_list(sequences_path_list_text_file)
    read_sequences_end = time.time()
    read_sequences_seconds = read_sequences_end - read_sequences_start
    read_sequences_minutes = read_sequences_seconds / 60
    generate_sequences_list_duration_message = "({0}) Generate Sequences List Duration: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(read_sequences_seconds, 4)),
                str(round(read_sequences_minutes, 4)))
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
    divider = total_cores_count
    while True:
        partitions_size = dataframe_size_in_bytes / divider
        if partitions_size < max_partition_size:
            number_of_dataframe_partitions = divider
            break
        divider = divider + 1
    return number_of_dataframe_partitions


# TODO: REFACTOR
def generate_dataframes_list_from_sequences_list(spark_session: SparkSession,
                                                 spark_context: SparkContext,
                                                 sequences_list: list) -> list:
    dataframes_list = []
    dataframe_index = 0
    for sequence_index in range(len(sequences_list)):
        dataframe_data = []
        index_label = "Index"
        sequence_identification = sequences_list[sequence_index][0]
        if sequence_identification != "Seq":
            char_label = "Seq_" + sequence_identification
        else:
            char_label = "Seq_" + str(dataframe_index)
        sequence_data = sequences_list[sequence_index][1]
        sequence_length = len(sequence_data)
        for index in range(sequence_length):
            dataframe_data.append((index, sequence_data[index]))
        dataframe_schema = StructType() \
            .add(index_label, LongType(), nullable=False) \
            .add(char_label, StringType(), nullable=False)
        estimated_dataframe_size_in_bytes = estimate_dataframe_size_in_bytes(sequence_length,
                                                                             dataframe_schema)
        dataframe = spark_session.createDataFrame(data=dataframe_data,
                                                  schema=dataframe_schema,
                                                  verifySchema=True)
        number_of_dataframe_partitions = calculate_number_of_dataframe_partitions(spark_context,
                                                                                  estimated_dataframe_size_in_bytes)
        dataframe = dataframe.coalesce(number_of_dataframe_partitions)
        dataframes_list.append((dataframe, sequence_length))
        dataframe_index = dataframe_index + 1
    return dataframes_list


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


def get_biggest_sequence_length_from_blocks(sequences_list: list,
                                            first_block_sequences_indices_list: list,
                                            second_block_sequences_indices_list: list) -> int:
    biggest_sequence_length_from_blocks = 0
    first_block_sequence_length = len(sequences_list[first_block_sequences_indices_list[0]][1])
    if biggest_sequence_length_from_blocks < first_block_sequence_length:
        biggest_sequence_length_from_blocks = first_block_sequence_length
    for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):
        second_block_index_sequence_length = \
            len(sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1])
        if biggest_sequence_length_from_blocks < second_block_index_sequence_length:
            biggest_sequence_length_from_blocks = second_block_index_sequence_length
    return biggest_sequence_length_from_blocks


# TODO: REFACTOR
def generate_dataframes_list_from_sequences_index_blocks_list(spark_session: SparkSession,
                                                              spark_context: SparkContext,
                                                              sequences_list: list) -> list:
    dataframes_list = []
    sequences_indices_blocks_list = generate_sequences_indices_blocks_list(len(sequences_list))
    for index_sequences_indices_blocks_list in range(len(sequences_indices_blocks_list)):
        first_block_sequences_indices_list = sequences_indices_blocks_list[index_sequences_indices_blocks_list][0]
        second_block_sequences_indices_list = sequences_indices_blocks_list[index_sequences_indices_blocks_list][1]
        biggest_sequence_length_from_blocks = \
            get_biggest_sequence_length_from_blocks(sequences_list,
                                                    first_block_sequences_indices_list,
                                                    second_block_sequences_indices_list)
        first_dataframe_data = []
        first_dataframe_index_label = "Index"
        first_block_sequence_identification = sequences_list[first_block_sequences_indices_list[0]][0]
        if first_block_sequence_identification != "Seq":
            first_dataframe_char_label = "Seq_" + first_block_sequence_identification
        else:
            first_dataframe_char_label = "Seq_" + "0"
        first_dataframe_schema = StructType() \
            .add(first_dataframe_index_label, LongType(), nullable=False) \
            .add(first_dataframe_char_label, StringType(), nullable=True)
        second_dataframe_data = []
        second_dataframe_data_aux = []
        second_dataframe_index_label = "Index"
        second_dataframe_schema = StructType() \
            .add(second_dataframe_index_label, LongType(), nullable=False)
        for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):
            second_block_sequence_identification = \
                sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][0]
            if second_block_sequence_identification != "Seq":
                second_dataframe_char_label = "Seq_" + second_block_sequence_identification
            else:
                second_dataframe_char_label = "Seq_" + str(index_second_block_sequences_indices_list + 1)
            second_dataframe_schema.add(second_dataframe_char_label, StringType(), nullable=True)
        for index_biggest_sequence_length_from_blocks in range(biggest_sequence_length_from_blocks):
            try:
                if sequences_list[first_block_sequences_indices_list[0]][1][index_biggest_sequence_length_from_blocks]:
                    first_dataframe_data.append((index_biggest_sequence_length_from_blocks, sequences_list[first_block_sequences_indices_list[0]][1][index_biggest_sequence_length_from_blocks]))
            except IndexError:
                first_dataframe_data.append((index_biggest_sequence_length_from_blocks, None))
            second_dataframe_data_aux.append(index_biggest_sequence_length_from_blocks)
            for index_second_block_sequences_indices_list in range(len(second_block_sequences_indices_list)):
                try:
                    if sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1][index_biggest_sequence_length_from_blocks]:
                        second_dataframe_data_aux.append((sequences_list[second_block_sequences_indices_list[index_second_block_sequences_indices_list]][1][index_biggest_sequence_length_from_blocks]))
                except IndexError:
                    second_dataframe_data_aux.append(None)
            second_dataframe_data.append(second_dataframe_data_aux)
            second_dataframe_data_aux = []
        estimated_first_dataframe_size_in_bytes = \
            estimate_dataframe_size_in_bytes(biggest_sequence_length_from_blocks, first_dataframe_schema)
        first_dataframe = spark_session.createDataFrame(data=first_dataframe_data,
                                                        schema=first_dataframe_schema,
                                                        verifySchema=True)
        first_dataframe_number_of_partitions = \
            calculate_number_of_dataframe_partitions(spark_context, estimated_first_dataframe_size_in_bytes)
        first_dataframe = first_dataframe.coalesce(first_dataframe_number_of_partitions)
        dataframes_list.append((first_dataframe, biggest_sequence_length_from_blocks))
        estimated_second_dataframe_size_in_bytes = \
            estimate_dataframe_size_in_bytes(biggest_sequence_length_from_blocks, second_dataframe_schema)
        second_dataframe = spark_session.createDataFrame(data=second_dataframe_data,
                                                         schema=second_dataframe_schema,
                                                         verifySchema=True)
        second_dataframe_number_of_partitions = \
            calculate_number_of_dataframe_partitions(spark_context, estimated_second_dataframe_size_in_bytes)
        second_dataframe = second_dataframe.coalesce(second_dataframe_number_of_partitions)
        dataframes_list.append((second_dataframe, biggest_sequence_length_from_blocks))
    return dataframes_list


def generate_dataframes_list(spark_session: SparkSession,
                             spark_context: SparkContext,
                             app_name: str,
                             diff_approach: int,
                             sequences_list: list,
                             logger: Logger) -> list:
    # GENERATE DATAFRAMES LIST
    generate_dataframes_start = time.time()
    dataframes_list = []
    if diff_approach == 1:
        dataframes_list = generate_dataframes_list_from_sequences_list(spark_session,
                                                                       spark_context,
                                                                       sequences_list)
    elif diff_approach == 2:
        dataframes_list = generate_dataframes_list_from_sequences_index_blocks_list(spark_session,
                                                                                    spark_context,
                                                                                    sequences_list)
    generate_dataframes_end = time.time()
    generate_dataframes_seconds = generate_dataframes_end - generate_dataframes_start
    generate_dataframes_minutes = generate_dataframes_seconds / 60
    generate_dataframes_list_message = "({0}) Generate Dataframes List Duration: {1} sec (≈ {2} min)" \
        .format(app_name,
                str(round(generate_dataframes_seconds, 4)),
                str(round(generate_dataframes_minutes, 4)))
    logger.info(generate_dataframes_list_message)
    return dataframes_list


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

    # GENERATE DATAFRAMES LIST
    dataframes_list = generate_dataframes_list(dss.spark_session,
                                               dss.spark_context,
                                               dss.app_name,
                                               dsp.diff_approach,
                                               sequences_list,
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
