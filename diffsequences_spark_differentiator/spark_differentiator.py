from configparser import ConfigParser
from spark_differentiator_exceptions import *
from spark_differentiator_job_metrics import get_spark_job_metrics_counts_list
from functools import reduce
from logging import basicConfig, getLogger, INFO, Logger
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
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
        self.sequences_path_list_text_file = None
        self.diff_approach = None
        self.collect_approach = None


def set_logger_basic_config() -> None:
    basicConfig(filename="logging.log",
                format="%(asctime)s %(message)s",
                level=INFO)


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("spark_differentiator.dict", number_of_arguments_provided - 1)
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
    config_parser.read_dict(parameters_dictionary)
    return config_parser


def load_diff_sequences_parameters(dsp: DiffSequencesParameters,
                                   parsed_parameters_dictionary: dict) -> None:
    # READ FASTA SEQUENCES PATH LIST TEXT FILE
    dsp.sequences_path_list_text_file = \
        Path(str(parsed_parameters_dictionary["DiffSequencesParameters"]["sequences_path_list_text_file"]))

    # READ DIFF APPROACH
    dsp.diff_approach = int(parsed_parameters_dictionary["DiffSequencesParameters"]["diff_approach"])

    # READ COLLECT APPROACH
    dsp.collect_approach = str(parsed_parameters_dictionary["DiffSequencesParameters"]["collect_approach"])


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


def get_spark_executors_list(spark_context: SparkContext) -> list:
    return [executor.host() for executor in spark_context._jsc.sc().statusTracker().getExecutorInfos()]


def get_spark_executors_count(spark_context: SparkContext) -> int:
    return len(get_spark_executors_list(spark_context))


def get_spark_cores_per_executor(spark_context: SparkContext) -> int:
    return get_spark_cores_max_count(spark_context) * get_spark_executors_count(spark_context)


def get_spark_executor_memory(spark_context: SparkContext) -> str:
    return spark_context.getConf().get("spark.executor.memory")


def get_spark_max_partition_size_in_bytes(spark_context: SparkContext) -> int:
    return int(spark_context.getConf().get("spark.sql.files.maxpartitionbytes"))


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
                            logger: Logger) -> list:
    # GENERATE SEQUENCES LIST
    read_sequences_start = time.time()
    parsed_sequences_list = parse_sequences_list(sequences_path_list_text_file)
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
        .format(app_name, str(round(generate_dataframes_seconds, 4)), str(round(generate_dataframes_minutes, 4)))
    logger.info(generate_dataframes_list_message)
    return dataframes_list


def estimate_highest_resulting_dataframe_after_diff_size_in_bytes(df1_length: int,
                                                                  df1_schema: StructType,
                                                                  df2_length: int,
                                                                  df2_schema: StructType) -> int:
    longtype_count = 0
    longtype_default_size = 8  # LongType(): 8 Bytes
    stringtype_count = 0
    stringtype_default_size = 4  # StringType(): 4 Bytes + (1 Byte * String Length)
    df1_schema_list = [[field.dataType, field.name] for field in df1_schema.fields]
    for data in df1_schema_list:
        if data[0] == LongType():
            longtype_count = longtype_count + 1
        elif data[0] == StringType():
            stringtype_count = stringtype_count + 1
    df2_schema_list = [[field.dataType, field.name] for field in df2_schema.fields]
    for data in df2_schema_list:
        if data[0] == LongType():
            longtype_count = longtype_count + 1
        elif data[0] == StringType():
            stringtype_count = stringtype_count + 1
    longtype_count = longtype_count - 1  # Removing one "Index" column count (df2.Index will be dropped after join)
    min_df_length = min(df1_length, df2_length)
    longtype_size_one_row = longtype_count * longtype_default_size
    stringtype_size_one_row = stringtype_count * (stringtype_default_size + 1)
    return min_df_length * (longtype_size_one_row + stringtype_size_one_row)


# TODO: REFACTOR
def execute_diff_approach_1(spark_context: SparkContext,
                            dataframes_list: list,
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
            destination_file = "{0}Result/Sequence_{1}_Diff_Sequence_{2}.csv" \
                .format(app_name, str(first_dataframe_index), str(second_dataframe_index))
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
                estimate_highest_resulting_dataframe_after_diff_size_in_bytes(df1_length,
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


# TODO: REFACTOR
def execute_diff_approach_2(spark_context: SparkContext,
                            dataframes_list: list,
                            app_name: str,
                            logger: Logger) -> list:
    diff_result_list = []
    diff_operations_count = 0
    partitions_count = 0
    for dataframe_index in range(0, len(dataframes_list), 2):
        first_dataframe_index = dataframe_index
        second_dataframe_index = dataframe_index + 1
        destination_file = "{0}Result/Block_{1}_Diff_Block_{2}.csv" \
            .format(app_name, str(first_dataframe_index), str(second_dataframe_index))
        df1 = dataframes_list[first_dataframe_index][0]
        df1_length = dataframes_list[first_dataframe_index][1]
        df1_schema = df1.schema
        df1_column_names = df1_schema.names
        df2 = dataframes_list[second_dataframe_index][0]
        df2_length = dataframes_list[second_dataframe_index][1]
        df2_schema = df2.schema
        df2_column_names = df2_schema.names
        join_index_condition = df1["Index"] == df2["Index"]
        join_conditions_list = []
        for index_df2_column_names in range(len(df2_column_names)):
            df2_column_name = "`" + df2_column_names[index_df2_column_names] + "`"
            for index_df1_column_names in range(len(df1_column_names)):
                df1_column_name = "`" + df1_column_names[index_df1_column_names] + "`"
                if df1_column_name.find("Seq_") != -1 and df2_column_name.find("Seq_") != -1:
                    join_conditions_list.append(df1[df1_column_name] != df2[df2_column_name])
        join_conditions = join_index_condition & reduce(lambda x, y: x | y, join_conditions_list)
        diff_result = df1.join(df2, join_conditions, "fullouter") \
            .sort(df1["Index"].asc_nulls_last(), df2["Index"].asc_nulls_last()) \
            .filter(df1["Index"].isNotNull() & df2["Index"].isNotNull()) \
            .drop(df2["Index"])
        # CHANGE NON-DIFF LINE VALUES TO "="
        df1_sequence_identification_column_quoted = "`" + df1_column_names[1] + "`"
        new_value = "="
        diff_result_df2_columns_only_list = [column for column in diff_result.columns if column not in df1_column_names]
        for df2_column in diff_result_df2_columns_only_list:
            df2_column_quoted = "`" + df2_column + "`"
            column_expression = when(col(df2_column_quoted) == diff_result[df1_sequence_identification_column_quoted],
                                     new_value) \
                .otherwise(col(df2_column_quoted))
            diff_result = diff_result.withColumn(df2_column, column_expression)
        # HIGHEST ESTIMATE DIFF RESULTING DATAFRAME SIZE
        highest_estimate_resulting_dataframe_after_diff_size_in_bytes = \
            estimate_highest_resulting_dataframe_after_diff_size_in_bytes(df1_length,
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


def differentiate_dataframes_list(spark_context: SparkContext,
                                  app_name: str,
                                  diff_approach: int,
                                  dataframes_list: list,
                                  logger: Logger) -> list:
    # DIFFERENTIATE DATAFRAMES LIST
    diff_start = time.time()
    diff_result_list = []
    if diff_approach == 1:
        diff_result_list = execute_diff_approach_1(spark_context,
                                                   dataframes_list,
                                                   app_name,
                                                   logger)
    elif diff_approach == 2:
        diff_result_list = execute_diff_approach_2(spark_context,
                                                   dataframes_list,
                                                   app_name,
                                                   logger)
    diff_end = time.time()
    diff_seconds = diff_end - diff_start
    diff_minutes = diff_seconds / 60
    diff_dataframes_list_duration_message = \
        "({0}) Diff Dataframes List Duration (Transformation: Join): {1} sec (≈ {2} min)" \
        .format(app_name, str(round(diff_seconds, 4)), str(round(diff_minutes, 4)))
    logger.info(diff_dataframes_list_duration_message)
    return diff_result_list


def show_whole_dataframe(diff_result_list: list) -> None:
    for index in range(len(diff_result_list)):
        diff_result_list[index][0].show(diff_result_list[index][0].count(), truncate=False)


def write_dataframe_to_csv_file(diff_result_list: list,
                                collect_approach: str) -> None:
    if collect_approach == "WDM":
        # SAVE AS MULTIPLE CSV PART FILES (DISTRIBUTED PARTITIONS DATA)
        for index in range(len(diff_result_list)):
            diff_result_list[index][0].write.option("header", True).csv(diff_result_list[index][1])
    elif collect_approach == "WDS":
        # SAVE AS SINGLE CSV FILE (MERGE DATA FROM ALL PARTITIONS)
        for index in range(len(diff_result_list)):
            diff_result_list[index][0].coalesce(1).write.option("header", True).csv(diff_result_list[index][1])


def collect_diff_result_list(diff_result_list: list,
                             app_name: str,
                             collect_approach: str,
                             logger: Logger) -> None:
    if collect_approach == "None":
        # DO NOT COLLECT DIFFERENTIATE RESULT LIST
        pass
    elif collect_approach == "OT":
        # COLLECT DIFFERENTIATE RESULT LIST AND OUTPUT TO TERMINAL (OT)
        show_start = time.time()
        show_whole_dataframe(diff_result_list)
        show_end = time.time()
        show_seconds = show_end - show_start
        show_minutes = show_seconds / 60
        show_dataframes_list_duration_message = \
            "({0}) Show Dataframes List Duration (Action: Show the result in a table format): {1} sec (≈ {2} min)" \
            .format(app_name, str(round(show_seconds, 4)), str(round(show_minutes, 4)))
        logger.info(show_dataframes_list_duration_message)
    elif collect_approach == "WDM":
        # COLLECT DIFFERENTIATE RESULT LIST AND WRITE TO DISK (WD) MULTIPLE CSV PART FILES (DISTRIBUTED PARTITIONS DATA)
        write_start = time.time()
        write_dataframe_to_csv_file(diff_result_list, collect_approach)
        write_end = time.time()
        write_seconds = write_end - write_start
        write_minutes = write_seconds / 60
        write_dataframes_list_duration_message = \
            "({0}) Write Dataframes List Duration (Action: Save as Multiple CSV Files): {1} sec (≈ {2} min)" \
            .format(app_name, str(round(write_seconds, 4)), str(round(write_minutes, 4)))
        logger.info(write_dataframes_list_duration_message)
    elif collect_approach == "WDS":
        # COLLECT DIFFERENTIATE RESULT LIST AND WRITE TO DISK (WD) SINGLE CSV FILE (MERGE DATA FROM ALL PARTITIONS)
        write_start = time.time()
        write_dataframe_to_csv_file(diff_result_list, collect_approach)
        write_end = time.time()
        write_seconds = write_end - write_start
        write_minutes = write_seconds / 60
        write_dataframes_list_duration_message = \
            "({0}) Write Dataframes List Duration (Action: Save as Single CSV File): {1} sec (≈ {2} min)" \
            .format(app_name, str(round(write_seconds, 4)), str(round(write_minutes, 4)))
        logger.info(write_dataframes_list_duration_message)


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

    # CONFIGURE LOGGING
    set_logger_basic_config()
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
    load_diff_sequences_parameters(dsp, parsed_parameters_dictionary)

    # START DIFF SEQUENCES SPARK
    dss = DiffSequencesSpark()
    start_diff_sequences_spark(dss, parsed_parameters_dictionary, logger)

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

    # DIFFERENTIATE DATAFRAMES LIST
    diff_result_list = differentiate_dataframes_list(dss.spark_context,
                                                     dss.app_name,
                                                     dsp.diff_approach,
                                                     dataframes_list,
                                                     logger)

    # COLLECT DIFFERENTIATE RESULT LIST
    collect_diff_result_list(diff_result_list, dss.app_name, dsp.collect_approach, logger)

    # COLLECT SPARK JOB METRICS COUNTS LIST
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

    # STOP DIFF SEQUENCES SPARK
    stop_diff_sequences_spark(dss, logger)

    # END
    app_end_time = time.time()
    get_total_elapsed_time(dss.app_name, app_start_time, app_end_time, logger)
    print("Application Finished Successfully!")
    sys.exit(0)


if __name__ == "__main__":
    diff(sys.argv)
