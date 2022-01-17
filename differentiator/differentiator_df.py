from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from differentiator.exception.differentiator_df_exceptions import *
from functools import reduce
from logging import Logger
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType, StringType, StructType
from sequences_handler.sequences_handler import SequencesHandler
from time import time
from typing import Union


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


class DataFrameDifferentiator(Differentiator):

    def __init__(self) -> None:
        super().__init__()
        self.max_DF = None
        self.partitioning = None

    @staticmethod
    def __read_max_DF(differentiator_config_file: Path,
                      differentiator_config_parser: ConfigParser) -> Union[int, str]:
        exception_message = "{0}: 'max_DF' must be a integer value in range [1, N-1]!" \
            .format(differentiator_config_file)
        try:
            max_DF = str(differentiator_config_parser.get("DataFrames Settings",
                                                          "max_DF"))
            if max_DF != "N-1":
                max_DF = int(max_DF)
        except ValueError:
            raise InvalidMaxDFError(exception_message)
        return max_DF

    @staticmethod
    def __validate_max_DF(max_DF: Union[int, str]) -> None:
        exception_message = "Multiple Sequences DataFrames must have at least 1 sequence."
        if max_DF == "N-1":
            pass
        else:
            if max_DF < 1:
                raise InvalidMaxDFError(exception_message)

    def __set_max_DF(self,
                     N: int,
                     max_DF: Union[int, str]) -> None:
        if max_DF == "N-1":
            self.max_DF = N - 1
        else:
            self.max_DF = max_DF

    @staticmethod
    def __log_max_DF(spark_app_name: str,
                     max_DF: int,
                     logger: Logger) -> None:
        maximum_sequences_per_dataframe_message = "({0}) Maximum Sequences Per DataFrame (max_DF): {1}" \
            .format(spark_app_name,
                    str(max_DF))
        print(maximum_sequences_per_dataframe_message)
        logger.info(maximum_sequences_per_dataframe_message)

    def __get_max_DF(self) -> int:
        return self.max_DF

    @staticmethod
    def __read_partitioning(differentiator_config_file: Path,
                            differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'partitioning' must be a string value!" \
            .format(differentiator_config_file)
        try:
            partitioning = \
                str(differentiator_config_parser.get("DataFrames Settings",
                                                     "partitioning"))
        except ValueError:
            raise InvalidPartitioningError(exception_message)
        return partitioning

    @staticmethod
    def __validate_partitioning(partitioning: str) -> None:
        supported_partitioning = ["auto", "custom"]
        exception_message = "Supported partitioning: {0}" \
            .format(" | ".join(supported_partitioning))
        if partitioning not in supported_partitioning:
            raise InvalidPartitioningError(exception_message)

    def __set_partitioning(self,
                           partitioning: str) -> None:
        self.partitioning = partitioning

    @staticmethod
    def __log_partitioning(spark_app_name: str,
                           partitioning: str,
                           logger: Logger) -> None:
        partitioning_message = "({0}) Partitioning: {1}" \
            .format(spark_app_name,
                    partitioning.capitalize())
        print(partitioning_message)
        logger.info(partitioning_message)

    def __get_partitioning(self) -> str:
        return self.partitioning

    @staticmethod
    def __generate_dataframe_schema_struct_list(dataframe_sequences_data_list: list) -> list:
        dataframe_schema_struct_list = []
        dataframe_index_label = "Index"
        dataframe_schema_struct_list.append([dataframe_index_label,
                                             LongType(),
                                             False])
        for index_dataframe_sequences_data_list in range(len(dataframe_sequences_data_list)):
            dataframe_sequence_identification = \
                dataframe_sequences_data_list[index_dataframe_sequences_data_list][0]
            if dataframe_sequence_identification != "Seq":
                dataframe_char_label = "Seq_" + dataframe_sequence_identification
            else:
                dataframe_char_label = "Seq_" + str(index_dataframe_sequences_data_list + 1)
            dataframe_schema_struct_list.append([dataframe_char_label,
                                                 StringType(),
                                                 True])
        return dataframe_schema_struct_list

    @staticmethod
    def __create_dataframe_schema(dataframe_schema_struct_list: list) -> StructType:
        dataframe_schema = StructType()
        for index_dataframe_schema_struct_list in range(len(dataframe_schema_struct_list)):
            dataframe_schema.add(field=dataframe_schema_struct_list[index_dataframe_schema_struct_list][0],
                                 data_type=dataframe_schema_struct_list[index_dataframe_schema_struct_list][1],
                                 nullable=dataframe_schema_struct_list[index_dataframe_schema_struct_list][2])
        return dataframe_schema

    @staticmethod
    def __get_dataframe_schema_column_names(dataframe_schema: StructType) -> list:
        return dataframe_schema.names

    @staticmethod
    def __create_dataframe(spark_session: SparkSession,
                           dataframe_data: list,
                           dataframe_schema: StructType) -> DataFrame:
        return spark_session.createDataFrame(data=dataframe_data,
                                             schema=dataframe_schema,
                                             verifySchema=True)

    @staticmethod
    def __repartition_dataframe(dataframe: DataFrame,
                                new_number_of_partitions: int) -> DataFrame:
        current_dataframe_num_partitions = dataframe.rdd.getNumPartitions()
        if current_dataframe_num_partitions > new_number_of_partitions:
            # Execute Coalesce (Spark Less-Wide-Shuffle Transformation) Function
            dataframe = dataframe.coalesce(new_number_of_partitions)
        if current_dataframe_num_partitions < new_number_of_partitions:
            # Execute Repartition (Spark Wider-Shuffle Transformation) Function
            dataframe = dataframe.repartition(new_number_of_partitions)
        return dataframe

    def __apply_customized_partitioning_after_dataframe_creation(self,
                                                                 partitioning: str,
                                                                 spark_app_cores_max_count: int,
                                                                 spark_recommended_tasks_per_cpu: int,
                                                                 dataframe: DataFrame) -> DataFrame:
        if partitioning == "custom":
            dataframe_optimized_number_of_partitions = spark_app_cores_max_count * spark_recommended_tasks_per_cpu
            dataframe = self.__repartition_dataframe(dataframe,
                                                     dataframe_optimized_number_of_partitions)
        return dataframe

    @staticmethod
    def __assemble_join_conditions(first_dataframe: DataFrame,
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

    @staticmethod
    def __execute_dataframes_diff(first_dataframe: DataFrame,
                                  second_dataframe: DataFrame,
                                  join_conditions) -> DataFrame:
        # Execute Full Outer Join (Spark Wider-Shuffle Transformation),
        #         Filter (Spark Narrow Transformation) and
        #         Drop (Spark Narrow Transformation) Functions
        df_r = first_dataframe \
            .join(second_dataframe, join_conditions, "full_outer") \
            .filter(first_dataframe["Index"].isNotNull() & second_dataframe["Index"].isNotNull()) \
            .drop(second_dataframe["Index"])
        return df_r

    @staticmethod
    def __substitute_equal_nucleotide_letters_on_df_r(diff_phase: str,
                                                      df_r: DataFrame,
                                                      first_dataframe_column_names: list) -> DataFrame:
        if diff_phase == "opt":
            # Update Non-Diff Line Values to "=" Character (For Better Viewing)
            first_dataframe_nucleotide_letter_column_quoted = "`" + first_dataframe_column_names[1] + "`"
            first_dataframe_nucleotide_letter_column_new_value = "="
            df_r_second_dataframe_columns_only_list = \
                [column for column in df_r.columns if column not in first_dataframe_column_names]
            for second_dataframe_column in df_r_second_dataframe_columns_only_list:
                second_dataframe_column_quoted = "`" + second_dataframe_column + "`"
                is_non_diff_column_comparison = col(second_dataframe_column_quoted) == \
                    df_r[first_dataframe_nucleotide_letter_column_quoted]
                column_expression = when(is_non_diff_column_comparison,
                                         first_dataframe_nucleotide_letter_column_new_value) \
                    .otherwise(col(second_dataframe_column_quoted))
                df_r = df_r.withColumn(second_dataframe_column,
                                       column_expression)
        return df_r

    @staticmethod
    def __estimate_highest_df_r_size_in_bytes(first_dataframe_schema: StructType,
                                              first_dataframe_num_rows: int,
                                              second_dataframe_schema: StructType,
                                              second_dataframe_num_rows: int) -> int:
        long_type_count = 0
        long_type_default_size = 8  # LongType(): 8 Bytes Each
        string_type_count = 0
        string_type_default_size = 4  # StringType(): 4 Bytes Each + (1 Byte * String Length)
        first_dataframe_schema_list = [[field.dataType, field.name] for field in first_dataframe_schema.fields]
        for schema_field_list in first_dataframe_schema_list:
            if schema_field_list[0] == LongType():
                long_type_count = long_type_count + 1
            elif schema_field_list[0] == StringType():
                string_type_count = string_type_count + 1
        second_dataframe_schema_list = [[field.dataType, field.name] for field in second_dataframe_schema.fields]
        for schema_field_list in second_dataframe_schema_list:
            if schema_field_list[0] == LongType():
                long_type_count = long_type_count + 1
            elif schema_field_list[0] == StringType():
                string_type_count = string_type_count + 1
        long_type_count = long_type_count - 1  # Discounting Index of Second DataFrame (Dropped After Join)
        minimum_dataframe_num_rows = min(first_dataframe_num_rows, second_dataframe_num_rows)
        long_type_size_one_row = long_type_count * long_type_default_size
        string_type_size_one_row = string_type_count * (string_type_default_size + 1)
        return minimum_dataframe_num_rows * (long_type_size_one_row + string_type_size_one_row)

    @staticmethod
    def __get_optimal_num_of_partitions_after_dataframe_shuffling(spark_max_recommended_partition_size: int,
                                                                  spark_app_cores_max_count: int,
                                                                  spark_recommended_tasks_per_cpu: int,
                                                                  dataframe_size_in_bytes: int) -> int:
        # Set Initial Divider Variable Value
        divider = spark_app_cores_max_count * spark_recommended_tasks_per_cpu
        # Search Optimized Number of Partitions
        while True:
            if (dataframe_size_in_bytes / divider) <= spark_max_recommended_partition_size:
                return divider
            divider = divider + 1

    def __apply_customized_partitioning_after_dataframes_diff(self,
                                                              partitioning: str,
                                                              first_dataframe_schema: StructType,
                                                              first_dataframe_num_rows: int,
                                                              second_dataframe_schema: StructType,
                                                              second_dataframe_num_rows: int,
                                                              spark_max_recommended_partition_size: int,
                                                              spark_app_cores_max_count: int,
                                                              spark_recommended_tasks_per_cpu: int,
                                                              df_r: DataFrame) -> DataFrame:
        if partitioning == "custom":
            estimated_df_r_size_in_bytes = self.__estimate_highest_df_r_size_in_bytes(first_dataframe_schema,
                                                                                      first_dataframe_num_rows,
                                                                                      second_dataframe_schema,
                                                                                      second_dataframe_num_rows)
            optimized_num_of_dataframe_partitions = \
                self.__get_optimal_num_of_partitions_after_dataframe_shuffling(spark_max_recommended_partition_size,
                                                                               spark_app_cores_max_count,
                                                                               spark_recommended_tasks_per_cpu,
                                                                               estimated_df_r_size_in_bytes)
            df_r = self.__repartition_dataframe(df_r,
                                                optimized_num_of_dataframe_partitions)
        return df_r

    def __execute_diff_phase(self,
                             diff_phase: str,
                             partitioning: str,
                             first_dataframe_struct: DataFrameStruct,
                             second_dataframe_struct: DataFrameStruct,
                             spark_max_recommended_partition_size: int,
                             spark_app_cores_max_count: int,
                             spark_recommended_tasks_per_cpu: int) -> DataFrame:
        # Get Struct Values of First DataFrame
        first_dataframe = first_dataframe_struct.dataframe
        first_dataframe_schema = first_dataframe_struct.schema
        first_dataframe_column_names = first_dataframe_struct.column_names
        first_dataframe_num_rows = first_dataframe_struct.num_rows
        # Get Struct Values of Second DataFrame
        second_dataframe = second_dataframe_struct.dataframe
        second_dataframe_schema = second_dataframe_struct.schema
        second_dataframe_column_names = second_dataframe_struct.column_names
        second_dataframe_num_rows = second_dataframe_struct.num_rows
        # Assemble Join Conditions
        join_conditions = self.__assemble_join_conditions(first_dataframe,
                                                          first_dataframe_column_names,
                                                          second_dataframe,
                                                          second_dataframe_column_names)
        # Execute DataFrames Diff (Resulting DataFrame: df_r)
        df_r = self.__execute_dataframes_diff(first_dataframe,
                                              second_dataframe,
                                              join_conditions)
        # Substitute Equal Nucleotide Letters on df_r (If diff_phase = opt)
        df_r = self.__substitute_equal_nucleotide_letters_on_df_r(diff_phase,
                                                                  df_r,
                                                                  first_dataframe_column_names)
        # Apply Customized Partitioning on df_r After Diff (If Enabled)
        df_r = self.__apply_customized_partitioning_after_dataframes_diff(partitioning,
                                                                          first_dataframe_schema,
                                                                          first_dataframe_num_rows,
                                                                          second_dataframe_schema,
                                                                          second_dataframe_num_rows,
                                                                          spark_max_recommended_partition_size,
                                                                          spark_app_cores_max_count,
                                                                          spark_recommended_tasks_per_cpu,
                                                                          df_r)
        return df_r

    @staticmethod
    def __show_dataframe(dataframe: DataFrame,
                         number_of_rows_to_show: int,
                         truncate_boolean: bool) -> None:
        # Execute Sort (Spark Wider-Shuffle Transformation) and
        #         Show (Spark Action) Functions
        dataframe \
            .sort(dataframe["Index"].asc_nulls_last()) \
            .show(n=number_of_rows_to_show,
                  truncate=truncate_boolean)

    @staticmethod
    def __write_dataframe_as_distributed_partial_multiple_csv_files(dataframe: DataFrame,
                                                                    destination_file_path: Path,
                                                                    header_boolean: bool,
                                                                    write_mode: str) -> None:
        # Execute Sort (Spark Wider-Shuffle Transformation) and
        #         Write.CSV (Spark Action) Functions
        dataframe \
            .sort(dataframe["Index"].asc_nulls_last()) \
            .write \
            .csv(path=str(destination_file_path),
                 header=header_boolean,
                 mode=write_mode)

    @staticmethod
    def __write_dataframe_as_merged_single_csv_file(dataframe: DataFrame,
                                                    destination_file_path: Path,
                                                    header_boolean: bool,
                                                    write_mode: str) -> None:
        # Execute Coalesce (Spark Less-Wide-Shuffle Transformation),
        #         Sort (Spark Wider-Shuffle Transformation) and
        #         Write.CSV (Spark Action) Functions
        dataframe \
            .coalesce(1) \
            .sort(dataframe["Index"].asc_nulls_last()) \
            .write \
            .csv(path=str(destination_file_path),
                 header=header_boolean,
                 mode=write_mode)

    def __execute_collection_phase(self,
                                   dataframe: DataFrame,
                                   collection_phase: str,
                                   destination_file_path: Path) -> None:
        if collection_phase == "None":
            # Do Not Collect Resulting DataFrame (df_r)
            pass
        elif collection_phase == "SC":  # SP = Show/Collect
            # Show Resulting DataFrame (df_r) as Table Format on Command-Line Interface
            self.__show_dataframe(dataframe,
                                  dataframe.count(),
                                  False)
        elif collection_phase == "DW":  # DW = Distributed Write
            # Write to Disk Resulting DataFrame (df_r) as Multiple Partial CSV Files
            # (Each Spark Executor Writes Its Partition's Data Locally)
            self.__write_dataframe_as_distributed_partial_multiple_csv_files(dataframe,
                                                                             destination_file_path,
                                                                             True,
                                                                             "append")
        elif collection_phase == "MW":  # MW = Merged Write
            # Write to Disk Resulting DataFrame (df_r) as Single CSV File
            # (Each Spark Executor Sends Its Partition's Data to One Executor Which Will Merge and Write Them)
            self.__write_dataframe_as_merged_single_csv_file(dataframe,
                                                             destination_file_path,
                                                             True,
                                                             "append")

    def diff_sequences(self) -> None:
        # Initialize Metrics Variables
        sequences_comparisons_count = 0
        spark_dataframe_partitions_count = 0
        diff_phases_time_in_seconds = 0
        collection_phases_time_in_seconds = 0
        average_sequences_comparison_time_in_seconds = 0
        sequences_comparisons_time_in_seconds = 0
        # Get Spark Context
        spark_context = self.get_spark_context()
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Spark App Id
        spark_app_id = self.get_spark_app_id(spark_context)
        # Get Output Directory
        output_directory = self.get_output_directory()
        # Get Diff Phase
        diff_phase = self.get_diff_phase()
        # Get Collection Phase
        collection_phase = self.get_collection_phase()
        # Get Data Structure
        data_structure = self.get_data_structure()
        # Get Number of Sequences to Compare (N)
        N = self.get_N()
        # Get Differentiator Config File
        differentiator_config_file = self.get_differentiator_config_file()
        # Init ConfigParser Object
        config_parser = ConfigParser()
        # Case Preservation of Each Option Name
        config_parser.optionxform = str
        # Load config_parser
        config_parser.read(differentiator_config_file,
                           encoding="utf-8")
        if diff_phase == "1":
            max_DF = 1
            # Set Maximum Sequences Per DataFrame (max_DF)
            self.__set_max_DF(N,
                              max_DF)
        elif diff_phase == "opt":
            # Read Maximum Sequences Per DataFrame (max_DF)
            max_DF = self.__read_max_DF(differentiator_config_file,
                                        config_parser)
            # Validate Maximum Sequences Per DataFrame (max_DF)
            self.__validate_max_DF(max_DF)
            # Set Maximum Sequences Per DataFrame (max_DF)
            self.__set_max_DF(N,
                              max_DF)
        # Get Maximum Sequences Per DataFrame (max_DF)
        max_DF = self.__get_max_DF()
        # Get Logger
        logger = self.get_logger()
        # Log Maximum Sequences Per DataFrame (max_DF)
        self.__log_max_DF(spark_app_name,
                          max_DF,
                          logger)
        # Read Partitioning
        partitioning = self.__read_partitioning(differentiator_config_file,
                                                config_parser)
        # Validate Partitioning
        self.__validate_partitioning(partitioning)
        # Set Partitioning
        self.__set_partitioning(partitioning)
        # Log Partitioning
        self.__log_partitioning(spark_app_name,
                                partitioning,
                                logger)
        # Get Estimated Amount of Diffs (d_a)
        estimated_d_a = self.estimate_amount_of_diffs(diff_phase,
                                                      N,
                                                      max_DF)
        # Log Estimated Amount of Diffs (d_a)
        self.log_estimated_amount_of_diffs(spark_app_name,
                                           estimated_d_a,
                                           logger)
        # Get Sequences List Text File
        sequences_list_text_file = self.get_sequences_list_text_file()
        # Init SequencesHandler Object
        sh = SequencesHandler(sequences_list_text_file)
        # Generate Sequences Indices List
        sequences_indices_list = sh.generate_sequences_indices_list(N,
                                                                    max_DF)
        # Get Actual Amount of Diffs
        actual_d_a = self.get_actual_amount_of_diffs(sequences_indices_list)
        # Log Actual Amount of Diffs
        self.log_actual_amount_of_diffs(spark_app_name,
                                        actual_d_a,
                                        logger)
        # Calculate Amount of Diffs (d_a) Estimation Absolute Error
        d_a_estimation_absolute_error = self.calculate_amount_of_diffs_estimation_absolute_error(estimated_d_a,
                                                                                                 actual_d_a)
        # Calculate Amount of Diffs (d_a) Estimation Percent Error
        d_a_estimation_percent_error = self.calculate_amount_of_diffs_estimation_percent_error(estimated_d_a,
                                                                                               actual_d_a)
        # Log Amount of Diffs (d_a) Estimation Absolute and Percent Errors
        self.log_d_a_estimation_errors(spark_app_name,
                                       d_a_estimation_absolute_error,
                                       d_a_estimation_percent_error,
                                       logger)
        # Get Spark Session
        spark_session = self.get_spark_session()
        # Iterate Through Sequences Indices List
        for index_sequences_indices_list in range(actual_d_a):
            # Sequences Comparison Start Time
            sequences_comparison_start_time = time()
            # Get First DataFrame Sequences Indices List
            first_dataframe_sequences_indices_list = sequences_indices_list[index_sequences_indices_list][0]
            # Get Second DataFrame Sequences Indices List
            second_dataframe_sequences_indices_list = sequences_indices_list[index_sequences_indices_list][1]
            # Get First DataFrame Sequences Data List
            first_dataframe_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                             first_dataframe_sequences_indices_list)
            # Get Second DataFrame Sequences Data List
            second_dataframe_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                              second_dataframe_sequences_indices_list)
            # Get the Biggest Sequence Length Among DataFrames
            biggest_sequence_length_among_dataframes = \
                self.get_biggest_sequence_length_among_data_structures(first_dataframe_sequences_data_list,
                                                                       second_dataframe_sequences_data_list)
            # Set Length of First DataFrame
            first_dataframe_length = biggest_sequence_length_among_dataframes
            # Set Length of Second DataFrame
            second_dataframe_length = biggest_sequence_length_among_dataframes
            # Generate Schema Struct List of First DataFrame
            first_dataframe_schema_struct_list = \
                self.__generate_dataframe_schema_struct_list(first_dataframe_sequences_data_list)
            # Create Schema of First DataFrame
            first_dataframe_schema = self.__create_dataframe_schema(first_dataframe_schema_struct_list)
            # Get Schema Column Names of First DataFrame
            first_dataframe_schema_column_names = self.__get_dataframe_schema_column_names(first_dataframe_schema)
            # Get Data of First DataFrame
            first_dataframe_data = self.get_data_structure_data(first_dataframe_length,
                                                                first_dataframe_sequences_data_list)
            # Create First DataFrame
            first_dataframe = self.__create_dataframe(spark_session,
                                                      first_dataframe_data,
                                                      first_dataframe_schema)
            # Get Spark App Cores Max Count
            spark_app_cores_max_count = self.get_spark_app_cores_max_count(spark_context)
            # Get Spark Recommended Tasks per CPU
            spark_recommended_tasks_per_cpu = self.get_spark_recommended_tasks_per_cpu()
            # Apply Customized Partitioning on First DataFrame After Creation (If Enabled)
            first_dataframe = \
                self.__apply_customized_partitioning_after_dataframe_creation(partitioning,
                                                                              spark_app_cores_max_count,
                                                                              spark_recommended_tasks_per_cpu,
                                                                              first_dataframe)
            # Get Number of Partitions of First DataFrame
            first_dataframe_partitions_number = first_dataframe.rdd.getNumPartitions()
            # Increase Spark DataFrame Partitions Count
            spark_dataframe_partitions_count = spark_dataframe_partitions_count + first_dataframe_partitions_number
            # Create Struct of First DataFrame
            first_dataframe_struct = DataFrameStruct(first_dataframe,
                                                     first_dataframe_schema,
                                                     first_dataframe_schema_column_names,
                                                     first_dataframe_length)
            # Generate Schema Struct List of Second DataFrame
            second_dataframe_schema_struct_list = \
                self.__generate_dataframe_schema_struct_list(second_dataframe_sequences_data_list)
            # Create Schema of Second DataFrame
            second_dataframe_schema = self.__create_dataframe_schema(second_dataframe_schema_struct_list)
            # Get Schema Column Names of Second DataFrame
            second_dataframe_schema_column_names = self.__get_dataframe_schema_column_names(second_dataframe_schema)
            # Get Data of Second DataFrame
            second_dataframe_data = self.get_data_structure_data(second_dataframe_length,
                                                                 second_dataframe_sequences_data_list)
            # Create Second DataFrame
            second_dataframe = self.__create_dataframe(spark_session,
                                                       second_dataframe_data,
                                                       second_dataframe_schema)
            # Apply Customized Partitioning on Second DataFrame After Creation (If Enabled)
            second_dataframe = \
                self.__apply_customized_partitioning_after_dataframe_creation(partitioning,
                                                                              spark_app_cores_max_count,
                                                                              spark_recommended_tasks_per_cpu,
                                                                              second_dataframe)
            # Get Number of Partitions of Second DataFrame
            second_dataframe_partitions_number = second_dataframe.rdd.getNumPartitions()
            # Increase Spark DataFrame Partitions Count
            spark_dataframe_partitions_count = spark_dataframe_partitions_count + second_dataframe_partitions_number
            # Create Struct of Second DataFrame
            second_dataframe_struct = DataFrameStruct(second_dataframe,
                                                      second_dataframe_schema,
                                                      second_dataframe_schema_column_names,
                                                      second_dataframe_length)
            # Get Spark Maximum Recommended Partition Size in Bytes
            spark_max_recommended_partition_size = 134217728  # 128 MB
            # Diff Phase Start Time
            diff_phase_start_time = time()
            # Execute Diff Phase
            df_r = self.__execute_diff_phase(diff_phase,
                                             partitioning,
                                             first_dataframe_struct,
                                             second_dataframe_struct,
                                             spark_max_recommended_partition_size,
                                             spark_app_cores_max_count,
                                             spark_recommended_tasks_per_cpu)
            # Time to Execute Diff Phase in Seconds
            time_to_execute_diff_phase_in_seconds = time() - diff_phase_start_time
            # Increase Diff Phases Time
            diff_phases_time_in_seconds = diff_phases_time_in_seconds + time_to_execute_diff_phase_in_seconds
            # Increase Sequences Comparisons Count
            sequences_comparisons_count = sequences_comparisons_count + 1
            # Get Partition Number of Resulting DataFrame (df_r)
            df_r_partitions_number = df_r.rdd.getNumPartitions()
            # Increase Spark DataFrame Partitions Count
            spark_dataframe_partitions_count = spark_dataframe_partitions_count + df_r_partitions_number
            # Get First Sequence Index of First DataFrame
            first_dataframe_first_sequence_index = first_dataframe_sequences_indices_list[0]
            # Get First Sequence Index of Second DataFrame
            second_dataframe_first_sequence_index = second_dataframe_sequences_indices_list[0]
            # Get Last Sequence Index of Second DataFrame
            second_dataframe_last_sequence_index = second_dataframe_sequences_indices_list[-1]
            # Get Destination File Path for Collection Phase
            collection_phase_destination_file_path = \
                self.get_collection_phase_destination_file_path(output_directory,
                                                                spark_app_name,
                                                                spark_app_id,
                                                                first_dataframe_first_sequence_index,
                                                                second_dataframe_first_sequence_index,
                                                                second_dataframe_last_sequence_index)
            # Collection Phase Start Time
            collection_phase_start_time = time()
            # Execute Collection Phase
            self.__execute_collection_phase(df_r,
                                            collection_phase,
                                            collection_phase_destination_file_path)
            # Time to Execute Collection Phase in Seconds
            time_to_execute_collection_phase_in_seconds = time() - collection_phase_start_time
            # Increase Collection Phases Time
            collection_phases_time_in_seconds = \
                collection_phases_time_in_seconds + time_to_execute_collection_phase_in_seconds
            # Time to Compare Sequences in Seconds
            time_to_compare_sequences_in_seconds = time() - sequences_comparison_start_time
            # Increase Sequences Comparisons Time
            sequences_comparisons_time_in_seconds = \
                sequences_comparisons_time_in_seconds + time_to_compare_sequences_in_seconds
            # Log Time to Compare Sequences
            self.log_time_to_compare_sequences(spark_app_name,
                                               first_dataframe_first_sequence_index,
                                               second_dataframe_first_sequence_index,
                                               second_dataframe_last_sequence_index,
                                               data_structure,
                                               time_to_compare_sequences_in_seconds,
                                               logger)
            # Get Number of Sequences Comparisons Left
            number_of_sequences_comparisons_left = \
                self.get_number_of_sequences_comparisons_left(actual_d_a,
                                                              sequences_comparisons_count)
            # Get Average Sequences Comparison Time
            average_sequences_comparison_time_in_seconds = \
                self.get_average_sequences_comparison_time(sequences_comparisons_time_in_seconds,
                                                           sequences_comparisons_count)
            # Estimate Time Left
            estimated_time_left_in_seconds = self.estimate_time_left(number_of_sequences_comparisons_left,
                                                                     average_sequences_comparison_time_in_seconds)
            # Print Real Time Metrics
            self.print_real_time_metrics(spark_app_name,
                                         sequences_comparisons_count,
                                         number_of_sequences_comparisons_left,
                                         average_sequences_comparison_time_in_seconds,
                                         estimated_time_left_in_seconds)
        # Log Average Sequences Comparison Time
        self.log_average_sequences_comparison_time(spark_app_name,
                                                   data_structure,
                                                   average_sequences_comparison_time_in_seconds,
                                                   logger)
        # Log Sequences Comparisons Count
        self.log_sequences_comparisons_count(spark_app_name,
                                             sequences_comparisons_count,
                                             logger)
        # Log Diff Phases Time
        self.log_diff_phases_time(spark_app_name,
                                  diff_phase,
                                  diff_phases_time_in_seconds,
                                  logger)
        # Log Collection Phases Time
        self.log_collection_phases_time(spark_app_name,
                                        collection_phase,
                                        collection_phases_time_in_seconds,
                                        logger)
        # Log Spark DataFrame Partitions Count
        self.log_spark_data_structure_partitions_count(spark_app_name,
                                                       data_structure,
                                                       spark_dataframe_partitions_count,
                                                       logger)
        # Delete SequencesHandler Object
        del sh
