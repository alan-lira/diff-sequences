from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from functools import reduce
from pathlib import Path
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType, StringType, StructType
from sequences_handler.sequences_handler import SequencesHandler
from time import time


class DataFrameDifferentiator(Differentiator):

    def __init__(self) -> None:
        super().__init__()

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

    def __create_dataframe(self,
                           spark_session: SparkSession,
                           dataframe_data: list,
                           dataframe_schema: StructType,
                           dataframe_number_of_partitions: int) -> DataFrame:
        dataframe = spark_session.createDataFrame(data=dataframe_data,
                                                  schema=dataframe_schema,
                                                  verifySchema=True)
        dataframe = self.repartition_data_structure(dataframe,
                                                    dataframe_number_of_partitions)
        # Increase Map Tasks Count
        self.increase_map_tasks_count(dataframe.rdd.getNumPartitions())
        return dataframe

    @staticmethod
    def __assemble_join_conditions(first_dataframe: DataFrame,
                                   first_dataframe_column_names: list,
                                   second_dataframe: DataFrame,
                                   second_dataframe_column_names: list) -> Column:
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
    def __execute_dataframe_diff(first_dataframe: DataFrame,
                                 second_dataframe: DataFrame,
                                 join_conditions: Column) -> DataFrame:
        # Execute Full Outer Join (Spark Wider-Shuffle Transformation),
        #         Filter (Spark Narrow Transformation) and
        #         Drop (Spark Narrow Transformation) Functions
        df_r = first_dataframe \
            .join(second_dataframe, join_conditions, "full_outer") \
            .filter(first_dataframe["Index"].isNotNull() & second_dataframe["Index"].isNotNull()) \
            .drop(second_dataframe["Index"])
        return df_r

    @staticmethod
    def __substitute_equal_nucleotide_letters_on_df_r(df_r: DataFrame,
                                                      first_dataframe_column_names: list) -> DataFrame:
        # Update Non-Diff Line Values to "=" Character (For Data Reading Visual Enhancing)
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

    def __execute_diff_phase(self,
                             first_dataframe: DataFrame,
                             first_dataframe_column_names: list,
                             second_dataframe: DataFrame,
                             second_dataframe_column_names: list) -> DataFrame:
        # Assemble Join Conditions
        join_conditions = self.__assemble_join_conditions(first_dataframe,
                                                          first_dataframe_column_names,
                                                          second_dataframe,
                                                          second_dataframe_column_names)
        # Execute DataFrame Diff (Resulting DataFrame: df_r)
        df_r = self.__execute_dataframe_diff(first_dataframe,
                                             second_dataframe,
                                             join_conditions)
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
    def __write_dataframe_as_csv_file(dataframe: DataFrame,
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
            self.__write_dataframe_as_csv_file(dataframe,
                                               destination_file_path,
                                               True,
                                               "append")
        elif collection_phase == "MW":  # MW = Merged Write
            # Write to Disk Resulting DataFrame (df_r) as Single CSV File
            # (Each Spark Executor Sends Its Partition's Data to One Executor Which Will Merge and Write Them)
            dataframe = self.repartition_data_structure(dataframe,
                                                        1)
            self.__write_dataframe_as_csv_file(dataframe,
                                               destination_file_path,
                                               True,
                                               "append")
        # Increase Reduce Tasks Count
        self.increase_reduce_tasks_count(dataframe.rdd.getNumPartitions())

    def diff_sequences(self) -> None:
        # Get Spark Context
        spark_context = self.get_spark_context()
        # Get Spark Session
        spark_session = self.get_spark_session()
        # Get Spark App Name
        spark_app_name = self.get_spark_app_name(spark_context)
        # Get Spark App Id
        spark_app_id = self.get_spark_app_id(spark_context)
        # Get Output Directory
        output_directory = self.get_output_directory()
        # Get Data Structure
        data_structure = self.get_data_structure()
        # Get Diff Phase
        diff_phase = self.get_diff_phase()
        # Get Collection Phase
        collection_phase = self.get_collection_phase()
        # Get Partitioning
        partitioning = self.get_partitioning()
        # Get Sequences List Text File
        sequences_list_text_file = self.get_sequences_list_text_file()
        # Init SequencesHandler Object
        sh = SequencesHandler(sequences_list_text_file)
        # Get Sequences List Length
        sequences_list_length = sh.get_sequences_list_length()
        # Set Number of Sequences to Compare (N)
        self.set_n(sequences_list_length)
        # Get Number of Sequences to Compare (N)
        n = self.get_n()
        # Get Logger
        logger = self.get_logger()
        # Log Number of Sequences to Compare (N)
        self.log_n(n,
                   logger)
        if diff_phase == "1":
            # Set Maximum of One Sequence per RDD
            max_s = 1
            # Set Maximum Sequences Per RDD (maxₛ)
            self.set_max_s(n,
                           max_s)
        elif diff_phase == "opt":
            # Get Differentiator Config File
            differentiator_config_file = self.get_differentiator_config_file()
            # Init ConfigParser Object
            config_parser = ConfigParser()
            # Case Preservation of Each Option Name
            config_parser.optionxform = str
            # Load config_parser
            config_parser.read(differentiator_config_file,
                               encoding="utf-8")
            # Read Maximum Sequences Per RDD (maxₛ)
            max_s = self.read_max_s(differentiator_config_file,
                                    config_parser)
            # Set Maximum Sequences Per RDD (maxₛ)
            self.set_max_s(n,
                           max_s)
        # Get Maximum Sequences Per RDD (maxₛ)
        max_s = self.get_max_s()
        # Log Maximum Sequences Per RDD (maxₛ)
        self.log_max_s(data_structure,
                       max_s,
                       logger)
        # Estimate Total Number of Diffs (Dₐ Estimation)
        estimated_d_a = self.estimate_total_number_of_diffs(diff_phase,
                                                            n,
                                                            max_s)
        # Log Dₐ Estimation
        self.log_estimated_total_number_of_diffs(estimated_d_a,
                                                 logger)
        # Generate Sequences Indices List
        sequences_indices_list = sh.generate_sequences_indices_list(n,
                                                                    max_s)
        # Get Actual Total Number of Diffs (Dₐ)
        actual_d_a = self.get_actual_total_number_of_diffs(sequences_indices_list)
        # Log Dₐ
        self.log_actual_total_number_of_diffs(actual_d_a,
                                              logger)
        # Calculate Absolute Error of Dₐ Estimation
        d_a_estimation_absolute_error = self.calculate_absolute_error_of_total_number_of_diffs_estimation(estimated_d_a,
                                                                                                          actual_d_a)
        # Calculate Percent Error of Dₐ Estimation
        d_a_estimation_percent_error = self.calculate_percent_error_of_total_number_of_diffs_estimation(estimated_d_a,
                                                                                                        actual_d_a)
        # Log Dₐ Estimation Errors
        self.log_total_number_of_diffs_estimation_errors(d_a_estimation_absolute_error,
                                                         d_a_estimation_percent_error,
                                                         logger)
        # Iterate Through Sequences Indices List
        for index_sequences_indices_list in range(actual_d_a):
            # Sequences Comparison Start Time
            sequences_comparison_start_time = time()
            # Get Current Number of Executors
            current_number_of_executors = self.get_current_number_of_executors()
            # Get Total Number of Cores of the Current Executors
            total_number_of_cores_of_the_current_executors = self.get_total_number_of_cores_of_the_current_executors()
            # Get Converted Total Amount of Memory (Heap Space Fraction) of the Current Executors
            converted_total_amount_of_memory_of_the_current_executors = \
                self.get_converted_total_amount_of_memory_of_the_current_executors()
            # BEGIN OF MAP PHASE
            # Get Number of Available Map Cores (Equals to Total Number of Cores of the Current Executors)
            number_of_available_map_cores = total_number_of_cores_of_the_current_executors
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
            first_dataframe_number_of_partitions = 0
            if partitioning == "auto":
                first_dataframe_number_of_partitions = number_of_available_map_cores
            elif partitioning == "adaptive":
                k_i = self.get_k_i()
                first_dataframe_number_of_partitions = int(number_of_available_map_cores / k_i)
            first_dataframe = self.__create_dataframe(spark_session,
                                                      first_dataframe_data,
                                                      first_dataframe_schema,
                                                      first_dataframe_number_of_partitions)
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
            second_dataframe_number_of_partitions = 0
            if partitioning == "auto":
                second_dataframe_number_of_partitions = number_of_available_map_cores
            elif partitioning == "adaptive":
                k_i = self.get_k_i()
                second_dataframe_number_of_partitions = int(number_of_available_map_cores / k_i)
            second_dataframe = self.__create_dataframe(spark_session,
                                                       second_dataframe_data,
                                                       second_dataframe_schema,
                                                       second_dataframe_number_of_partitions)
            # END OF MAP PHASE
            # BEGIN OF REDUCE PHASE
            # Execute Diff Phase
            df_r = self.__execute_diff_phase(first_dataframe,
                                             first_dataframe_schema_column_names,
                                             second_dataframe,
                                             second_dataframe_schema_column_names)
            # Substitute Equal Nucleotide Letters on df_r (If diff_phase = opt)
            if diff_phase == "opt":
                df_r = self.__substitute_equal_nucleotide_letters_on_df_r(df_r,
                                                                          first_dataframe_schema_column_names)
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
            # Execute Collection Phase
            self.__execute_collection_phase(df_r,
                                            collection_phase,
                                            collection_phase_destination_file_path)
            # END OF REDUCE PHASE
            # Increase Sequences Comparisons Count
            self.increase_sequences_comparisons_count(1)
            # Time to Compare Sequences in Seconds
            time_to_compare_sequences_in_seconds = time() - sequences_comparison_start_time
            # Increase Sequences Comparisons Time
            self.increase_sequences_comparisons_time_in_seconds(time_to_compare_sequences_in_seconds)
            # Log Time to Compare Sequences
            self.log_time_to_compare_sequences(first_dataframe_first_sequence_index,
                                               second_dataframe_first_sequence_index,
                                               second_dataframe_last_sequence_index,
                                               time_to_compare_sequences_in_seconds,
                                               current_number_of_executors,
                                               total_number_of_cores_of_the_current_executors,
                                               converted_total_amount_of_memory_of_the_current_executors,
                                               logger)
            # Get Sequences Comparisons Count
            sequences_comparisons_count = self.get_sequences_comparisons_count()
            # Get Sequences Comparisons Time in Seconds
            sequences_comparisons_time_in_seconds = self.get_sequences_comparisons_time_in_seconds()
            # Get Number of Sequences Comparisons Left
            number_of_sequences_comparisons_left = \
                self.calculate_number_of_sequences_comparisons_left(actual_d_a,
                                                                    sequences_comparisons_count)
            # Calculate Sequences Comparisons Average Time in Seconds
            sequences_comparisons_average_time_in_seconds = \
                self.calculate_sequences_comparisons_average_time(sequences_comparisons_time_in_seconds,
                                                                  sequences_comparisons_count)
            # Set Sequences Comparisons Average Time in Seconds
            self.set_sequences_comparisons_average_time_in_seconds(sequences_comparisons_average_time_in_seconds)
            # Estimate Time Left in Seconds
            estimated_time_left_in_seconds = self.estimate_time_left(number_of_sequences_comparisons_left,
                                                                     sequences_comparisons_average_time_in_seconds)
            # Print Real Time Metrics
            self.print_real_time_metrics(spark_app_name,
                                         sequences_comparisons_count,
                                         number_of_sequences_comparisons_left,
                                         sequences_comparisons_average_time_in_seconds,
                                         estimated_time_left_in_seconds)
            # Search For 'k_opt', If Not Found Yet
            if partitioning == "adaptive" and sequences_comparisons_count > 1:
                self.find_and_log_k_opt_using_adaptive_partitioning(time_to_compare_sequences_in_seconds,
                                                                    logger)
        # Get Sequences Comparisons Average Time in Seconds
        sequences_comparisons_average_time_in_seconds = self.get_sequences_comparisons_average_time_in_seconds()
        # Log Sequences Comparisons Average Time
        self.log_sequences_comparisons_average_time(data_structure,
                                                    sequences_comparisons_average_time_in_seconds,
                                                    logger)
        # Log Map Tasks Count
        map_tasks_count = self.get_map_tasks_count()
        self.log_tasks_count("Map",
                             map_tasks_count,
                             logger)
        # Log Reduce Tasks Count
        reduce_tasks_count = self.get_reduce_tasks_count()
        self.log_tasks_count("Reduce",
                             reduce_tasks_count,
                             logger)
        # Delete SequencesHandler Object
        del sh
