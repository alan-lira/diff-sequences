from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from functools import reduce
from pathlib import Path
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import LongType, StringType, StructType
from queue import Empty, Full
from sequences_handler.sequences_handler import SequencesHandler
from thread_builder.thread_builder import ThreadBuilder
from time import time


class DataFrameDifferentiator(Differentiator):

    def __init__(self,
                 differentiator_config_file: Path) -> None:
        super().__init__(differentiator_config_file)

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
        # Set Scheduler Pool to 'fair_pool' (Takes Effect Only if Scheduler Mode is set to 'FAIR')
        self.set_scheduler_pool("fair_pool")
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

    def __produce_dataframes(self,
                             producer_name: str,
                             sh: SequencesHandler,
                             sequences_list_text_file: Path,
                             partitioning: str,
                             spark_session: SparkSession) -> None:
        full_products_queue_waiting_timeout = self.get_full_products_queue_waiting_timeout()
        waiting_timeout_sec = self.convert_waiting_timeout_to_sec(full_products_queue_waiting_timeout)
        while True:
            self.sequences_indices_list_lock.acquire()
            if self.sequences_indices_list:
                # Get Next DataFrames Sequences Indices
                sequences_indices = self.sequences_indices_list.pop(0)
                self.sequences_indices_list_lock.release()
            else:
                self.sequences_indices_list_lock.release()
                break
            # Get Total Number of Cores of the Current Executors
            total_number_of_cores_of_the_current_executors = \
                self.get_total_number_of_cores_of_the_current_executors()
            # BEGIN OF MAP PHASE
            # Get Number of Available Map Cores (Equals to Total Number of Cores of the Current Executors)
            number_of_available_map_cores = total_number_of_cores_of_the_current_executors
            # Get First DataFrame Sequences Indices List
            first_dataframe_sequences_indices_list = sequences_indices[0]
            # Get Second DataFrame Sequences Indices List
            second_dataframe_sequences_indices_list = sequences_indices[1]
            # Get First DataFrame Sequences Data List
            first_dataframe_sequences_data_list = \
                sh.generate_sequences_list(sequences_list_text_file,
                                           first_dataframe_sequences_indices_list)
            # Get Second DataFrame Sequences Data List
            second_dataframe_sequences_data_list = \
                sh.generate_sequences_list(sequences_list_text_file,
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
            first_dataframe_data = self.get_data_structure_data(DataFrame,
                                                                first_dataframe_length,
                                                                first_dataframe_sequences_data_list)
            # Create First DataFrame
            first_dataframe_number_of_partitions = 0
            if partitioning == "Auto":
                first_dataframe_number_of_partitions = number_of_available_map_cores
            elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
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
            second_dataframe_data = self.get_data_structure_data(DataFrame,
                                                                 second_dataframe_length,
                                                                 second_dataframe_sequences_data_list)
            # Create Second DataFrame
            second_dataframe_number_of_partitions = 0
            if partitioning == "Auto":
                second_dataframe_number_of_partitions = number_of_available_map_cores
            elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
                k_i = self.get_k_i()
                second_dataframe_number_of_partitions = int(number_of_available_map_cores / k_i)
            second_dataframe = self.__create_dataframe(spark_session,
                                                       second_dataframe_data,
                                                       second_dataframe_schema,
                                                       second_dataframe_number_of_partitions)
            # END OF MAP PHASE
            # Produce Item
            produced_item = [first_dataframe,
                             first_dataframe_schema_column_names,
                             first_dataframe_sequences_indices_list,
                             second_dataframe,
                             second_dataframe_schema_column_names,
                             second_dataframe_sequences_indices_list]
            try:
                # Put Produced Item Into Queue (Block if Necessary Until a Free Slot is Available)
                self.products_queue.put(produced_item,
                                        timeout=waiting_timeout_sec)
                # Print Produced Item Message
                produced_item_message = "[{0}] Produced Item '{1}' (Current Products Queue Size: {2})" \
                    .format(producer_name,
                            str(produced_item),
                            str(self.products_queue.qsize()))
                print(produced_item_message)
            except Full:
                products_queue_full_timeout_exception_message = \
                    "Products Queue Full Timeout Reached: {0} has been Decommissioned!".format(producer_name)
                print(products_queue_full_timeout_exception_message)
                break

    def __consume_dataframes(self,
                             consumer_name: str,
                             diff_phase: str,
                             output_directory: Path,
                             spark_app_name: str,
                             spark_app_id: str,
                             collection_phase: str) -> None:
        empty_products_queue_waiting_timeout = self.get_empty_products_queue_waiting_timeout()
        waiting_timeout_sec = self.convert_waiting_timeout_to_sec(empty_products_queue_waiting_timeout)
        while True:
            try:
                # Get Item to Consume From Queue (Block if Necessary Until an Item is Available)
                item_to_consume = self.products_queue.get(timeout=waiting_timeout_sec)
                # BEGIN OF REDUCE PHASE
                # Get First DataFrame
                first_dataframe = item_to_consume[0]
                # Get First DataFrame Schema Column Names
                first_dataframe_schema_column_names = item_to_consume[1]
                # Get Second DataFrame
                second_dataframe = item_to_consume[3]
                # Get Second DataFrame Schema Column Names
                second_dataframe_schema_column_names = item_to_consume[4]
                # Execute Diff Phase
                df_r = self.__execute_diff_phase(first_dataframe,
                                                 first_dataframe_schema_column_names,
                                                 second_dataframe,
                                                 second_dataframe_schema_column_names)
                # Substitute Equal Nucleotide Letters on df_r (If diff_phase = DIFF_opt)
                if diff_phase == "DIFF_opt":
                    df_r = self.__substitute_equal_nucleotide_letters_on_df_r(df_r,
                                                                              first_dataframe_schema_column_names)
                # Get First Sequence Index of First DataFrame
                first_dataframe_first_sequence_index = item_to_consume[2][0]
                # Get First Sequence Index of Second DataFrame
                second_dataframe_first_sequence_index = item_to_consume[5][0]
                # Get Last Sequence Index of Second DataFrame
                second_dataframe_last_sequence_index = item_to_consume[5][-1]
                # Get Destination File Path for Collection Phase
                collection_phase_destination_file_path = \
                    self.get_collection_phase_destination_file_path(output_directory,
                                                                    spark_app_name,
                                                                    spark_app_id,
                                                                    first_dataframe_first_sequence_index,
                                                                    second_dataframe_first_sequence_index,
                                                                    second_dataframe_last_sequence_index)
                # Execute Collection Phase (Concurrent Spark Active Job)
                self.__execute_collection_phase(df_r,
                                                collection_phase,
                                                collection_phase_destination_file_path)
                # END OF REDUCE PHASE
                # Print Consumed Item Message
                consumed_item_message = "[{0}] Consumed Item '{1}' (Current Products Queue Size: {2})" \
                    .format(consumer_name,
                            str(item_to_consume),
                            str(self.products_queue.qsize()))
                print(consumed_item_message)
                self.products_queue.task_done()
            except Empty:
                products_queue_empty_timeout_exception_message = \
                    "Products Queue Empty Timeout Reached: {0} has been Decommissioned!".format(consumer_name)
                print(products_queue_empty_timeout_exception_message)
                break

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
        # Get Allow Producer-Consumer Threads
        allow_producer_consumer_threads = self.get_allow_producer_consumer_threads()
        # Get Allow Simultaneous Jobs Run
        allow_simultaneous_jobs_run = self.get_allow_simultaneous_jobs_run()
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
        if diff_phase == "DIFF_1":
            # Set Maximum of One Sequence per RDD
            max_s = 1
            # Set Maximum Sequences Per RDD (maxₛ)
            self.set_max_s(n,
                           max_s)
        elif diff_phase == "DIFF_opt":
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
        # Set Sequences Indices List
        self.set_sequences_indices_list(sequences_indices_list)
        # Get Actual Total Number of Diffs (Dₐ)
        actual_d_a = self.get_actual_total_number_of_diffs()
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
        if allow_producer_consumer_threads:
            # Get Producer-Consumer Threads and Queue Properties
            number_of_producers = self.get_number_of_producers()
            products_queue_max_size = self.get_products_queue_max_size()
            number_of_consumers = self.get_number_of_consumers()
            # Initialize Products Queue
            self.initialize_products_queue(products_queue_max_size)
            # Initialize Products Queue Lock
            self.initialize_products_queue_lock()
            # Spawn Producers Thread Pool
            for i in range(1, number_of_producers + 1):
                tb_producer_target_method = self.__produce_dataframes
                tb_producer_name = "Producer_" + str(i)
                tb_producer_target_method_arguments = (tb_producer_name,
                                                       sh,
                                                       sequences_list_text_file,
                                                       partitioning,
                                                       spark_session)
                tb_producer_daemon_mode = False
                tb = ThreadBuilder(tb_producer_target_method,
                                   tb_producer_name,
                                   tb_producer_target_method_arguments,
                                   tb_producer_daemon_mode)
                producer = tb.build()
                producer.start()
            # Spawn Consumers Thread Pool
            for i in range(1, number_of_consumers + 1):
                tb_consumer_target_method = self.__consume_dataframes
                tb_consumer_name = "Consumer_" + str(i)
                tb_consumer_target_method_arguments = (tb_consumer_name,
                                                       diff_phase,
                                                       output_directory,
                                                       spark_app_name,
                                                       spark_app_id,
                                                       collection_phase)
                tb_consumer_daemon_mode = False
                tb = ThreadBuilder(tb_consumer_target_method,
                                   tb_consumer_name,
                                   tb_consumer_target_method_arguments,
                                   tb_consumer_daemon_mode)
                consumer = tb.build()
                consumer.start()
            # Join Non-Daemonic Threads (Waiting for Completion)
            self.join_non_daemonic_threads()
        else:
            # Iterate Through Sequences Indices List
            for index_sequences_indices_list in range(actual_d_a):
                # Sequences Comparison Start Time
                sequences_comparison_start_time = time()
                # Get Current Number of Executors
                current_number_of_executors = self.get_current_number_of_executors()
                # Get Total Number of Cores of the Current Executors
                total_number_of_cores_of_the_current_executors = \
                    self.get_total_number_of_cores_of_the_current_executors()
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
                first_dataframe_sequences_data_list = \
                    sh.generate_sequences_list(sequences_list_text_file,
                                               first_dataframe_sequences_indices_list)
                # Get Second DataFrame Sequences Data List
                second_dataframe_sequences_data_list = \
                    sh.generate_sequences_list(sequences_list_text_file,
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
                first_dataframe_data = self.get_data_structure_data(DataFrame,
                                                                    first_dataframe_length,
                                                                    first_dataframe_sequences_data_list)
                # Create First DataFrame
                first_dataframe_number_of_partitions = 0
                if partitioning == "Auto":
                    first_dataframe_number_of_partitions = number_of_available_map_cores
                elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
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
                second_dataframe_data = self.get_data_structure_data(DataFrame,
                                                                     second_dataframe_length,
                                                                     second_dataframe_sequences_data_list)
                # Create Second DataFrame
                second_dataframe_number_of_partitions = 0
                if partitioning == "Auto":
                    second_dataframe_number_of_partitions = number_of_available_map_cores
                elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
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
                # Substitute Equal Nucleotide Letters on df_r (If diff_phase = DIFF_opt)
                if diff_phase == "DIFF_opt":
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
                if allow_simultaneous_jobs_run:
                    # Execute Collection Phase Using Non-Daemonic Thread (Allows Concurrent Spark Active Jobs)
                    tb_collection_phase_target_method = self.__execute_collection_phase
                    tb_collection_phase_name = "Collection_Phase_" + str(index_sequences_indices_list)
                    tb_collection_phase_target_method_arguments = (df_r,
                                                                   collection_phase,
                                                                   collection_phase_destination_file_path)
                    tb_collection_phase_daemon_mode = False
                    tb = ThreadBuilder(tb_collection_phase_target_method,
                                       tb_collection_phase_name,
                                       tb_collection_phase_target_method_arguments,
                                       tb_collection_phase_daemon_mode)
                    tb_collection_phase = tb.build()
                    tb_collection_phase.start()
                else:
                    # Execute Collection Phase (Unique Spark Active Job)
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
                # Search For 'k_opt', If Not Found Yet & Adaptive_K Partitioning is Enabled
                if partitioning == "Adaptive_K" and sequences_comparisons_count > 1:
                    self.find_and_log_k_opt_using_adaptive_k_partitioning(time_to_compare_sequences_in_seconds,
                                                                          logger)
            if allow_simultaneous_jobs_run:
                # Join Non-Daemonic Threads (Waiting for Completion)
                self.join_non_daemonic_threads()
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
