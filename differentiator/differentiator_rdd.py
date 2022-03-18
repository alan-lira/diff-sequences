from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from operator import __getitem__
from os import walk
from pathlib import Path
from pyspark import RDD, SparkContext
from queue import Empty, Full
from sequences_handler.sequences_handler import SequencesHandler
from thread_builder.thread_builder import ThreadBuilder
from time import time
from typing import List, Optional
from zipfile import ZipFile


def reduce_function(first_rdd_element: {__getitem__},
                    second_rdd_element: {__getitem__}) -> List[Optional[str]]:
    first_rdd_first_sequence_index = first_rdd_element[1][0]
    second_rdd_first_sequence_index = second_rdd_element[1][0]
    if first_rdd_first_sequence_index > second_rdd_first_sequence_index:  # sequences are switched (fix before compare)
        aux = first_rdd_element
        first_rdd_element = second_rdd_element
        second_rdd_element = aux
    rdd_r_candidate_element = []
    number_of_sequences_on_second_rdd = len(second_rdd_element[0])
    first_rdd_sequence_nucleotide_letter = first_rdd_element[0][0]
    if number_of_sequences_on_second_rdd == 1:
        second_rdd_sequence_nucleotide_letter = second_rdd_element[0][0]
        if first_rdd_sequence_nucleotide_letter is None or second_rdd_sequence_nucleotide_letter is None:
            pass  # nucleotide letter from a sequence is missing, making incomparable (position to discard)
        elif first_rdd_sequence_nucleotide_letter == second_rdd_sequence_nucleotide_letter:
            pass  # identical nucleotide letters in both sequences (position to discard)
        elif first_rdd_sequence_nucleotide_letter != second_rdd_sequence_nucleotide_letter:
            rdd_r_candidate_element = [first_rdd_sequence_nucleotide_letter,
                                       second_rdd_sequence_nucleotide_letter,
                                       "Diff_Sequences"]
    elif number_of_sequences_on_second_rdd > 1:
        for i in range(number_of_sequences_on_second_rdd):
            second_rdd_ith_sequence_nucleotide_letter = second_rdd_element[0][i]
            if first_rdd_sequence_nucleotide_letter is None or second_rdd_ith_sequence_nucleotide_letter is None:
                pass  # nucleotide letter from a sequence is missing, making incomparable (position to discard)
            elif first_rdd_sequence_nucleotide_letter == second_rdd_ith_sequence_nucleotide_letter:
                rdd_r_candidate_element.append("=")  # identical nucleotide letters in both sequences (append '=')
            elif first_rdd_sequence_nucleotide_letter != second_rdd_ith_sequence_nucleotide_letter:
                rdd_r_candidate_element.append(second_rdd_ith_sequence_nucleotide_letter)
        nucleotide_letters_from_all_sequences_are_identical = all(letter == "=" for letter in rdd_r_candidate_element)
        if nucleotide_letters_from_all_sequences_are_identical:
            rdd_r_candidate_element = []  # identical nucleotide letters in all sequences (position to discard)
        else:
            rdd_r_candidate_element.insert(0, first_rdd_sequence_nucleotide_letter)
            rdd_r_candidate_element.append("Diff_Sequences")
    return rdd_r_candidate_element


def filter_function(rdd_r_candidate_element: {__getitem__}) -> bool:
    return rdd_r_candidate_element[1] and "Diff_Sequences" in rdd_r_candidate_element[1]


def map_function(rdd_r_element: {__getitem__}) -> str:
    rdd_r_transformed_element = ",".join(str(data) for data in rdd_r_element)
    characters_to_remove = ["'", " ", "(", ")", "["]
    rdd_r_transformed_element = "".join((filter(lambda c: c not in characters_to_remove, rdd_r_transformed_element)))
    rdd_r_transformed_element = rdd_r_transformed_element.partition(",Diff_Sequences")[0]
    return rdd_r_transformed_element


class DifferentiatorRDD(Differentiator):

    def __init__(self,
                 differentiator_config_file: Path) -> None:
        super().__init__(differentiator_config_file)

    @staticmethod
    def __compress_as_archive_file(compressed_file: Path,
                                   root_directory: Path) -> None:
        with ZipFile(file=compressed_file, mode="w") as zip_file:
            for directory, sub_directories, files in walk(root_directory):
                pycache_directory = "__pycache__"
                if pycache_directory in sub_directories:
                    sub_directories.remove(pycache_directory)
                zip_file.write(directory)
                for file in files:
                    file_path = Path(directory).joinpath(file)
                    zip_file.write(file_path)

    @staticmethod
    def __remove_archive_file(compressed_file: Path) -> None:
        compressed_file.unlink()

    @staticmethod
    def __add_py_file_dependencies_to_spark_context(spark_context: SparkContext,
                                                    py_files_dependencies: list) -> None:
        for index in range(len(py_files_dependencies)):
            spark_context.addPyFile(str(py_files_dependencies[index]))

    def __create_rdd(self,
                     spark_context: SparkContext,
                     rdd_sequences_indices_list: list,
                     rdd_sequence_name: str,
                     rdd_data: list,
                     rdd_number_of_partitions: int) -> RDD:
        data_rows = []
        for index in range(len(rdd_data)):
            data_rows.append((rdd_data[index][0], (rdd_data[index][1], rdd_sequences_indices_list, rdd_sequence_name)))
        rdd = spark_context.parallelize(data_rows,
                                        numSlices=rdd_number_of_partitions)
        # Increase Map Tasks Count
        self.increase_map_tasks_count(rdd.getNumPartitions())
        return rdd

    @staticmethod
    def __execute_rdd_diff(united_rdd: RDD) -> RDD:
        # Execute ReduceByKey (Spark Wider-Shuffle Transformation) and
        #         Filter (Spark Narrow Transformation) Functions
        rdd_r = united_rdd.reduceByKey(lambda first_rdd_element, second_rdd_element:
                                       reduce_function(first_rdd_element, second_rdd_element)) \
                          .filter(lambda rdd_r_candidate_element: filter_function(rdd_r_candidate_element))
        return rdd_r

    def __execute_diff_phase(self,
                             united_rdd: RDD) -> RDD:
        # Execute RDD Diff (Resulting RDD: rdd_r)
        rdd_r = self.__execute_rdd_diff(united_rdd)
        return rdd_r

    @staticmethod
    def __collect_rdd(rdd: RDD,
                      ascending_boolean: bool) -> None:
        # Execute SortByKey (Spark Wider-Shuffle Transformation) and
        #         Collect (Spark Action) Functions
        rdd \
            .sortByKey(ascending=ascending_boolean)
        for data_element in rdd.collect():
            print(map_function(data_element))

    @staticmethod
    def __write_rdd_as_text_file(rdd: RDD,
                                 ascending_boolean: bool,
                                 destination_file_path: Path) -> None:
        # Execute SortByKey (Spark Wider-Shuffle Transformation),
        #         Map (Spark Narrow Transformation) and
        #         SaveAsTextFile (Spark Action) Functions
        rdd \
            .sortByKey(ascending=ascending_boolean) \
            .map(lambda rdd_r_element: map_function(rdd_r_element)) \
            .saveAsTextFile(str(destination_file_path))

    def __execute_collection_phase(self,
                                   rdd: RDD,
                                   collection_phase: str,
                                   destination_file_path: Path) -> None:
        # Set Scheduler Pool to 'fair_pool' (Takes Effect Only if Scheduler Mode is set to 'FAIR')
        self.set_scheduler_pool("fair_pool")
        if collection_phase == "None":
            # Do Not Collect Resulting RDD (rdd_r)
            pass
        elif collection_phase == "SC":  # SC = Show/Collect
            # Collect Resulting RDD (rdd_r) and Print on Command-Line Interface
            self.__collect_rdd(rdd,
                               True)
        elif collection_phase == "DW":  # DW = Distributed Write
            # Write to Disk Resulting RDD (rdd_r) as Multiple Partial Text Files
            # (Each Spark Executor Writes Its Partition's Data Locally)
            self.__write_rdd_as_text_file(rdd,
                                          True,
                                          destination_file_path)
        elif collection_phase == "MW":  # MW = Merged Write
            # Write to Disk Resulting RDD (rdd_r) as Single Text File
            # (Each Spark Executor Sends Its Partition's Data to One Executor Which Will Merge and Write Them)
            rdd = self.repartition_data_structure(rdd,
                                                  1)
            self.__write_rdd_as_text_file(rdd,
                                          True,
                                          destination_file_path)
        # Increase Reduce Tasks Count
        self.increase_reduce_tasks_count(rdd.getNumPartitions())

    def __produce_united_rdd(self,
                             producer_name: str,
                             sh: SequencesHandler,
                             sequences_list_text_file: Path,
                             partitioning: str,
                             spark_context: SparkContext) -> None:
        full_products_queue_waiting_timeout = self.get_full_products_queue_waiting_timeout()
        waiting_timeout_sec = self.convert_waiting_timeout_to_sec(full_products_queue_waiting_timeout)
        while True:
            self.sequences_indices_list_lock.acquire()
            if self.sequences_indices_list:
                # Get Next RDDs Sequences Indices
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
            # Get First RDD Sequences Indices List
            first_rdd_sequences_indices_list = sequences_indices[0]
            # Get Second RDD Sequences Indices List
            second_rdd_sequences_indices_list = sequences_indices[1]
            # Get First RDD Sequences Data List
            first_rdd_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                       first_rdd_sequences_indices_list)
            # Get Second RDD Sequences Data List
            second_rdd_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                        second_rdd_sequences_indices_list)
            # Get the Biggest Sequence Length Among RDDs
            biggest_sequence_length_among_rdd = \
                self.get_biggest_sequence_length_among_data_structures(first_rdd_sequences_data_list,
                                                                       second_rdd_sequences_data_list)
            # Set Length of First RDD
            first_rdd_length = biggest_sequence_length_among_rdd
            # Set Length of Second RDD
            second_rdd_length = biggest_sequence_length_among_rdd
            # Get Sequence Name of First RDD
            first_rdd_sequence_name = first_rdd_sequences_data_list[0][0]
            # Get Data of First RDD
            first_rdd_data = self.get_data_structure_data(RDD,
                                                          first_rdd_length,
                                                          first_rdd_sequences_data_list)
            # Create First RDD
            first_rdd_number_of_partitions = 0
            if partitioning == "Auto":
                first_rdd_number_of_partitions = number_of_available_map_cores
            elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
                k_i = self.get_k_i()
                first_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
            first_rdd = self.__create_rdd(spark_context,
                                          first_rdd_sequences_indices_list,
                                          first_rdd_sequence_name,
                                          first_rdd_data,
                                          first_rdd_number_of_partitions)
            # Get Sequence Name of Second RDD
            second_rdd_sequence_name = second_rdd_sequences_data_list[0][0]
            # Get Data of Second RDD
            second_rdd_data = self.get_data_structure_data(RDD,
                                                           second_rdd_length,
                                                           second_rdd_sequences_data_list)
            # Create Second RDD
            second_rdd_number_of_partitions = 0
            if partitioning == "Auto":
                second_rdd_number_of_partitions = number_of_available_map_cores
            elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
                k_i = self.get_k_i()
                second_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
            second_rdd = self.__create_rdd(spark_context,
                                           second_rdd_sequences_indices_list,
                                           second_rdd_sequence_name,
                                           second_rdd_data,
                                           second_rdd_number_of_partitions)
            # Unite RDDs
            united_rdd = first_rdd.union(second_rdd)
            # END OF MAP PHASE
            # Produce Item
            produced_item = [united_rdd,
                             first_rdd_sequences_indices_list,
                             second_rdd_sequences_indices_list]
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

    def __consume_united_rdd(self,
                             consumer_name: str,
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
                # Get United_RDD
                united_rdd = item_to_consume[0]
                # Execute Diff Phase
                rdd_r = self.__execute_diff_phase(united_rdd)
                # Get First Sequence Index of First RDD
                first_rdd_first_sequence_index = item_to_consume[1][0]
                # Get First Sequence Index of Second RDD
                second_rdd_first_sequence_index = item_to_consume[2][0]
                # Get Last Sequence Index of Second RDD
                second_rdd_last_sequence_index = item_to_consume[2][-1]
                # Get Destination File Path for Collection Phase
                collection_phase_destination_file_path = \
                    self.get_collection_phase_destination_file_path(output_directory,
                                                                    spark_app_name,
                                                                    spark_app_id,
                                                                    first_rdd_first_sequence_index,
                                                                    second_rdd_first_sequence_index,
                                                                    second_rdd_last_sequence_index)
                # Execute Collection Phase (Concurrent Spark Active Job)
                self.__execute_collection_phase(rdd_r,
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
        # Set Py Files Dependencies to be Copied to Worker Nodes (Required for ReduceByKey Function)
        py_files_dependencies = ["diff.py"]
        # Compress 'differentiator' Module as Archive File
        compressed_differentiator = Path("differentiator.zip")
        differentiator_path = Path("differentiator")
        self.__compress_as_archive_file(compressed_differentiator,
                                        differentiator_path)
        # Append Compressed 'differentiator' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_differentiator)
        # Compress 'thread_builder' Module as Archive File
        compressed_thread_builder = Path("thread_builder.zip")
        thread_builder_path = Path("thread_builder")
        self.__compress_as_archive_file(compressed_thread_builder,
                                        thread_builder_path)
        # Append Compressed 'thread_builder' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_thread_builder)
        # Compress 'sequences_handler' Module as Archive File
        compressed_sequences_handler = Path("sequences_handler.zip")
        sequences_handler_path = Path("sequences_handler")
        self.__compress_as_archive_file(compressed_sequences_handler,
                                        sequences_handler_path)
        # Append Compressed 'sequences_handler' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_sequences_handler)
        # Get Spark Context
        spark_context = self.get_spark_context()
        # Add Py Files Dependencies to SparkContext
        self.__add_py_file_dependencies_to_spark_context(spark_context,
                                                         py_files_dependencies)
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
                tb_producer_target_method = self.__produce_united_rdd
                tb_producer_name = "Producer_" + str(i)
                tb_producer_target_method_arguments = (tb_producer_name,
                                                       sh,
                                                       sequences_list_text_file,
                                                       partitioning,
                                                       spark_context)
                tb_producer_daemon_mode = False
                tb = ThreadBuilder(tb_producer_target_method,
                                   tb_producer_name,
                                   tb_producer_target_method_arguments,
                                   tb_producer_daemon_mode)
                producer = tb.build()
                producer.start()
            # Spawn Consumers Thread Pool
            for i in range(1, number_of_consumers + 1):
                tb_consumer_target_method = self.__consume_united_rdd
                tb_consumer_name = "Consumer_" + str(i)
                tb_consumer_target_method_arguments = (tb_consumer_name,
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
                # Get First RDD Sequences Indices List
                first_rdd_sequences_indices_list = sequences_indices_list[index_sequences_indices_list][0]
                # Get Second RDD Sequences Indices List
                second_rdd_sequences_indices_list = sequences_indices_list[index_sequences_indices_list][1]
                # Get First RDD Sequences Data List
                first_rdd_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                           first_rdd_sequences_indices_list)
                # Get Second RDD Sequences Data List
                second_rdd_sequences_data_list = sh.generate_sequences_list(sequences_list_text_file,
                                                                            second_rdd_sequences_indices_list)
                # Get the Biggest Sequence Length Among RDDs
                biggest_sequence_length_among_rdd = \
                    self.get_biggest_sequence_length_among_data_structures(first_rdd_sequences_data_list,
                                                                           second_rdd_sequences_data_list)
                # Set Length of First RDD
                first_rdd_length = biggest_sequence_length_among_rdd
                # Set Length of Second RDD
                second_rdd_length = biggest_sequence_length_among_rdd
                # Get Sequence Name of First RDD
                first_rdd_sequence_name = first_rdd_sequences_data_list[0][0]
                # Get Data of First RDD
                first_rdd_data = self.get_data_structure_data(RDD,
                                                              first_rdd_length,
                                                              first_rdd_sequences_data_list)
                # Create First RDD
                first_rdd_number_of_partitions = 0
                if partitioning == "Auto":
                    first_rdd_number_of_partitions = number_of_available_map_cores
                elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
                    k_i = self.get_k_i()
                    first_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
                first_rdd = self.__create_rdd(spark_context,
                                              first_rdd_sequences_indices_list,
                                              first_rdd_sequence_name,
                                              first_rdd_data,
                                              first_rdd_number_of_partitions)
                # Get Sequence Name of Second RDD
                second_rdd_sequence_name = second_rdd_sequences_data_list[0][0]
                # Get Data of Second RDD
                second_rdd_data = self.get_data_structure_data(RDD,
                                                               second_rdd_length,
                                                               second_rdd_sequences_data_list)
                # Create Second RDD
                second_rdd_number_of_partitions = 0
                if partitioning == "Auto":
                    second_rdd_number_of_partitions = number_of_available_map_cores
                elif partitioning == "Fixed_K" or partitioning == "Adaptive_K":
                    k_i = self.get_k_i()
                    second_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
                second_rdd = self.__create_rdd(spark_context,
                                               second_rdd_sequences_indices_list,
                                               second_rdd_sequence_name,
                                               second_rdd_data,
                                               second_rdd_number_of_partitions)
                # Unite RDDs
                united_rdd = first_rdd.union(second_rdd)
                # END OF MAP PHASE
                # BEGIN OF REDUCE PHASE
                # Execute Diff Phase
                rdd_r = self.__execute_diff_phase(united_rdd)
                # Get First Sequence Index of First RDD
                first_rdd_first_sequence_index = first_rdd_sequences_indices_list[0]
                # Get First Sequence Index of Second RDD
                second_rdd_first_sequence_index = second_rdd_sequences_indices_list[0]
                # Get Last Sequence Index of Second RDD
                second_rdd_last_sequence_index = second_rdd_sequences_indices_list[-1]
                # Get Destination File Path for Collection Phase
                collection_phase_destination_file_path = \
                    self.get_collection_phase_destination_file_path(output_directory,
                                                                    spark_app_name,
                                                                    spark_app_id,
                                                                    first_rdd_first_sequence_index,
                                                                    second_rdd_first_sequence_index,
                                                                    second_rdd_last_sequence_index)
                if allow_simultaneous_jobs_run:
                    # Execute Collection Phase Using Non-Daemonic Thread (Allows Concurrent Spark Active Jobs)
                    tb_collection_phase_target_method = self.__execute_collection_phase
                    tb_collection_phase_name = "Collection_Phase_" + str(index_sequences_indices_list)
                    tb_collection_phase_target_method_arguments = (rdd_r,
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
                    self.__execute_collection_phase(rdd_r,
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
                self.log_time_to_compare_sequences(first_rdd_first_sequence_index,
                                                   second_rdd_first_sequence_index,
                                                   second_rdd_last_sequence_index,
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
        # Delete Compressed 'differentiator' Module
        self.__remove_archive_file(compressed_differentiator)
        # Delete Compressed 'thread_builder' Module
        self.__remove_archive_file(compressed_thread_builder)
        # Delete Compressed 'sequences_handler' Module
        self.__remove_archive_file(compressed_sequences_handler)
