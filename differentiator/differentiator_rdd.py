from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from logging import Logger
from math import inf
from os import walk
from pathlib import Path
from pyspark import RDD, SparkContext
from sequences_handler.sequences_handler import SequencesHandler
from time import time
from typing import Tuple
from zipfile import ZipFile


def diff_associative_reduce_function(x, y):
    result = None
    if x[0] is None or y[0] is None:
        pass
    if x[0] == y[0]:
        result = "=", "="
    if x[0] != y[0]:
        result = x[0], y[0]
    return result


def diff_filter_function(x):
    return "=" not in x[1] and None not in x[1]


def line_transformation(line):
    transformation = ",".join(str(data) for data in line)
    characters_to_remove = ["'", " ", "(", ")"]
    transformation = "".join((filter(lambda i: i not in characters_to_remove, transformation)))
    return transformation


class ResilientDistributedDatasetDifferentiator(Differentiator):

    def __init__(self) -> None:
        super().__init__()

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

    @staticmethod
    def __find_divisors_set(divisible_number: int) -> set:
        k = set()
        for i in range(1, divisible_number + 1):
            if divisible_number % i == 0:
                k.add(i)
        return k

    @staticmethod
    def __create_rdd(spark_context: SparkContext,
                     rdd_sequence_name: str,
                     rdd_data: list,
                     rdd_number_of_partitions: int) -> RDD:
        data_rows = []
        for index in range(len(rdd_data)):
            data_rows.append((rdd_data[index][0], (rdd_data[index][1], rdd_sequence_name)))
        return spark_context.parallelize(data_rows,
                                         numSlices=rdd_number_of_partitions)

    @staticmethod
    def __execute_rdd_diff(united_rdd: RDD) -> RDD:
        # Execute ReduceByKey (Spark Wider-Shuffle Transformation) and
        #         Filter (Spark Narrow Transformation) Functions
        rdd_r = united_rdd.reduceByKey(lambda x, y: diff_associative_reduce_function(x, y)) \
                          .filter(lambda x: diff_filter_function(x))
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
            print(line_transformation(data_element))

    @staticmethod
    def __write_rdd_as_distributed_partial_multiple_txt_files(rdd: RDD,
                                                              ascending_boolean: bool,
                                                              destination_file_path: Path) -> None:
        # Execute SortByKey (Spark Wider-Shuffle Transformation),
        #         Map (Spark Narrow Transformation) and
        #         SaveAsTextFile (Spark Action) Functions
        rdd \
            .sortByKey(ascending=ascending_boolean) \
            .map(line_transformation) \
            .saveAsTextFile(str(destination_file_path))

    @staticmethod
    def __write_rdd_as_merged_single_txt_file(rdd: RDD,
                                              ascending_boolean: bool,
                                              destination_file_path: Path) -> None:
        # Execute Coalesce (Spark Less-Wide-Shuffle Transformation),
        #         SortByKey (Spark Wider-Shuffle Transformation),
        #         Map (Spark Narrow Transformation) and
        #         SaveAsTextFile (Spark Action) Functions
        rdd \
            .coalesce(1) \
            .sortByKey(ascending=ascending_boolean) \
            .map(line_transformation) \
            .saveAsTextFile(str(destination_file_path))

    def __execute_collection_phase(self,
                                   rdd: RDD,
                                   collection_phase: str,
                                   destination_file_path: Path) -> None:
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
            self.__write_rdd_as_distributed_partial_multiple_txt_files(rdd,
                                                                       True,
                                                                       destination_file_path)
        elif collection_phase == "MW":  # MW = Merged Write
            # Write to Disk Resulting RDD (rdd_r) as Single Text File
            # (Each Spark Executor Sends Its Partition's Data to One Executor Which Will Merge and Write Them)
            self.__write_rdd_as_merged_single_txt_file(rdd,
                                                       True,
                                                       destination_file_path)

    @staticmethod
    def __find_k_opt_using_adaptive_partitioning(time_to_compare_sequences_in_seconds: time,
                                                 best_sequences_comparison_time_in_seconds: time,
                                                 k_list: list,
                                                 k_index: int,
                                                 k_i: int,
                                                 k_opt_found: bool) -> Tuple[int, int, int, bool]:
        if not k_opt_found:
            if best_sequences_comparison_time_in_seconds >= time_to_compare_sequences_in_seconds:
                best_sequences_comparison_time_in_seconds = time_to_compare_sequences_in_seconds
                k_index = k_index + 1
                if 0 <= k_index < len(k_list) - 1:
                    k_i = k_list[k_index]
                else:
                    k_opt_found = True
            else:
                k_index = k_index - 1
                k_i = k_list[k_index]
                k_opt_found = True
        return best_sequences_comparison_time_in_seconds, k_index, k_i, k_opt_found

    @staticmethod
    def __log_k_opt(k_opt: int,
                    logger: Logger) -> None:
        k_opt_message = "K Optimal: {0}" \
            .format(str(k_opt))
        logger.info(k_opt_message)

    def diff_sequences(self) -> None:
        # Initialize Metrics Variables
        diff_phase_partitions_count = 0
        collection_phase_partitions_count = 0
        sequences_comparisons_count = 0
        sequences_comparisons_time_in_seconds = 0
        sequences_comparisons_average_time_in_seconds = 0
        # Define Py Files Dependencies to be Copied to Worker Nodes (Required for ReduceByKey Function)
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
        self.set_N(sequences_list_length)
        # Get Number of Sequences to Compare (N)
        N = self.get_N()
        # Get Logger
        logger = self.get_logger()
        # Log Number of Sequences to Compare (N)
        self.log_N(N,
                   logger)
        if diff_phase == "1":
            # Set Maximum of One Sequence per RDD
            max_s = 1
            # Set Maximum Sequences Per RDD (maxₛ)
            self.set_max_s(N,
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
            self.set_max_s(N,
                           max_s)
        # Get Maximum Sequences Per RDD (maxₛ)
        max_s = self.get_max_s()
        # Log Maximum Sequences Per RDD (maxₛ)
        self.log_max_s(data_structure,
                       max_s,
                       logger)
        # Estimate Total Number of Diffs (Dₐ Estimation)
        estimated_d_a = self.estimate_total_number_of_diffs(diff_phase,
                                                            N,
                                                            max_s)
        # Log Dₐ Estimation
        self.log_estimated_total_number_of_diffs(estimated_d_a,
                                                 logger)
        # Generate Sequences Indices List
        sequences_indices_list = sh.generate_sequences_indices_list(N,
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
        # Initialize Variables Used to Find 'k_opt' (Local Optimal 'k_i' that Minimizes the Sequences Comparison Time)
        best_sequences_comparison_time_in_seconds = inf
        k_list = []
        k_index = 0
        k_i = 1
        k_opt_found = False
        if partitioning == "adaptive":
            # Get Number of Available Map Cores (Equals to Total Number of Cores of the Current Executors)
            number_of_available_map_cores = self.get_total_number_of_cores_of_the_current_executors()
            # Find K Set (Set of All Divisors of the Number of Available Map Cores)
            k = self.__find_divisors_set(number_of_available_map_cores)
            # Get List from K Set (Ordered K)
            k_list = sorted(k)
            # Set Initial k_i (Initial k_i of k_list)
            if 0 <= k_index < len(k_list) - 1:
                k_i = k_list[k_index]
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
            first_rdd_data = self.get_data_structure_data(first_rdd_length,
                                                          first_rdd_sequences_data_list)
            # Create First RDD
            first_rdd_number_of_partitions = 0
            if partitioning == "auto":
                first_rdd_number_of_partitions = number_of_available_map_cores
            elif partitioning == "adaptive":
                first_rdd_number_of_partitions = number_of_available_map_cores / k_i
            first_rdd = self.__create_rdd(spark_context,
                                          first_rdd_sequence_name,
                                          first_rdd_data,
                                          first_rdd_number_of_partitions)
            # Get Sequence Name of Second RDD
            second_rdd_sequence_name = second_rdd_sequences_data_list[0][0]
            # Get Data of Second RDD
            second_rdd_data = self.get_data_structure_data(second_rdd_length,
                                                           second_rdd_sequences_data_list)
            # Create Second RDD
            second_rdd_number_of_partitions = 0
            if partitioning == "auto":
                second_rdd_number_of_partitions = number_of_available_map_cores
            elif partitioning == "adaptive":
                second_rdd_number_of_partitions = number_of_available_map_cores / k_i
            second_rdd = self.__create_rdd(spark_context,
                                           second_rdd_sequence_name,
                                           second_rdd_data,
                                           second_rdd_number_of_partitions)
            # Unite RDDs
            united_rdd = first_rdd.union(second_rdd)
            # Get Number of Partitions of United RDD
            united_rdd_partitions_number = united_rdd.getNumPartitions()
            # END OF MAP PHASE
            # BEGIN OF REDUCE PHASE
            # Increase Diff Phase Partitions Count
            diff_phase_partitions_count = diff_phase_partitions_count + united_rdd_partitions_number
            # Execute Diff Phase
            rdd_r = self.__execute_diff_phase(united_rdd)
            # Increase Sequences Comparisons Count
            sequences_comparisons_count = sequences_comparisons_count + 1
            # Get Partition Number of Resulting RDD (rdd_r)
            rdd_r_partitions_number = rdd_r.getNumPartitions()
            # Increase Collection Phase Partitions Count
            collection_phase_partitions_count = collection_phase_partitions_count + rdd_r_partitions_number
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
            # Execute Collection Phase
            self.__execute_collection_phase(rdd_r,
                                            collection_phase,
                                            collection_phase_destination_file_path)
            # END OF REDUCE PHASE
            # Time to Compare Sequences in Seconds
            time_to_compare_sequences_in_seconds = time() - sequences_comparison_start_time
            # Increase Sequences Comparisons Time
            sequences_comparisons_time_in_seconds = \
                sequences_comparisons_time_in_seconds + time_to_compare_sequences_in_seconds
            # Log Time to Compare Sequences
            self.log_time_to_compare_sequences(first_rdd_first_sequence_index,
                                               second_rdd_first_sequence_index,
                                               second_rdd_last_sequence_index,
                                               time_to_compare_sequences_in_seconds,
                                               current_number_of_executors,
                                               total_number_of_cores_of_the_current_executors,
                                               converted_total_amount_of_memory_of_the_current_executors,
                                               logger)
            # Get Number of Sequences Comparisons Left
            number_of_sequences_comparisons_left = \
                self.get_number_of_sequences_comparisons_left(actual_d_a,
                                                              sequences_comparisons_count)
            # Get Average Sequences Comparison Time
            sequences_comparisons_average_time_in_seconds = \
                self.get_average_sequences_comparison_time(sequences_comparisons_time_in_seconds,
                                                           sequences_comparisons_count)
            # Estimate Time Left
            estimated_time_left_in_seconds = self.estimate_time_left(number_of_sequences_comparisons_left,
                                                                     sequences_comparisons_average_time_in_seconds)
            # Print Real Time Metrics
            self.print_real_time_metrics(spark_app_name,
                                         sequences_comparisons_count,
                                         number_of_sequences_comparisons_left,
                                         sequences_comparisons_average_time_in_seconds,
                                         estimated_time_left_in_seconds)
            # Search For 'k_opt', If Not Found Yet
            if partitioning == "adaptive":
                best_sequences_comparison_time_in_seconds, k_index, k_i, k_opt_found = \
                    self.__find_k_opt_using_adaptive_partitioning(time_to_compare_sequences_in_seconds,
                                                                  best_sequences_comparison_time_in_seconds,
                                                                  k_list,
                                                                  k_index,
                                                                  k_i,
                                                                  k_opt_found)
        # Log Sequences Comparisons Average Time
        self.log_sequences_comparisons_average_time(data_structure,
                                                    sequences_comparisons_average_time_in_seconds,
                                                    logger)
        # Log 'k_opt'
        if partitioning != "auto":
            self.__log_k_opt(k_i,
                             logger)
        # Log Diff Phase Partitions Count
        self.log_partitions_count("Diff",
                                  diff_phase_partitions_count,
                                  logger)
        # Log Collection Phase Partitions Count
        self.log_partitions_count("Collection",
                                  collection_phase_partitions_count,
                                  logger)
        # Delete SequencesHandler Object
        del sh
        # Delete Compressed 'differentiator' Module
        self.__remove_archive_file(compressed_differentiator)
        # Delete Compressed 'thread_builder' Module
        self.__remove_archive_file(compressed_thread_builder)
        # Delete Compressed 'sequences_handler' Module
        self.__remove_archive_file(compressed_sequences_handler)
