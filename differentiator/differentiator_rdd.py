from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from differentiator.exception.differentiator_rdd_exceptions import *
from logging import Logger
from math import inf, isclose
from os import walk
from pathlib import Path
from pyspark import RDD, SparkContext
from sequences_handler.sequences_handler import SequencesHandler
from time import time
from typing import Tuple, Union
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
        self.max_RDD = None
        self.partitioning = None

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
    def __read_max_RDD(differentiator_config_file: Path,
                       differentiator_config_parser: ConfigParser) -> Union[int, str]:
        exception_message = "{0}: 'max_RDD' must be a integer value in range [1, N-1]!" \
            .format(differentiator_config_file)
        try:
            max_RDD = str(differentiator_config_parser.get("RDD Settings",
                                                           "max_RDD"))
            if max_RDD != "N-1":
                max_RDD = int(max_RDD)
        except ValueError:
            raise InvalidMaxRDDError(exception_message)
        return max_RDD

    @staticmethod
    def __validate_max_RDD(max_RDD: Union[int, str]) -> None:
        exception_message = "Multiple Sequences RDDs must have at least 1 sequence."
        if max_RDD == "N-1":
            pass
        else:
            if max_RDD < 1:
                raise InvalidMaxRDDError(exception_message)

    def __set_max_RDD(self,
                      N: int,
                      max_RDD: Union[int, str]) -> None:
        if max_RDD == "N-1":
            self.max_RDD = N - 1
        else:
            self.max_RDD = max_RDD

    @staticmethod
    def __log_max_RDD(spark_app_name: str,
                      max_RDD: int,
                      logger: Logger) -> None:
        maximum_sequences_per_rdd_message = "({0}) Maximum Sequences Per RDD (max_RDD): {1}" \
            .format(spark_app_name,
                    str(max_RDD))
        print(maximum_sequences_per_rdd_message)
        logger.info(maximum_sequences_per_rdd_message)

    def __get_max_RDD(self) -> int:
        return self.max_RDD

    @staticmethod
    def __read_partitioning(differentiator_config_file: Path,
                            differentiator_config_parser: ConfigParser) -> str:
        exception_message = "{0}: 'partitioning' must be a string value!" \
            .format(differentiator_config_file)
        try:
            partitioning = \
                str(differentiator_config_parser.get("RDD Settings",
                                                     "partitioning"))
        except ValueError:
            raise InvalidPartitioningError(exception_message)
        return partitioning

    @staticmethod
    def __validate_partitioning(partitioning: str) -> None:
        supported_partitioning = ["auto", "adaptive"]
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
    def __find_divisors_set(divisible_number: int) -> set:
        k = set()
        for i in range(1, divisible_number + 1):
            if divisible_number % i == 0:
                k.add(i)
        return k

    @staticmethod
    def __create_rdd(spark_context: SparkContext,
                     rdd_sequence_name: str,
                     rdd_data: list) -> RDD:
        data_rows = []
        for index in range(len(rdd_data)):
            data_rows.append((rdd_data[index][0], (rdd_data[index][1], rdd_sequence_name)))
        return spark_context.parallelize(data_rows)

    @staticmethod
    def __partition_rdd(united_rdd: RDD,
                        available_cores: int) -> RDD:
        united_rdd_current_num_partitions = united_rdd.getNumPartitions()
        new_number_of_partitions = available_cores
        if united_rdd_current_num_partitions > new_number_of_partitions:
            # Execute Coalesce (Spark Less-Wide-Shuffle Transformation) Function
            united_rdd = united_rdd.coalesce(new_number_of_partitions)
        if united_rdd_current_num_partitions < new_number_of_partitions:
            # Execute Repartition (Spark Wider-Shuffle Transformation) Function
            united_rdd = united_rdd.repartition(new_number_of_partitions)
        return united_rdd

    @staticmethod
    def __partition_rdd_with_adaptive_approach(united_rdd: RDD,
                                               number_of_rdds_on_united_rdd: int,
                                               available_cores: int,
                                               k_i: int) -> RDD:
        united_rdd_current_num_partitions = united_rdd.getNumPartitions()
        new_number_of_partitions = int(number_of_rdds_on_united_rdd * available_cores / k_i)
        if united_rdd_current_num_partitions > new_number_of_partitions:
            # Execute Coalesce (Spark Less-Wide-Shuffle Transformation) Function
            united_rdd = united_rdd.coalesce(new_number_of_partitions)
        if united_rdd_current_num_partitions < new_number_of_partitions:
            # Execute Repartition (Spark Wider-Shuffle Transformation) Function
            united_rdd = united_rdd.repartition(new_number_of_partitions)
        return united_rdd

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
    def __maintain_or_update_variables_of_adaptive_partitioning(best_sequences_comparison_time: time,
                                                                time_to_compare_sequences_in_seconds: time,
                                                                absolute_tolerance: float,
                                                                k_i: int,
                                                                k_m_list_length: int) -> Tuple[int, float]:
        if best_sequences_comparison_time == inf:
            best_sequences_comparison_time = time_to_compare_sequences_in_seconds
        else:
            if not isclose(best_sequences_comparison_time,
                           time_to_compare_sequences_in_seconds,
                           abs_tol=absolute_tolerance):
                if 0 <= k_i < k_m_list_length - 1:
                    k_i = k_i + 1
            if best_sequences_comparison_time > time_to_compare_sequences_in_seconds:
                best_sequences_comparison_time = time_to_compare_sequences_in_seconds
        return k_i, best_sequences_comparison_time

    def diff_sequences(self) -> None:
        # Initialize Metrics Variables
        sequences_comparisons_count = 0
        spark_rdd_partitions_count = 0
        diff_phases_time_in_seconds = 0
        collection_phases_time_in_seconds = 0
        average_sequences_comparison_time_in_seconds = 0
        sequences_comparisons_time_in_seconds = 0
        # Get Spark Context
        spark_context = self.get_spark_context()
        # Define Py Files Dependencies to be Copied to Worker Nodes (Required for ReduceByKey Function)
        py_files_dependencies = ["diff.py"]
        # Compress 'differentiator' Module as Archive File
        compressed_differentiator = Path("differentiator.zip")
        differentiator_path = Path("differentiator")
        self.__compress_as_archive_file(compressed_differentiator,
                                        differentiator_path)
        # Append Compressed 'differentiator' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_differentiator)
        # Compress 'interval_timer' Module as Archive File
        compressed_interval_timer = Path("interval_timer.zip")
        interval_timer_path = Path("interval_timer")
        self.__compress_as_archive_file(compressed_interval_timer,
                                        interval_timer_path)
        # Append Compressed 'interval_timer' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_interval_timer)
        # Compress 'sequences_handler' Module as Archive File
        compressed_sequences_handler = Path("sequences_handler.zip")
        sequences_handler_path = Path("sequences_handler")
        self.__compress_as_archive_file(compressed_sequences_handler,
                                        sequences_handler_path)
        # Append Compressed 'sequences_handler' Module to Py Files Dependencies
        py_files_dependencies.append(compressed_sequences_handler)
        # Add Py Files Dependencies to SparkContext
        self.__add_py_file_dependencies_to_spark_context(spark_context,
                                                         py_files_dependencies)
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
            max_RDD = 1
            # Set Maximum Sequences Per RDD (max_RDD)
            self.__set_max_RDD(N,
                               max_RDD)
        elif diff_phase == "opt":
            # Read Maximum Sequences Per RDD (max_RDD)
            max_RDD = self.__read_max_RDD(differentiator_config_file,
                                          config_parser)
            # Validate Maximum Sequences Per RDD (max_RDD)
            self.__validate_max_RDD(max_RDD)
            # Set Maximum Sequences Per RDD (max_RDD)
            self.__set_max_RDD(N,
                               max_RDD)
        # Get Maximum Sequences Per RDD (max_RDD)
        max_RDD = self.__get_max_RDD()
        # Get Logger
        logger = self.get_logger()
        # Log Maximum Sequences Per RDD (max_RDD)
        self.__log_max_RDD(spark_app_name,
                           max_RDD,
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
        # Get Available Map Cores (Equals to Spark App Cores Max Count)
        available_map_cores = self.get_spark_app_cores_max_count(spark_context)
        # Get Available Reduce Cores (Equals to Spark App Cores Max Count)
        available_reduce_cores = self.get_spark_app_cores_max_count(spark_context)
        # Generate k_m List (List of Divisors of Available Map Cores), If Adaptive Mode is Enabled
        k_m_list = []
        k_m_list_length = 0
        if partitioning == "adaptive":
            # Get k_m Set for Map Phase
            k_m = self.__find_divisors_set(available_map_cores)
            # Get k_m List (Ordered k_m)
            k_m_list = sorted(k_m)
            # Get Length of k_m List
            k_m_list_length = len(k_m_list)
        # Set k_i (Initial Index of k_m_list)
        k_i = 0
        # Set Initial Best Sequences Comparison Time (∞)
        best_sequences_comparison_time = inf
        # Set Absolute Tolerance Between Sequences Comparison Times
        absolute_tolerance = 1
        # Get Estimated Amount of Diffs (d_a)
        estimated_d_a = self.estimate_amount_of_diffs(diff_phase,
                                                      N,
                                                      max_RDD)
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
                                                                    max_RDD)
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
        # Iterate Through Sequences Indices List
        for index_sequences_indices_list in range(actual_d_a):
            # Sequences Comparison Start Time
            sequences_comparison_start_time = time()
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
            first_rdd = self.__create_rdd(spark_context,
                                          first_rdd_sequence_name,
                                          first_rdd_data)
            # Get Sequence Name of Second RDD
            second_rdd_sequence_name = second_rdd_sequences_data_list[0][0]
            # Get Data of Second RDD
            second_rdd_data = self.get_data_structure_data(second_rdd_length,
                                                           second_rdd_sequences_data_list)
            # Create Second RDD
            second_rdd = self.__create_rdd(spark_context,
                                           second_rdd_sequence_name,
                                           second_rdd_data)
            # Unite RDDs
            united_rdd = first_rdd.union(second_rdd)
            # Set Number of RDDs on United RDD
            number_of_rdds_on_united_rdd = 2
            # Ensuring That R = r, i.e., Number of Reduce Tasks = Number of Available Cores for Reduce Phase (Diff)
            united_rdd = self.__partition_rdd(united_rdd,
                                              available_reduce_cores)
            # Partition United RDD, If Adaptive Mode is Enabled
            if partitioning == "adaptive":
                # Partition United RDD Considering Current Index (k_i) of Divisors List of Available Map Cores
                united_rdd = self.__partition_rdd_with_adaptive_approach(united_rdd,
                                                                         number_of_rdds_on_united_rdd,
                                                                         available_map_cores,
                                                                         k_m_list[k_i])
            # Get Number of Partitions of United RDD
            united_rdd_partitions_number = united_rdd.getNumPartitions()
            # Increase Spark RDD Partitions Count
            spark_rdd_partitions_count = spark_rdd_partitions_count + united_rdd_partitions_number
            # Diff Phase Start Time
            diff_phase_start_time = time()
            # Execute Diff Phase
            rdd_r = self.__execute_diff_phase(united_rdd)
            # Time to Execute Diff Phase in Seconds
            time_to_execute_diff_phase_in_seconds = time() - diff_phase_start_time
            # Increase Diff Phases Time
            diff_phases_time_in_seconds = diff_phases_time_in_seconds + time_to_execute_diff_phase_in_seconds
            # Increase Sequences Comparisons Count
            sequences_comparisons_count = sequences_comparisons_count + 1
            # Get Partition Number of Resulting RDD (rdd_r)
            rdd_r_partitions_number = rdd_r.getNumPartitions()
            # Increase Spark RDD Partitions Count
            spark_rdd_partitions_count = spark_rdd_partitions_count + rdd_r_partitions_number
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
            # Collection Phase Start Time
            collection_phase_start_time = time()
            # Execute Collection Phase
            self.__execute_collection_phase(rdd_r,
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
                                               first_rdd_first_sequence_index,
                                               second_rdd_first_sequence_index,
                                               second_rdd_last_sequence_index,
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
            # Maintain or Update Variables of Adaptive Partitioning, If Adaptive Mode is Enabled
            if partitioning == "adaptive":
                k_i, best_sequences_comparison_time = \
                    self.__maintain_or_update_variables_of_adaptive_partitioning(best_sequences_comparison_time,
                                                                                 time_to_compare_sequences_in_seconds,
                                                                                 absolute_tolerance,
                                                                                 k_i,
                                                                                 k_m_list_length)
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
        # Log Spark RDD Partitions Count
        self.log_spark_data_structure_partitions_count(spark_app_name,
                                                       data_structure,
                                                       spark_rdd_partitions_count,
                                                       logger)
        # Delete SequencesHandler Object
        del sh
        # Delete Compressed 'differentiator' Module
        self.__remove_archive_file(compressed_differentiator)
        # Delete Compressed 'interval_timer' Module
        self.__remove_archive_file(compressed_interval_timer)
        # Delete Compressed 'sequences_handler' Module
        self.__remove_archive_file(compressed_sequences_handler)
