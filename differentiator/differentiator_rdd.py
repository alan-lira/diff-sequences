from configparser import ConfigParser
from differentiator.differentiator import Differentiator
from os import walk
from pathlib import Path
from pyspark import RDD, SparkContext
from sequences_handler.sequences_handler import SequencesHandler
from time import time
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

    def __create_rdd(self,
                     spark_context: SparkContext,
                     rdd_sequence_name: str,
                     rdd_data: list,
                     rdd_number_of_partitions: int) -> RDD:
        data_rows = []
        for index in range(len(rdd_data)):
            data_rows.append((rdd_data[index][0], (rdd_data[index][1], rdd_sequence_name)))
        rdd = spark_context.parallelize(data_rows,
                                        numSlices=rdd_number_of_partitions)
        # Increase Map Tasks Count
        self.increase_map_tasks_count(rdd.getNumPartitions())
        return rdd

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
    def __write_rdd_as_text_file(rdd: RDD,
                                 ascending_boolean: bool,
                                 destination_file_path: Path) -> None:
        # Execute SortByKey (Spark Wider-Shuffle Transformation),
        #         Map (Spark Narrow Transformation) and
        #         SaveAsTextFile (Spark Action) Functions
        rdd \
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

    def diff_sequences(self) -> None:
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
                k_i = self.get_k_i()
                first_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
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
                k_i = self.get_k_i()
                second_rdd_number_of_partitions = int(number_of_available_map_cores / k_i)
            second_rdd = self.__create_rdd(spark_context,
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
            # Execute Collection Phase
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
        # Delete Compressed 'differentiator' Module
        self.__remove_archive_file(compressed_differentiator)
        # Delete Compressed 'thread_builder' Module
        self.__remove_archive_file(compressed_thread_builder)
        # Delete Compressed 'sequences_handler' Module
        self.__remove_archive_file(compressed_sequences_handler)
