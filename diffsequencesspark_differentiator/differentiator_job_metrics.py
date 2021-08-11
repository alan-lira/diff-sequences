from differentiator_exceptions import InvalidNumberOfArgumentsError
from pathlib import Path
import json
import wget


class SparkJobMetrics:

    def __init__(self) -> None:
        self.metrics_directory_path = None
        self.app_metrics_directory_path = None
        self.jobs_metrics_url = None
        self.jobs_metrics_destination_file_path = None
        self.stages_metrics_url = None
        self.stages_metrics_destination_file_path = None
        self.executors_metrics_url = None
        self.executors_metrics_destination_file_path = None
        self.storage_metrics_url = None
        self.storage_metrics_destination_file_path = None


class ExecutorLogs:

    def __init__(self,
                 stdout,  # 'stdout'
                 stderr) -> None:  # 'stderr'
        self.stdout = stdout
        self.stderr = stderr


class ExecutorMetrics:

    def __init__(self,
                 executor_id,  # 'id'
                 host_port,  # 'hostPort'
                 is_active,  # 'isActive'
                 rdd_blocks,  # 'rddBlocks'
                 memory_used,  # 'memoryUsed'
                 disk_used,  # 'diskUsed'
                 total_cores,  # 'totalCores'
                 max_tasks,  # 'maxTasks'
                 active_tasks,  # 'activeTasks'
                 failed_tasks,  # 'failedTasks'
                 completed_tasks,  # 'completedTasks'
                 total_tasks,  # 'totalTasks'
                 total_duration,  # 'totalDuration'
                 total_gc_time,  # 'totalGCTime'
                 total_input_bytes,  # 'totalInputBytes'
                 total_shuffle_read,  # 'totalShuffleRead'
                 total_shuffle_write,  # 'totalShuffleWrite'
                 is_blacklisted,  # 'isBlacklisted'
                 max_memory,  # 'maxMemory'
                 add_time,  # 'addTime'
                 executor_logs,  # 'executorLogs'
                 memory_metrics,  # 'memoryMetrics'
                 blacklisted_in_stages,  # 'blacklistedInStages'
                 peak_memory_metrics,  # 'peakMemoryMetrics'
                 attributes,  # 'attributes'
                 resources,  # 'resources'
                 resource_profile_id,  # 'resourceProfileId'
                 is_excluded,  # 'isExcluded'
                 excluded_in_stages) -> None:  # 'excludedInStages'
        self.executor_id = executor_id
        self.host_port = host_port
        self.is_active = is_active
        self.rdd_blocks = rdd_blocks
        self.memory_used = memory_used
        self.disk_used = disk_used
        self.total_cores = total_cores
        self.max_tasks = max_tasks
        self.active_tasks = active_tasks
        self.failed_tasks = failed_tasks
        self.completed_tasks = completed_tasks
        self.total_tasks = total_tasks
        self.total_duration = total_duration
        self.total_gc_time = total_gc_time
        self.total_input_bytes = total_input_bytes
        self.total_shuffle_read = total_shuffle_read
        self.total_shuffle_write = total_shuffle_write
        self.is_blacklisted = is_blacklisted
        self.max_memory = max_memory
        self.add_time = add_time
        self.executor_logs = executor_logs
        self.memory_metrics = memory_metrics
        self.blacklisted_in_stages = blacklisted_in_stages
        self.peak_memory_metrics = peak_memory_metrics
        self.attributes = attributes
        self.resources = resources
        self.resource_profile_id = resource_profile_id
        self.is_excluded = is_excluded
        self.excluded_in_stages = excluded_in_stages


class PeakyExecutorMetrics:

    def __init__(self,
                 jvm_heap_memory,  # 'JVMHeapMemory'
                 jvm_off_heap_memory,  # 'JVMOffHeapMemory'
                 on_heap_execution_memory,  # 'OnHeapExecutionMemory'
                 off_heap_execution_memory,  # 'OffHeapExecutionMemory'
                 on_heap_storage_memory,  # 'OnHeapStorageMemory'
                 off_heap_storage_memory,  # 'OffHeapStorageMemory'
                 on_heap_unified_memory,  # 'OnHeapUnifiedMemory'
                 off_heap_unified_memory,  # 'OffHeapUnifiedMemory'
                 direct_pool_memory,  # 'DirectPoolMemory'
                 mapped_pool_memory,  # 'MappedPoolMemory'
                 process_tree_jvm_memory,  # 'ProcessTreeJVMVMemory'
                 process_tree_jvm_rss_memory,  # 'ProcessTreeJVMRSSMemory'
                 process_tree_python_v_memory,  # 'ProcessTreePythonVMemory'
                 process_tree_python_rss_memory,  # 'ProcessTreePythonRSSMemory'
                 process_tree_other_v_memory,  # 'ProcessTreeOtherVMemory'
                 process_tree_other_rss_memory,  # 'ProcessTreeOtherRSSMemory'
                 minor_gc_count,  # 'MinorGCCount'
                 minor_gc_time,  # 'MinorGCTime'
                 major_gc_count,  # 'MajorGCCount'
                 major_gc_time) -> None:  # 'MajorGCTime'
        self.jvm_heap_memory = jvm_heap_memory
        self.jvm_off_heap_memory = jvm_off_heap_memory
        self.on_heap_execution_memory = on_heap_execution_memory
        self.off_heap_execution_memory = off_heap_execution_memory
        self.on_heap_storage_memory = on_heap_storage_memory
        self.off_heap_storage_memory = off_heap_storage_memory
        self.on_heap_unified_memory = on_heap_unified_memory
        self.off_heap_unified_memory = off_heap_unified_memory
        self.direct_pool_memory = direct_pool_memory
        self.mapped_pool_memory = mapped_pool_memory
        self.process_tree_jvm_memory = process_tree_jvm_memory
        self.process_tree_jvm_rss_memory = process_tree_jvm_rss_memory
        self.process_tree_python_v_memory = process_tree_python_v_memory
        self.process_tree_python_rss_memory = process_tree_python_rss_memory
        self.process_tree_other_v_memory = process_tree_other_v_memory
        self.process_tree_other_rss_memory = process_tree_other_rss_memory
        self.minor_gc_count = minor_gc_count
        self.minor_gc_time = minor_gc_time
        self.major_gc_count = major_gc_count
        self.major_gc_time = major_gc_time


class JobMetrics:

    def __init__(self,
                 job_id,  # 'jobId'
                 name,  # 'name'
                 submission_time,  # 'submissionTime'
                 completion_time,  # 'completionTime'
                 stage_ids,  # 'stageIds'
                 status,  # 'status'
                 num_tasks,  # 'numTasks'
                 num_active_tasks,  # 'numActiveTasks'
                 num_completed_tasks,  # 'numCompletedTasks'
                 num_skipped_tasks,  # 'numSkippedTasks'
                 num_failed_tasks,  # 'numFailedTasks'
                 num_killed_tasks,  # 'numKilledTasks'
                 num_completed_indices,  # 'numCompletedIndices'
                 num_active_stages,  # 'numActiveStages'
                 num_completed_stages,  # 'numCompletedStages'
                 num_skipped_stages,  # 'numSkippedStages'
                 num_failed_stages,  # 'numFailedStages'
                 killed_tasks_summary) -> None:  # 'killedTasksSummary'
        self.job_id = job_id
        self.name = name
        self.submission_time = submission_time
        self.completion_time = completion_time
        self.stage_ids = stage_ids
        self.status = status
        self.num_tasks = num_tasks
        self.num_active_tasks = num_active_tasks
        self.num_completed_tasks = num_completed_tasks
        self.num_skipped_tasks = num_skipped_tasks
        self.num_failed_tasks = num_failed_tasks
        self.num_killed_tasks = num_killed_tasks
        self.num_completed_indices = num_completed_indices
        self.num_active_stages = num_active_stages
        self.num_completed_stages = num_completed_stages
        self.num_skipped_stages = num_skipped_stages
        self.num_failed_stages = num_failed_stages
        self.killed_tasks_summary = killed_tasks_summary


class MemoryMetrics:

    def __init__(self,
                 used_on_heap_storage_memory,  # 'usedOnHeapStorageMemory'
                 used_off_heap_storage_memory,  # 'usedOffHeapStorageMemory'
                 total_on_heap_storage_memory,  # 'totalOnHeapStorageMemory'
                 total_off_heap_storage_memory) -> None:  # 'totalOffHeapStorageMemory'
        self.used_on_heap_storage_memory = used_on_heap_storage_memory
        self.used_off_heap_storage_memory = used_off_heap_storage_memory
        self.total_on_heap_storage_memory = total_on_heap_storage_memory
        self.total_off_heap_storage_memory = total_off_heap_storage_memory


class PeakyMemoryMetrics:

    def __init__(self,
                 jvm_heap_memory,  # 'JVMHeapMemory'
                 jvm_off_heap_memory,  # 'JVMOffHeapMemory'
                 on_heap_execution_memory,  # 'OnHeapExecutionMemory'
                 off_heap_execution_memory,  # 'OffHeapExecutionMemory'
                 on_heap_storage_memory,  # 'OnHeapStorageMemory'
                 off_heap_storage_memory,  # 'OffHeapStorageMemory'
                 on_heap_unified_memory,  # 'OnHeapUnifiedMemory'
                 off_heap_unified_memory,  # 'OffHeapUnifiedMemory'
                 direct_pool_memory,  # 'DirectPoolMemory'
                 mapped_pool_memory,  # 'MappedPoolMemory'
                 process_tree_jvm_memory,  # 'ProcessTreeJVMVMemory'
                 process_tree_jvm_rss_memory,  # 'ProcessTreeJVMRSSMemory'
                 process_tree_python_v_memory,  # 'ProcessTreePythonVMemory'
                 process_tree_python_rss_memory,  # 'ProcessTreePythonRSSMemory'
                 process_tree_other_v_memory,  # 'ProcessTreeOtherVMemory'
                 process_tree_other_rss_memory,  # 'ProcessTreeOtherRSSMemory'
                 minor_gc_count,  # 'MinorGCCount'
                 minor_gc_time,  # 'MinorGCTime'
                 major_gc_count,  # 'MajorGCCount'
                 major_gc_time) -> None:  # 'MajorGCTime'
        self.jvm_heap_memory = jvm_heap_memory
        self.jvm_off_heap_memory = jvm_off_heap_memory
        self.on_heap_execution_memory = on_heap_execution_memory
        self.off_heap_execution_memory = off_heap_execution_memory
        self.on_heap_storage_memory = on_heap_storage_memory
        self.off_heap_storage_memory = off_heap_storage_memory
        self.on_heap_unified_memory = on_heap_unified_memory
        self.off_heap_unified_memory = off_heap_unified_memory
        self.direct_pool_memory = direct_pool_memory
        self.mapped_pool_memory = mapped_pool_memory
        self.process_tree_jvm_memory = process_tree_jvm_memory
        self.process_tree_jvm_rss_memory = process_tree_jvm_rss_memory
        self.process_tree_python_v_memory = process_tree_python_v_memory
        self.process_tree_python_rss_memory = process_tree_python_rss_memory
        self.process_tree_other_v_memory = process_tree_other_v_memory
        self.process_tree_other_rss_memory = process_tree_other_rss_memory
        self.minor_gc_count = minor_gc_count
        self.minor_gc_time = minor_gc_time
        self.major_gc_count = major_gc_count
        self.major_gc_time = major_gc_time


class StageMetrics:

    def __init__(self,
                 status,  # 'status'
                 stage_id,  # 'stageId'
                 attempt_id,  # 'attemptId'
                 num_tasks,  # 'numTasks'
                 num_active_tasks,  # 'numActiveTasks'
                 num_complete_tasks,  # 'numCompleteTasks'
                 num_failed_tasks,  # 'numFailedTasks'
                 num_killed_tasks,  # 'numKilledTasks'
                 num_completed_indices,  # 'numCompletedIndices'
                 submission_time,  # 'submissionTime'
                 first_task_launched_time,  # 'firstTaskLaunchedTime'
                 completion_time,  # 'completionTime'
                 executor_deserialize_time,  # 'executorDeserializeTime'
                 executor_deserialize_cpu_time,  # 'executorDeserializeCpuTime'
                 executor_run_time,  # 'executorRunTime'
                 executor_cpu_time,  # 'executorCpuTime'
                 result_size,  # 'resultSize'
                 jvm_gc_time,  # 'jvmGcTime'
                 result_serialization_time,  # 'resultSerializationTime'
                 memory_bytes_spilled,  # 'memoryBytesSpilled'
                 disk_bytes_spilled,  # 'diskBytesSpilled'
                 peak_execution_memory,  # 'peakExecutionMemory'
                 input_bytes,  # 'inputBytes'
                 input_records,  # 'inputRecords'
                 output_bytes,  # 'outputBytes'
                 output_records,  # 'outputRecords'
                 shuffle_remote_blocks_fetched,  # 'shuffleRemoteBlocksFetched'
                 shuffle_local_blocks_fetched,  # 'shuffleLocalBlocksFetched'
                 shuffle_fetch_wait_time,  # 'shuffleFetchWaitTime'
                 shuffle_remote_bytes_read,  # 'shuffleRemoteBytesRead'
                 shuffle_remote_bytes_read_to_disk,  # 'shuffleRemoteBytesReadToDisk'
                 shuffle_local_bytes_read,  # 'shuffleLocalBytesRead'
                 shuffle_read_bytes,  # 'shuffleReadBytes'
                 shuffle_read_records,  # 'shuffleReadRecords'
                 shuffle_write_bytes,  # 'shuffleWriteBytes'
                 shuffle_write_time,  # 'shuffleWriteTime'
                 shuffle_write_records,  # 'shuffleWriteRecords'
                 name,  # 'name'
                 details,  # 'details'
                 scheduling_pool,  # 'schedulingPool'
                 rdd_ids,  # 'rddIds'
                 accumulator_updates,  # 'accumulatorUpdates'
                 killed_tasks_summary,  # 'killedTasksSummary'
                 resource_profile_id,  # 'resourceProfileId'
                 peak_executor_metrics) -> None:  # 'peakExecutorMetrics'
        self.status = status
        self.stage_id = stage_id
        self.attempt_id = attempt_id
        self.num_tasks = num_tasks
        self.num_active_tasks = num_active_tasks
        self.num_complete_tasks = num_complete_tasks
        self.num_failed_tasks = num_failed_tasks
        self.num_killed_tasks = num_killed_tasks
        self.num_completed_indices = num_completed_indices
        self.submission_time = submission_time
        self.first_task_launched_time = first_task_launched_time
        self.completion_time = completion_time
        self.executor_deserialize_time = executor_deserialize_time
        self.executor_deserialize_cpu_time = executor_deserialize_cpu_time
        self.executor_run_time = executor_run_time
        self.executor_cpu_time = executor_cpu_time
        self.result_size = result_size
        self.jvm_gc_time = jvm_gc_time
        self.result_serialization_time = result_serialization_time
        self.memory_bytes_spilled = memory_bytes_spilled
        self.disk_bytes_spilled = disk_bytes_spilled
        self.peak_execution_memory = peak_execution_memory
        self.input_bytes = input_bytes
        self.input_records = input_records
        self.output_bytes = output_bytes
        self.output_records = output_records
        self.shuffle_remote_blocks_fetched = shuffle_remote_blocks_fetched
        self.shuffle_local_blocks_fetched = shuffle_local_blocks_fetched
        self.shuffle_fetch_wait_time = shuffle_fetch_wait_time
        self.shuffle_remote_bytes_read = shuffle_remote_bytes_read
        self.shuffle_remote_bytes_read_to_disk = shuffle_remote_bytes_read_to_disk
        self.shuffle_local_bytes_read = shuffle_local_bytes_read
        self.shuffle_read_bytes = shuffle_read_bytes
        self.shuffle_read_records = shuffle_read_records
        self.shuffle_write_bytes = shuffle_write_bytes
        self.shuffle_write_time = shuffle_write_time
        self.shuffle_write_records = shuffle_write_records
        self.name = name
        self.details = details
        self.scheduling_pool = scheduling_pool
        self.rdd_ids = rdd_ids
        self.accumulator_updates = accumulator_updates
        self.killed_tasks_summary = killed_tasks_summary
        self.resource_profile_id = resource_profile_id
        self.peak_executor_metrics = peak_executor_metrics


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 4:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 3 Arguments: {0}, {1} and {2}. \n" \
            "Provided: {3} Argument(s)." \
            .format("spark_driver_host",
                    "spark_app_id",
                    "spark_ui_port",
                    number_of_arguments_provided - 1)
        raise InvalidNumberOfArgumentsError(invalid_number_of_arguments_message)


def load_spark_job_metrics_parameters(sjm: SparkJobMetrics,
                                      spark_driver_host: str,
                                      spark_app_name: str,
                                      spark_app_id: str,
                                      spark_ui_port: str,
                                      metrics_directory_path: Path) -> None:
    # READ METRICS DIRECTORY PATH
    sjm.metrics_directory_path = Path(metrics_directory_path).joinpath(spark_app_name)

    # CREATE METRICS DIRECTORY (IF NOT EXISTS)
    if not sjm.metrics_directory_path.exists():
        sjm.metrics_directory_path.mkdir()

    # CREATE APP METRICS DIRECTORY (IF NOT EXISTS)
    sjm.app_metrics_directory_path = sjm.metrics_directory_path.joinpath(Path(spark_app_id))
    if not sjm.app_metrics_directory_path.exists():
        sjm.app_metrics_directory_path.mkdir()

    # READ JOBS METRICS URL
    sjm.jobs_metrics_url = "http://{0}:{1}/api/v1/applications/{2}/jobs" \
        .format(spark_driver_host,
                spark_ui_port,
                spark_app_id)

    # READ JOBS METRICS DESTINATION FILE PATH
    sjm.jobs_metrics_destination_file_path = Path(sjm.app_metrics_directory_path).joinpath("jobs_metrics.json")

    # READ STAGES METRICS URL
    sjm.stages_metrics_url = "http://{0}:{1}/api/v1/applications/{2}/stages" \
        .format(spark_driver_host,
                spark_ui_port,
                spark_app_id)

    # READ STAGES METRICS DESTINATION FILE PATH
    sjm.stages_metrics_destination_file_path = Path(sjm.app_metrics_directory_path).joinpath("stages_metrics.json")

    # READ EXECUTORS METRICS URL
    sjm.executors_metrics_url = "http://{0}:{1}/api/v1/applications/{2}/executors" \
        .format(spark_driver_host,
                spark_ui_port,
                spark_app_id)

    # READ EXECUTORS METRICS DESTINATION FILE PATH
    sjm.executors_metrics_destination_file_path = \
        Path(sjm.app_metrics_directory_path).joinpath("executors_metrics.json")

    # READ STORAGE METRICS URL
    sjm.storage_metrics_url = "http://{0}:{1}/api/v1/applications/{2}/storage/rdd" \
        .format(spark_driver_host,
                spark_ui_port,
                spark_app_id)

    # READ STORAGE METRICS DESTINATION FILE PATH
    sjm.storage_metrics_destination_file_path = Path(sjm.app_metrics_directory_path).joinpath("storage_metrics.json")


def get_json_object_list(json_file_path: Path) -> list:
    with open(json_file_path, mode="r") as json_file:
        json_objects = json_file.read()
    json_objects_list = json.loads(json_objects)
    return json_objects_list


def parse_jobs_json_list(json_file_path: Path) -> list:
    jobs_json_list = get_json_object_list(json_file_path)
    jobs_metrics_list = []
    for job_json in jobs_json_list:
        job_id = None
        if "jobId" in job_json:
            job_id = job_json["jobId"]
        name = None
        if "name" in job_json:
            name = job_json["name"]
        submission_time = None
        if "submissionTime" in job_json:
            submission_time = job_json["submissionTime"]
        completion_time = None
        if "completionTime" in job_json:
            completion_time = job_json["completionTime"]
        stage_ids = None
        if "stageIds" in job_json:
            stage_ids = job_json["stageIds"]
        status = None
        if "status" in job_json:
            status = job_json["status"]
        num_tasks = None
        if "numTasks" in job_json:
            num_tasks = job_json["numTasks"]
        num_active_tasks = None
        if "numActiveTasks" in job_json:
            num_active_tasks = job_json["numActiveTasks"]
        num_completed_tasks = None
        if "numCompletedTasks" in job_json:
            num_completed_tasks = job_json["numCompletedTasks"]
        num_skipped_tasks = None
        if "numSkippedTasks" in job_json:
            num_skipped_tasks = job_json["numSkippedTasks"]
        num_failed_tasks = None
        if "numFailedTasks" in job_json:
            num_failed_tasks = job_json["numFailedTasks"]
        num_killed_tasks = None
        if "numKilledTasks" in job_json:
            num_killed_tasks = job_json["numKilledTasks"]
        num_completed_indices = None
        if "numCompletedIndices" in job_json:
            num_completed_indices = job_json["numCompletedIndices"]
        num_active_stages = None
        if "numActiveStages" in job_json:
            num_active_stages = job_json["numActiveStages"]
        num_completed_stages = None
        if "numCompletedStages" in job_json:
            num_completed_stages = job_json["numCompletedStages"]
        num_skipped_stages = None
        if "numSkippedStages" in job_json:
            num_skipped_stages = job_json["numSkippedStages"]
        num_failed_stages = None
        if "numFailedStages" in job_json:
            num_failed_stages = job_json["numFailedStages"]
        killed_tasks_summary = None
        if "killedTasksSummary" in job_json:
            killed_tasks_summary = job_json["killedTasksSummary"]
        job_metrics = JobMetrics(job_id,
                                 name,
                                 submission_time,
                                 completion_time,
                                 stage_ids,
                                 status,
                                 num_tasks,
                                 num_active_tasks,
                                 num_completed_tasks,
                                 num_skipped_tasks,
                                 num_failed_tasks,
                                 num_killed_tasks,
                                 num_completed_indices,
                                 num_active_stages,
                                 num_completed_stages,
                                 num_skipped_stages,
                                 num_failed_stages,
                                 killed_tasks_summary)
        jobs_metrics_list.append(job_metrics)
    return jobs_metrics_list


def parse_stages_json_list(json_file_path: Path) -> list:
    stages_json_list = get_json_object_list(json_file_path)
    stages_metrics_list = []
    for stage_json in stages_json_list:
        status = None
        if "status" in stage_json:
            status = stage_json["status"]
        stage_id = None
        if "stageId" in stage_json:
            stage_id = stage_json["stageId"]
        attempt_id = None
        if "attemptId" in stage_json:
            attempt_id = stage_json["attemptId"]
        num_tasks = None
        if "numTasks" in stage_json:
            num_tasks = stage_json["numTasks"]
        num_active_tasks = None
        if "numActiveTasks" in stage_json:
            num_active_tasks = stage_json["numActiveTasks"]
        num_complete_tasks = None
        if "numCompleteTasks" in stage_json:
            num_complete_tasks = stage_json["numCompleteTasks"]
        num_failed_tasks = None
        if "numFailedTasks" in stage_json:
            num_failed_tasks = stage_json["numFailedTasks"]
        num_killed_tasks = None
        if "numKilledTasks" in stage_json:
            num_killed_tasks = stage_json["numKilledTasks"]
        num_completed_indices = None
        if "numCompletedIndices" in stage_json:
            num_completed_indices = stage_json["numCompletedIndices"]
        submission_time = None
        if "submissionTime" in stage_json:
            submission_time = stage_json["submissionTime"]
        first_task_launched_time = None
        if "firstTaskLaunchedTime" in stage_json:
            first_task_launched_time = stage_json["firstTaskLaunchedTime"]
        completion_time = None
        if "completionTime" in stage_json:
            completion_time = stage_json["completionTime"]
        executor_deserialize_time = None
        if "executorDeserializeTime" in stage_json:
            executor_deserialize_time = stage_json["executorDeserializeTime"]
        executor_deserialize_cpu_time = None
        if "executorDeserializeCpuTime" in stage_json:
            executor_deserialize_cpu_time = stage_json["executorDeserializeCpuTime"]
        executor_run_time = None
        if "executorRunTime" in stage_json:
            executor_run_time = stage_json["executorRunTime"]
        executor_cpu_time = None
        if "executorCpuTime" in stage_json:
            executor_cpu_time = stage_json["executorCpuTime"]
        result_size = None
        if "resultSize" in stage_json:
            result_size = stage_json["resultSize"]
        jvm_gc_time = None
        if "jvmGcTime" in stage_json:
            jvm_gc_time = stage_json["jvmGcTime"]
        result_serialization_time = None
        if "resultSerializationTime" in stage_json:
            result_serialization_time = stage_json["resultSerializationTime"]
        memory_bytes_spilled = None
        if "memoryBytesSpilled" in stage_json:
            memory_bytes_spilled = stage_json["memoryBytesSpilled"]
        disk_bytes_spilled = None
        if "diskBytesSpilled" in stage_json:
            disk_bytes_spilled = stage_json["diskBytesSpilled"]
        peak_execution_memory = None
        if "peakExecutionMemory" in stage_json:
            peak_execution_memory = stage_json["peakExecutionMemory"]
        input_bytes = None
        if "inputBytes" in stage_json:
            input_bytes = stage_json["inputBytes"]
        input_records = None
        if "inputRecords" in stage_json:
            input_records = stage_json["inputRecords"]
        output_bytes = None
        if "outputBytes" in stage_json:
            output_bytes = stage_json["outputBytes"]
        output_records = None
        if "outputRecords" in stage_json:
            output_records = stage_json["outputRecords"]
        shuffle_remote_blocks_fetched = None
        if "shuffleRemoteBlocksFetched" in stage_json:
            shuffle_remote_blocks_fetched = stage_json["shuffleRemoteBlocksFetched"]
        shuffle_local_blocks_fetched = None
        if "shuffleLocalBlocksFetched" in stage_json:
            shuffle_local_blocks_fetched = stage_json["shuffleLocalBlocksFetched"]
        shuffle_fetch_wait_time = None
        if "shuffleFetchWaitTime" in stage_json:
            shuffle_fetch_wait_time = stage_json["shuffleFetchWaitTime"]
        shuffle_remote_bytes_read = None
        if "shuffleRemoteBytesRead" in stage_json:
            shuffle_remote_bytes_read = stage_json["shuffleRemoteBytesRead"]
        shuffle_remote_bytes_read_to_disk = None
        if "shuffleRemoteBytesReadToDisk" in stage_json:
            shuffle_remote_bytes_read_to_disk = stage_json["shuffleRemoteBytesReadToDisk"]
        shuffle_local_bytes_read = None
        if "shuffleLocalBytesRead" in stage_json:
            shuffle_local_bytes_read = stage_json["shuffleLocalBytesRead"]
        shuffle_read_bytes = None
        if "shuffleReadBytes" in stage_json:
            shuffle_read_bytes = stage_json["shuffleReadBytes"]
        shuffle_read_records = None
        if "shuffleReadRecords" in stage_json:
            shuffle_read_records = stage_json["shuffleReadRecords"]
        shuffle_write_bytes = None
        if "shuffleWriteBytes" in stage_json:
            shuffle_write_bytes = stage_json["shuffleWriteBytes"]
        shuffle_write_time = None
        if "shuffleWriteTime" in stage_json:
            shuffle_write_time = stage_json["shuffleWriteTime"]
        shuffle_write_records = None
        if "shuffleWriteRecords" in stage_json:
            shuffle_write_records = stage_json["shuffleWriteRecords"]
        name = None
        if "name" in stage_json:
            name = stage_json["name"]
        details = None
        if "details" in stage_json:
            details = stage_json["details"]
        scheduling_pool = None
        if "schedulingPool" in stage_json:
            scheduling_pool = stage_json["schedulingPool"]
        rdd_ids = None
        if "rddIds" in stage_json:
            rdd_ids = stage_json["rddIds"]
        accumulator_updates = None
        if "accumulatorUpdates" in stage_json:
            accumulator_updates = stage_json["accumulatorUpdates"]
        killed_tasks_summary = None
        if "killedTasksSummary" in stage_json:
            killed_tasks_summary = stage_json["killedTasksSummary"]
        resource_profile_id = None
        if "resourceProfileId" in stage_json:
            resource_profile_id = stage_json["resourceProfileId"]
        peaky_executor_metrics_object = None
        if "peakExecutorMetrics" in stage_json:
            peaky_executor_metrics = stage_json["peakExecutorMetrics"]
            jvm_heap_memory = None
            if "JVMHeapMemory" in peaky_executor_metrics:
                jvm_heap_memory = peaky_executor_metrics["JVMHeapMemory"]
            jvm_off_heap_memory = None
            if "JVMOffHeapMemory" in peaky_executor_metrics:
                jvm_off_heap_memory = peaky_executor_metrics["JVMOffHeapMemory"]
            on_heap_execution_memory = None
            if "OnHeapExecutionMemory" in peaky_executor_metrics:
                on_heap_execution_memory = peaky_executor_metrics["OnHeapExecutionMemory"]
            off_heap_execution_memory = None
            if "OffHeapExecutionMemory" in peaky_executor_metrics:
                off_heap_execution_memory = peaky_executor_metrics["OffHeapExecutionMemory"]
            on_heap_storage_memory = None
            if "OnHeapStorageMemory" in peaky_executor_metrics:
                on_heap_storage_memory = peaky_executor_metrics["OnHeapStorageMemory"]
            off_heap_storage_memory = None
            if "OffHeapStorageMemory" in peaky_executor_metrics:
                off_heap_storage_memory = peaky_executor_metrics["OffHeapStorageMemory"]
            on_heap_unified_memory = None
            if "OnHeapUnifiedMemory" in peaky_executor_metrics:
                on_heap_unified_memory = peaky_executor_metrics["OnHeapUnifiedMemory"]
            off_heap_unified_memory = None
            if "OffHeapUnifiedMemory" in peaky_executor_metrics:
                off_heap_unified_memory = peaky_executor_metrics["OffHeapUnifiedMemory"]
            direct_pool_memory = None
            if "DirectPoolMemory" in peaky_executor_metrics:
                direct_pool_memory = peaky_executor_metrics["DirectPoolMemory"]
            mapped_pool_memory = None
            if "MappedPoolMemory" in peaky_executor_metrics:
                mapped_pool_memory = peaky_executor_metrics["MappedPoolMemory"]
            process_tree_jvm_memory = None
            if "ProcessTreeJVMVMemory" in peaky_executor_metrics:
                process_tree_jvm_memory = peaky_executor_metrics["ProcessTreeJVMVMemory"]
            process_tree_jvm_rss_memory = None
            if "ProcessTreeJVMRSSMemory" in peaky_executor_metrics:
                process_tree_jvm_rss_memory = peaky_executor_metrics["ProcessTreeJVMRSSMemory"]
            process_tree_python_v_memory = None
            if "ProcessTreePythonVMemory" in peaky_executor_metrics:
                process_tree_python_v_memory = peaky_executor_metrics["ProcessTreePythonVMemory"]
            process_tree_python_rss_memory = None
            if "ProcessTreePythonRSSMemory" in peaky_executor_metrics:
                process_tree_python_rss_memory = peaky_executor_metrics["ProcessTreePythonRSSMemory"]
            process_tree_other_v_memory = None
            if "ProcessTreeOtherVMemory" in peaky_executor_metrics:
                process_tree_other_v_memory = peaky_executor_metrics["ProcessTreeOtherVMemory"]
            process_tree_other_rss_memory = None
            if "ProcessTreeOtherRSSMemory" in peaky_executor_metrics:
                process_tree_other_rss_memory = peaky_executor_metrics["ProcessTreeOtherRSSMemory"]
            minor_gc_count = None
            if "MinorGCCount" in peaky_executor_metrics:
                minor_gc_count = peaky_executor_metrics["MinorGCCount"]
            minor_gc_time = None
            if "MinorGCTime" in peaky_executor_metrics:
                minor_gc_time = peaky_executor_metrics["MinorGCTime"]
            major_gc_count = None
            if "MajorGCCount" in peaky_executor_metrics:
                major_gc_count = peaky_executor_metrics["MajorGCCount"]
            major_gc_time = None
            if "MajorGCTime" in peaky_executor_metrics:
                major_gc_time = peaky_executor_metrics["MajorGCTime"]
            peaky_executor_metrics_object = PeakyExecutorMetrics(jvm_heap_memory,
                                                                 jvm_off_heap_memory,
                                                                 on_heap_execution_memory,
                                                                 off_heap_execution_memory,
                                                                 on_heap_storage_memory,
                                                                 off_heap_storage_memory,
                                                                 on_heap_unified_memory,
                                                                 off_heap_unified_memory,
                                                                 direct_pool_memory,
                                                                 mapped_pool_memory,
                                                                 process_tree_jvm_memory,
                                                                 process_tree_jvm_rss_memory,
                                                                 process_tree_python_v_memory,
                                                                 process_tree_python_rss_memory,
                                                                 process_tree_other_v_memory,
                                                                 process_tree_other_rss_memory,
                                                                 minor_gc_count,
                                                                 minor_gc_time,
                                                                 major_gc_count,
                                                                 major_gc_time)
        stage_metrics = StageMetrics(status,
                                     stage_id,
                                     attempt_id,
                                     num_tasks,
                                     num_active_tasks,
                                     num_complete_tasks,
                                     num_failed_tasks,
                                     num_killed_tasks,
                                     num_completed_indices,
                                     submission_time,
                                     first_task_launched_time,
                                     completion_time,
                                     executor_deserialize_time,
                                     executor_deserialize_cpu_time,
                                     executor_run_time,
                                     executor_cpu_time,
                                     result_size,
                                     jvm_gc_time,
                                     result_serialization_time,
                                     memory_bytes_spilled,
                                     disk_bytes_spilled,
                                     peak_execution_memory,
                                     input_bytes,
                                     input_records,
                                     output_bytes,
                                     output_records,
                                     shuffle_remote_blocks_fetched,
                                     shuffle_local_blocks_fetched,
                                     shuffle_fetch_wait_time,
                                     shuffle_remote_bytes_read,
                                     shuffle_remote_bytes_read_to_disk,
                                     shuffle_local_bytes_read,
                                     shuffle_read_bytes,
                                     shuffle_read_records,
                                     shuffle_write_bytes,
                                     shuffle_write_time,
                                     shuffle_write_records,
                                     name,
                                     details,
                                     scheduling_pool,
                                     rdd_ids,
                                     accumulator_updates,
                                     killed_tasks_summary,
                                     resource_profile_id,
                                     peaky_executor_metrics_object)
        stages_metrics_list.append(stage_metrics)
    return stages_metrics_list


def parse_executors_json_list(json_file_path: Path) -> list:
    executors_json_list = get_json_object_list(json_file_path)
    executors_metrics_list = []
    for executor_json in executors_json_list:
        executor_id = None
        if "id" in executor_json:
            executor_id = executor_json["id"]
        host_port = None
        if "hostPort" in executor_json:
            host_port = executor_json["hostPort"]
        is_active = None
        if "isActive" in executor_json:
            is_active = executor_json["isActive"]
        rdd_blocks = None
        if "rddBlocks" in executor_json:
            rdd_blocks = executor_json["rddBlocks"]
        memory_used = None
        if "memoryUsed" in executor_json:
            memory_used = executor_json["memoryUsed"]
        disk_used = None
        if "diskUsed" in executor_json:
            disk_used = executor_json["diskUsed"]
        total_cores = None
        if "totalCores" in executor_json:
            total_cores = executor_json["totalCores"]
        max_tasks = None
        if "maxTasks" in executor_json:
            max_tasks = executor_json["maxTasks"]
        active_tasks = None
        if "activeTasks" in executor_json:
            active_tasks = executor_json["activeTasks"]
        failed_tasks = None
        if "failedTasks" in executor_json:
            failed_tasks = executor_json["failedTasks"]
        completed_tasks = None
        if "completedTasks" in executor_json:
            completed_tasks = executor_json["completedTasks"]
        total_tasks = None
        if "totalTasks" in executor_json:
            total_tasks = executor_json["totalTasks"]
        total_duration = None
        if "totalDuration" in executor_json:
            total_duration = executor_json["totalDuration"]
        total_gc_time = None
        if "totalGCTime" in executor_json:
            total_gc_time = executor_json["totalGCTime"]
        total_input_bytes = None
        if "totalInputBytes" in executor_json:
            total_input_bytes = executor_json["totalInputBytes"]
        total_shuffle_read = None
        if "totalShuffleRead" in executor_json:
            total_shuffle_read = executor_json["totalShuffleRead"]
        total_shuffle_write = None
        if "totalShuffleWrite" in executor_json:
            total_shuffle_write = executor_json["totalShuffleWrite"]
        is_blacklisted = None
        if "isBlacklisted" in executor_json:
            is_blacklisted = executor_json["isBlacklisted"]
        max_memory = None
        if "maxMemory" in executor_json:
            max_memory = executor_json["maxMemory"]
        add_time = None
        if "addTime" in executor_json:
            add_time = executor_json["addTime"]
        executor_logs_object = None
        if "executorLogs" in executor_json:
            executor_logs = executor_json["executorLogs"]
            stdout = None
            if "stdout" in executor_logs:
                stdout = executor_logs["stdout"]
            stderr = None
            if "stderr" in executor_logs:
                stderr = executor_logs["stderr"]
            executor_logs_object = ExecutorLogs(stdout, stderr)
        memory_metrics_object = None
        if "memoryMetrics" in executor_json:
            memory_metrics = executor_json["memoryMetrics"]
            used_on_heap_storage_memory = None
            if "usedOnHeapStorageMemory" in memory_metrics:
                used_on_heap_storage_memory = memory_metrics["usedOnHeapStorageMemory"]
            used_off_heap_storage_memory = None
            if "usedOffHeapStorageMemory" in memory_metrics:
                used_off_heap_storage_memory = memory_metrics["usedOffHeapStorageMemory"]
            total_on_heap_storage_memory = None
            if "totalOnHeapStorageMemory" in memory_metrics:
                total_on_heap_storage_memory = memory_metrics["totalOnHeapStorageMemory"]
            total_off_heap_storage_memory = None
            if "totalOffHeapStorageMemory" in memory_metrics:
                total_off_heap_storage_memory = memory_metrics["totalOffHeapStorageMemory"]
            memory_metrics_object = MemoryMetrics(used_on_heap_storage_memory,
                                                  used_off_heap_storage_memory,
                                                  total_on_heap_storage_memory,
                                                  total_off_heap_storage_memory)
        blacklisted_in_stages = None
        if "blacklistedInStages" in executor_json:
            blacklisted_in_stages = executor_json["blacklistedInStages"]
        peak_memory_metrics_object = None
        if "peakMemoryMetrics" in executor_json:
            peak_memory_metrics = executor_json["peakMemoryMetrics"]
            jvm_heap_memory = None
            if "JVMHeapMemory" in peak_memory_metrics:
                jvm_heap_memory = peak_memory_metrics["JVMHeapMemory"]
            jvm_off_heap_memory = None
            if "JVMOffHeapMemory" in peak_memory_metrics:
                jvm_off_heap_memory = peak_memory_metrics["JVMOffHeapMemory"]
            on_heap_execution_memory = None
            if "OnHeapExecutionMemory" in peak_memory_metrics:
                on_heap_execution_memory = peak_memory_metrics["OnHeapExecutionMemory"]
            off_heap_execution_memory = None
            if "OffHeapExecutionMemory" in peak_memory_metrics:
                off_heap_execution_memory = peak_memory_metrics["OffHeapExecutionMemory"]
            on_heap_storage_memory = None
            if "OnHeapStorageMemory" in peak_memory_metrics:
                on_heap_storage_memory = peak_memory_metrics["OnHeapStorageMemory"]
            off_heap_storage_memory = None
            if "OffHeapStorageMemory" in peak_memory_metrics:
                off_heap_storage_memory = peak_memory_metrics["OffHeapStorageMemory"]
            on_heap_unified_memory = None
            if "OnHeapUnifiedMemory" in peak_memory_metrics:
                on_heap_unified_memory = peak_memory_metrics["OnHeapUnifiedMemory"]
            off_heap_unified_memory = None
            if "OffHeapUnifiedMemory" in peak_memory_metrics:
                off_heap_unified_memory = peak_memory_metrics["OffHeapUnifiedMemory"]
            direct_pool_memory = None
            if "DirectPoolMemory" in peak_memory_metrics:
                direct_pool_memory = peak_memory_metrics["DirectPoolMemory"]
            mapped_pool_memory = None
            if "MappedPoolMemory" in peak_memory_metrics:
                mapped_pool_memory = peak_memory_metrics["MappedPoolMemory"]
            process_tree_jvm_memory = None
            if "ProcessTreeJVMVMemory" in peak_memory_metrics:
                process_tree_jvm_memory = peak_memory_metrics["ProcessTreeJVMVMemory"]
            process_tree_jvm_rss_memory = None
            if "ProcessTreeJVMRSSMemory" in peak_memory_metrics:
                process_tree_jvm_rss_memory = peak_memory_metrics["ProcessTreeJVMRSSMemory"]
            process_tree_python_v_memory = None
            if "ProcessTreePythonVMemory" in peak_memory_metrics:
                process_tree_python_v_memory = peak_memory_metrics["ProcessTreePythonVMemory"]
            process_tree_python_rss_memory = None
            if "ProcessTreePythonRSSMemory" in peak_memory_metrics:
                process_tree_python_rss_memory = peak_memory_metrics["ProcessTreePythonRSSMemory"]
            process_tree_other_v_memory = None
            if "ProcessTreeOtherVMemory" in peak_memory_metrics:
                process_tree_other_v_memory = peak_memory_metrics["ProcessTreeOtherVMemory"]
            process_tree_other_rss_memory = None
            if "ProcessTreeOtherRSSMemory" in peak_memory_metrics:
                process_tree_other_rss_memory = peak_memory_metrics["ProcessTreeOtherRSSMemory"]
            minor_gc_count = None
            if "MinorGCCount" in peak_memory_metrics:
                minor_gc_count = peak_memory_metrics["MinorGCCount"]
            minor_gc_time = None
            if "MinorGCTime" in peak_memory_metrics:
                minor_gc_time = peak_memory_metrics["MinorGCTime"]
            major_gc_count = None
            if "MajorGCCount" in peak_memory_metrics:
                major_gc_count = peak_memory_metrics["MajorGCCount"]
            major_gc_time = None
            if "MajorGCTime" in peak_memory_metrics:
                major_gc_time = peak_memory_metrics["MajorGCTime"]
            peak_memory_metrics_object = PeakyMemoryMetrics(jvm_heap_memory,
                                                            jvm_off_heap_memory,
                                                            on_heap_execution_memory,
                                                            off_heap_execution_memory,
                                                            on_heap_storage_memory,
                                                            off_heap_storage_memory,
                                                            on_heap_unified_memory,
                                                            off_heap_unified_memory,
                                                            direct_pool_memory,
                                                            mapped_pool_memory,
                                                            process_tree_jvm_memory,
                                                            process_tree_jvm_rss_memory,
                                                            process_tree_python_v_memory,
                                                            process_tree_python_rss_memory,
                                                            process_tree_other_v_memory,
                                                            process_tree_other_rss_memory,
                                                            minor_gc_count,
                                                            minor_gc_time,
                                                            major_gc_count,
                                                            major_gc_time)
        attributes = None
        if "attributes" in executor_json:
            attributes = executor_json["attributes"]
        resources = None
        if "resources" in executor_json:
            resources = executor_json["resources"]
        resource_profile_id = None
        if "resourceProfileId" in executor_json:
            resource_profile_id = executor_json["resourceProfileId"]
        is_excluded = None
        if "isExcluded" in executor_json:
            is_excluded = executor_json["isExcluded"]
        excluded_in_stages = None
        if "excludedInStages" in executor_json:
            excluded_in_stages = executor_json["excludedInStages"]
        executor_metrics = ExecutorMetrics(executor_id,
                                           host_port,
                                           is_active,
                                           rdd_blocks,
                                           memory_used,
                                           disk_used,
                                           total_cores,
                                           max_tasks,
                                           active_tasks,
                                           failed_tasks,
                                           completed_tasks,
                                           total_tasks,
                                           total_duration,
                                           total_gc_time,
                                           total_input_bytes,
                                           total_shuffle_read,
                                           total_shuffle_write,
                                           is_blacklisted,
                                           max_memory,
                                           add_time,
                                           executor_logs_object,
                                           memory_metrics_object,
                                           blacklisted_in_stages,
                                           peak_memory_metrics_object,
                                           attributes,
                                           resources,
                                           resource_profile_id,
                                           is_excluded,
                                           excluded_in_stages)
        executors_metrics_list.append(executor_metrics)
    return executors_metrics_list


def get_jobs_metrics_counts_list(jobs_metrics_list: list) -> list:
    total_jobs_count = 0
    succeeded_jobs_count = 0
    running_jobs_count = 0
    failed_jobs_count = 0
    for job_metric in jobs_metrics_list:
        total_jobs_count = total_jobs_count + 1
        if job_metric.status == "SUCCEEDED":
            succeeded_jobs_count = succeeded_jobs_count + 1
        if job_metric.status == "RUNNING":
            running_jobs_count = running_jobs_count + 1
        if job_metric.status == "FAILED":
            failed_jobs_count = failed_jobs_count + 1
    jobs_metrics_counts_list = [["total_jobs", total_jobs_count],
                                ["succeeded_jobs", succeeded_jobs_count],
                                ["running_jobs", running_jobs_count],
                                ["failed_jobs", failed_jobs_count]]
    return jobs_metrics_counts_list


def get_tasks_metrics_counts_list(jobs_metrics_list: list) -> list:
    completed_tasks_count = 0
    skipped_tasks_count = 0
    active_tasks_count = 0
    failed_tasks_count = 0
    killed_tasks_count = 0
    for job_metric in jobs_metrics_list:
        completed_tasks_count = completed_tasks_count + job_metric.num_completed_tasks
        skipped_tasks_count = skipped_tasks_count + job_metric.num_skipped_tasks
        active_tasks_count = active_tasks_count + job_metric.num_active_tasks
        failed_tasks_count = failed_tasks_count + job_metric.num_failed_tasks
        killed_tasks_count = killed_tasks_count + job_metric.num_killed_tasks
    total_tasks_count = completed_tasks_count \
        + skipped_tasks_count \
        + active_tasks_count \
        + failed_tasks_count \
        + killed_tasks_count
    tasks_metrics_counts_list = [["total_tasks", total_tasks_count],
                                 ["completed_tasks", completed_tasks_count],
                                 ["skipped_tasks", skipped_tasks_count],
                                 ["active_tasks", active_tasks_count],
                                 ["failed_tasks", failed_tasks_count],
                                 ["killed_tasks", killed_tasks_count]]
    return tasks_metrics_counts_list


def get_stages_metrics_counts_list(stages_metrics_list: list) -> list:
    total_stages_count = 0
    complete_stages_count = 0
    skipped_stages_count = 0
    active_stages_count = 0
    pending_stages_count = 0
    failed_stages_count = 0
    for stage_metric in stages_metrics_list:
        total_stages_count = total_stages_count + 1
        if stage_metric.status == "COMPLETE":
            complete_stages_count = complete_stages_count + 1
        if stage_metric.status == "SKIPPED":
            skipped_stages_count = skipped_stages_count + 1
        if stage_metric.status == "ACTIVE":
            active_stages_count = active_stages_count + 1
        if stage_metric.status == "PENDING":
            pending_stages_count = pending_stages_count + 1
        if stage_metric.status == "FAILED":
            failed_stages_count = failed_stages_count + 1
    stages_metrics_counts_list = [["total_stages", total_stages_count],
                                  ["complete_stages", complete_stages_count],
                                  ["skipped_stages", skipped_stages_count],
                                  ["active_stages", active_stages_count],
                                  ["pending_stages", pending_stages_count],
                                  ["failed_stages", failed_stages_count]]
    return stages_metrics_counts_list


def collect_spark_job_metrics(sjm: SparkJobMetrics) -> None:
    # COLLECT JOBS METRICS
    wget.download(sjm.jobs_metrics_url, str(sjm.jobs_metrics_destination_file_path))

    # COLLECT STAGES METRICS
    wget.download(sjm.stages_metrics_url, str(sjm.stages_metrics_destination_file_path))

    # COLLECT EXECUTORS METRICS
    wget.download(sjm.executors_metrics_url, str(sjm.executors_metrics_destination_file_path))

    # COLLECT STORAGE METRICS
    wget.download(sjm.storage_metrics_url, str(sjm.storage_metrics_destination_file_path))


def get_spark_job_metrics_counts_list(spark_driver_host: str,
                                      spark_app_name: str,
                                      spark_app_id: str,
                                      spark_ui_port: str,
                                      metrics_directory_path: Path) -> list:
    # LOAD SPARK JOBS METRICS PARAMETERS
    sjm = SparkJobMetrics()
    load_spark_job_metrics_parameters(sjm, spark_driver_host, spark_app_name, spark_app_id, spark_ui_port, metrics_directory_path)

    # COLLECT SPARK JOB METRICS
    collect_spark_job_metrics(sjm)

    # PARSE JOBS JSON LIST
    jobs_metrics_list = parse_jobs_json_list(sjm.jobs_metrics_destination_file_path)

    # GET JOBS METRICS COUNTS LIST
    jobs_metrics_counts_list = get_jobs_metrics_counts_list(jobs_metrics_list)

    # GET TASKS METRICS COUNTS LIST
    tasks_metrics_counts_list = get_tasks_metrics_counts_list(jobs_metrics_list)

    # PARSE STAGES JSON LIST
    stages_metrics_list = parse_stages_json_list(sjm.stages_metrics_destination_file_path)

    # GET STAGES METRICS COUNTS LIST
    stages_metrics_counts_list = get_stages_metrics_counts_list(stages_metrics_list)

    # PARSE EXECUTORS JSON LIST
    # executors_metrics_list = parse_executors_json_list(sjm.executors_metrics_destination_file_path)

    spark_job_metrics_list = [jobs_metrics_counts_list, tasks_metrics_counts_list, stages_metrics_counts_list]
    return spark_job_metrics_list
