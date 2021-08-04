from configparser import ConfigParser
from differentiator_exceptions import InvalidDictionaryError, InvalidNumberOfArgumentsError
from pathlib import Path
import ast
import re
import subprocess
import sys


class DiffSequencesJobExecutor:

    def __init__(self) -> None:
        self.spark_cluster_name = None
        self.spark_submit_bin_path = None
        self.spark_submit_conf = None
        self.spark_driver_host = None
        self.spark_driver_port = None
        self.spark_application_entry_point = None
        self.spark_application_arguments = None


def check_if_is_valid_number_of_arguments(number_of_arguments_provided: int) -> None:
    if number_of_arguments_provided != 2:
        invalid_number_of_arguments_message = \
            "Invalid Number of Arguments Provided! \n" \
            "Expected 1 Argument: {0} File. \n" \
            "Provided: {1} Argument(s)." \
            .format("differentiator_job_executor.dict", number_of_arguments_provided - 1)
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
    config_parser.optionxform = str
    config_parser.read_dict(parameters_dictionary)
    return config_parser


def load_diff_sequences_job_executor_parameters(dsje: DiffSequencesJobExecutor,
                                                parsed_parameters_dictionary: dict) -> None:
    # READ SPARK CLUSTER NAME
    dsje.spark_cluster_name = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_cluster_name"])

    # READ SPARK SUBMIT BIN PATH
    dsje.spark_submit_bin_path = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_submit_bin_path"])

    # READ ALL SPARK SUBMIT CONF
    spark_submit_conf = ""
    for key, value in parsed_parameters_dictionary["SparkSubmitConf"].items():
        spark_submit_conf = spark_submit_conf + " --conf {0}={1}".format(key, value)
    dsje.spark_submit_conf = spark_submit_conf[1:]

    # READ SPARK DRIVER HOST
    dsje.spark_driver_host = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_driver_host"])

    # READ SPARK DRIVER PORT
    dsje.spark_driver_port = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_driver_port"])

    # READ SPARK APPLICATION ENTRY POINT
    dsje.spark_application_entry_point = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_application_entry_point"])

    # READ SPARK APPLICATION ARGUMENTS
    dsje.spark_application_arguments = \
        str(parsed_parameters_dictionary["DiffSequencesJobExecutor"]["spark_application_arguments"])


def print_spark_application_output(process) -> None:
    for line in iter(lambda: process.stdout.readline(), ""):
        print(line.rstrip())
    process.stdout.close()


def write_spark_application_output(process,
                                   spark_application_output_file_path: Path) -> None:
    with open(spark_application_output_file_path, mode="w") as spark_application_output_file:
        for line in iter(lambda: process.stdout.readline(), ""):
            spark_application_output_file.write(line)


def get_spark_submitted_job_info_list(process) -> list:
    app_id = None
    app_name = None
    app_binded_spark_ui_port = None
    for line in iter(lambda: process.stderr.readline(), ""):
        match_app_id = re.search("INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID (.*)$", line)
        match_app_name = re.search("INFO SparkContext: Submitted application: (.*)$", line)
        match_binded_port = re.search("INFO Utils: Successfully started service 'SparkUI' on port (.*).$", line)
        if match_app_id:
            app_id = match_app_id.groups()[0]
        if match_app_name:
            app_name = match_app_name.groups()[0]
        if match_binded_port:
            app_binded_spark_ui_port = match_binded_port.groups()[0]
        if app_id and app_name and app_binded_spark_ui_port:
            process.stderr.close()
            break
    spark_submitted_job_info_list = [app_id, app_name, app_binded_spark_ui_port]
    return spark_submitted_job_info_list


def executor(argv: list) -> None:
    # BEGIN

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

    # LOAD DIFF SEQUENCES JOB EXECUTOR PARAMETERS
    dsje = DiffSequencesJobExecutor()
    load_diff_sequences_job_executor_parameters(dsje, parsed_parameters_dictionary)

    # SET SPARK-SUBMIT OPTIONS
    spark_submit_options = "{0} {1} --master spark://{2}:{3} {4} {5}" \
        .format(dsje.spark_submit_bin_path,
                dsje.spark_submit_conf,
                dsje.spark_driver_host,
                dsje.spark_driver_port,
                dsje.spark_application_entry_point,
                dsje.spark_application_arguments)

    # PRINT SPARK-SUBMIT OPTIONS MESSAGE
    spark_submit_options_message = "[{0}] Spark-Submit Options: {1}" \
        .format(dsje.spark_cluster_name, spark_submit_options)
    print(spark_submit_options_message)

    # START SPARK-SUBMIT PROCESS
    spark_submit_process = subprocess.Popen(spark_submit_options,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            universal_newlines=True,
                                            shell=True)

    # GET SPARK SUBMITTED JOB INFO LIST
    spark_submitted_job_info_list = get_spark_submitted_job_info_list(spark_submit_process)
    app_id = spark_submitted_job_info_list[0]
    app_name = spark_submitted_job_info_list[1]
    app_binded_spark_ui_port = spark_submitted_job_info_list[2]

    # READ SPARK APPLICATION OUTPUT FILE PATH
    # spark_application_output_file_path = Path(app_name + "_output.txt")

    # WRITE SPARK APPLICATION OUTPUT
    # write_spark_application_output(spark_submit_process, spark_application_output_file_path)

    # PRINT SPARK SUBMITTED JOB INFO MESSAGE
    spark_submitted_job_info_message = "[{0}] Spark Submitted Job Info:\n" \
                                       "[{0}] Application ID: {1}\n" \
                                       "[{0}] Application Name: {2}\n" \
                                       "[{0}] Application Binded at Spark UI's Port: {3}" \
        .format(dsje.spark_cluster_name, app_id, app_name, app_binded_spark_ui_port)
    print(spark_submitted_job_info_message)

    # PRINT SPARK APPLICATION OUTPUT
    print_spark_application_output(spark_submit_process)

    # GET SPARK-SUBMIT PROCESS RETURN CODE
    spark_submit_process_return_code = spark_submit_process.wait()

    # PRINT FINISHED SPARK JOB MESSAGE
    finished_spark_job_message = "[{0}] Spark application {1} ({2}) has finished! Return code: {3}." \
        .format(dsje.spark_cluster_name, app_name, app_id, spark_submit_process_return_code)
    print(finished_spark_job_message)

    # END
    sys.exit(0)


if __name__ == "__main__":
    executor(sys.argv)
