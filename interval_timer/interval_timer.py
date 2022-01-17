from logging import Logger
from threading import Thread
from time import sleep, time


class IntervalTimer:

    def __init__(self,
                 spark_app_name: str,
                 logger: Logger) -> None:
        self.interval_in_minutes = 15
        self.daemon_mode = True
        self.spark_app_name = spark_app_name
        self.logger = logger

    def __get_interval_in_minutes(self) -> int:
        return self.interval_in_minutes

    def __get_spark_app_name(self) -> str:
        return self.spark_app_name

    def __get_logger(self) -> Logger:
        return self.logger

    def __get_daemon_mode(self) -> bool:
        return self.daemon_mode

    @staticmethod
    def __get_ordinal_number_suffix(number: int) -> str:
        number_to_str = str(number)
        if number_to_str.endswith("1"):
            return "st"
        if number_to_str.endswith("2"):
            return "nd"
        if number_to_str.endswith("3"):
            return "rd"
        else:
            return "th"

    def __interval_timer_function(self,
                                  interval_in_minutes: int,
                                  spark_app_name: str,
                                  logger: Logger) -> None:
        interval_count = 0
        while True:
            start = time()
            while True:
                end = (time() - start) / 60
                if end >= interval_in_minutes:
                    interval_count = interval_count + 1
                    break
                sleep(1)
            ordinal_number_suffix = self.__get_ordinal_number_suffix(interval_count)
            interval_timer_message = "({0}) Interval Timer Thread: Interval of {1} minute(s) ({2}{3} time)" \
                .format(spark_app_name,
                        str(interval_in_minutes),
                        str(interval_count),
                        ordinal_number_suffix)
            print(interval_timer_message)
            logger.info(interval_timer_message)

    def start(self) -> None:
        # Get Interval in Minutes
        interval_in_minutes = self.__get_interval_in_minutes()
        # Get Spark App Name
        spark_app_name = self.__get_spark_app_name()
        # Get Logger
        logger = self.__get_logger()
        # Get Daemon Mode
        daemon_mode = self.__get_daemon_mode()
        # Create interval_timer_thread
        interval_timer_thread = Thread(target=self.__interval_timer_function,
                                       args=(interval_in_minutes,
                                             spark_app_name,
                                             logger),
                                       daemon=daemon_mode)
        # Start interval_timer_thread
        interval_timer_thread.start()
