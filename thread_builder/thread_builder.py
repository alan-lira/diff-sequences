from threading import Thread
from typing import Any, Tuple


class ThreadBuilder:

    def __init__(self,
                 target_method: Any,
                 name: str,
                 target_method_arguments: Tuple,
                 daemon_mode: bool) -> None:
        self.target_method = target_method
        self.name = name
        self.target_method_arguments = target_method_arguments
        self.daemon_mode = daemon_mode

    def __get_target_method(self) -> Any:
        return self.target_method

    def __get_name(self) -> str:
        return self.name

    def __get_target_method_arguments(self) -> Tuple:
        return self.target_method_arguments

    def __get_daemon_mode(self) -> bool:
        return self.daemon_mode

    def start(self) -> None:
        # Get Target Method
        target_method = self.__get_target_method()
        # Get Name
        name = self.__get_name()
        # Get Target Method Arguments
        target_method_arguments = self.__get_target_method_arguments()
        # Get Daemon Mode
        daemon_mode = self.__get_daemon_mode()
        # Create Thread
        thread = Thread(target=target_method,
                        name=name,
                        args=target_method_arguments,
                        daemon=daemon_mode)
        # Start Thread
        thread.start()
