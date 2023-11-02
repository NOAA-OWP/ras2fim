#!/usr/bin/env python3

import datetime as dt
import os
import traceback
from pathlib import Path

import colored as cl


# Globally scoped
CUSTOM_LOG_FILES_PATHS = {}
LOG_SYSTEM_IS_SETUP = False

# **********************
# NOTE: there is another global variable at the end of this script (and has to be at the end)


class RAS2FIM_logger:
    LOG_FILE_PATH = ""  # full path and file name
    LOG_DEFAULT_FOLDER = ""

    __log_warning_file_path = ""
    __log_error_file_path = ""

    # levels available for use
    # trace - does not show in console but goes to the default log file
    # lprint - goes to console and default log file
    # success - goes to console and default log file
    # warning - goes to console, log file and warning log file
    # error - goes to console, log file and error log file. Normally used when the error
    #     does not kill the application
    # critical - goes to console, log file and error log file. Normally used when the
    #     program is aborted

    # -------------------------------------------------
    def __get_dt(self):
        # {time:YYYY-MM-DD > HH:mm:ss!UTC}
        cur_dt = dt.datetime.utcnow()
        ret_dt = f"{cur_dt.strftime('%Y-%m-%d')} {cur_dt.strftime('%H:%M:%S')}"
        return ret_dt

    # -------------------------------------------------
    def __get_clog_dt(self):
        return f"{cl.fore.SPRING_GREEN_2B}{self.__get_dt()}{cl.style.RESET}"

    # -------------------------------------------------
    def setup(self, log_file_path: str):
        # Allows us to write the app wide global variables
        global LOG_SYSTEM_IS_SETUP

        # -----------
        # Validation
        if log_file_path is None:
            raise ValueError("Error: log_file_path not defined")

        log_file_path = log_file_path.strip()

        if log_file_path == "":
            raise ValueError("Error: log_file_path can not be empty")

        # The file need not exist, but folder must
        folder_path = os.path.dirname(log_file_path)
        log_file_name = os.path.basename(log_file_path)

        if os.path.exists(folder_path) is False:
            raise ValueError(
                "Error: The folder for log_file_path does not exist.\n"
                "Note: The file need not exist, but the folder must"
            )
        self.LOG_DEFAULT_FOLDER = folder_path

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        self.__calc_warning_error_file_names(log_file_path)

        self.LOG_FILE_PATH = log_file_path

        LOG_SYSTEM_IS_SETUP = True

    # -------------------------------------------------
    def __calc_warning_error_file_names(self, log_file_and_path):
        """
        Process:
            Parses the log_file_and_path to add either the name of _warnings or _errors
            into the file name.
            Why not update __log_warning_file_path and
        Input:
            log_file_and_path: ie) C:\ras2fim_data\output_ras2fim\12090301_2277_231101\logs\ras2fim.log
        Output:
            Updates __log_error_file_path and __log_warning_file_path variables
        """

        folder_path = os.path.dirname(log_file_and_path)
        log_file_name = os.path.basename(log_file_and_path)

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        # now calc the warning log file
        self.__log_warning_file_path = os.path.join(
            folder_path, file_name_parts[0] + "_warnings" + file_name_parts[1]
        )

        # now calc the error log file
        self.__log_error_file_path = os.path.join(
            folder_path, file_name_parts[0] + "_errors" + file_name_parts[1]
        )

    # -------------------------------------------------
    def merge_log_files(self, log_file_and_path, file_prefix):
        """
        Overview:
            This tool is mostly for merging log files during multi processing which each had their own file.

            This will search all of the files in directory in the same folder as the
            incoming log_file_and_path. It then looks for all files starting with the
            file_prefix and adds them to the log file (via log_file_and_path)
        Inputs:
            - log_file_and_path: ie) C:\ras2fim_data\output_ras2fim\12090301_2277_231101\logs\ras2fim.log
            - file_prefix: This value must be the start of file names. ie) mp_create_gdf_of_points
                as in C:\...\12090301_2277_231101\logs\mp_create_gdf_of_points_1235.log
        """

        # -----------
        # Validation
        if log_file_and_path is None:
            raise ValueError("Error: log_file_and_path not defined")

        log_file_and_path = log_file_and_path.strip()

        if log_file_and_path == "":
            raise ValueError("Error: log_file_and_path can not be empty")

        # The file need not exist, but folder must
        folder_path = os.path.dirname(log_file_and_path)

        if os.path.isfile(log_file_and_path) is False:
            raise ValueError("Error: log file and path does not exist.")

        log_file_list = list(Path(folder_path).rglob(f"{file_prefix}*"))
        if len(log_file_list) > 0:
            log_file_list.sort()

            self.lprint(".. merging log files")

            # open and write to the parent log
            # This will write all logs including errors and warning
            with open(log_file_and_path, 'a') as main_log:
                # Iterate through list
                for temp_log_file in log_file_list:
                    # Open each file in read mode
                    with open(temp_log_file) as infile:
                        main_log.write(infile.read())

            # now the warning files if there are any
            log_warning_file_list = list(Path(folder_path).rglob(f"{file_prefix}_warnings*"))
            if len(log_warning_file_list) > 0:
                log_warning_file_list.sort()
                with open(self.__log_warning_file_path, 'a') as warning_log:
                    # Iterate through list
                    for temp_log_file in log_warning_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            warning_log.write(infile.read())

            # now the warning files if there are any
            log_error_file_list = list(Path(folder_path).rglob(f"{file_prefix}_errors*"))
            if len(log_error_file_list) > 0:
                log_error_file_list.sort()
                with open(self.__log_error_file_path, 'a') as error_log:
                    # Iterate through list
                    for temp_log_file in log_error_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            error_log.write(infile.read())

        # now delete the all file with same prefix (reg, error and warning)
        # iterate through them a second time (do it doesn't mess up the for loop above)
        for temp_log_file in log_file_list:
            try:
                os.remove(temp_log_file)
            except OSError:
                self.error(f"Error deleting {temp_log_file}")
                self.error(traceback.format_exc())

    # -------------------------------------------------
    def trace(self, msg):
        # goes to file only, not console
        level = "TRACE   "  # keeps spacing the same
        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def lprint(self, msg):
        # goes to console and log file
        level = "LPRINT  "  # keeps spacing the same
        print(f"{msg} ")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def debug(self, msg):
        # goes to console and log file
        level = "DEBUG   "  # keeps spacing the same

        c_msg_type = f"{cl.fore.DODGER_BLUE_1}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def success(self, msg):
        # goes to console and log file
        level = "SUCCESS "  # keeps spacing the same

        c_msg_type = f"{cl.fore.SPRING_GREEN_2B}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def warning(self, msg):
        # goes to console and log file and warning log file
        level = "WARNING "  # keeps spacing the same

        c_msg_type = f"{cl.fore.LIGHT_YELLOW}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to warning logs
        with open(self.__log_warning_file_path, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def error(self, msg):
        # goes to console and log file and error log file
        level = "ERROR   "  # keeps spacing the same

        c_msg_type = f"{cl.fore.RED_1}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.__log_error_file_path, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def critical(self, msg):
        level = "CRITICAL"  # keeps spacing the same

        c_msg_type = f"{cl.style.BOLD}{cl.fore.RED_3A}{cl.back.WHITE}{self.__get_dt()}"
        c_msg_type += f" <{level}>"
        print(f" {c_msg_type} : {msg} {cl.style.RESET}")

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.__log_error_file_path, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")


# global RLOG
R2F_LOG = RAS2FIM_logger()
