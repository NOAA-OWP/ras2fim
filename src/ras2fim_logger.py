#!/usr/bin/env python3

import datetime as dt
import os
import traceback
from pathlib import Path

import colored as cl


# Careful... You might not put shared_functions here as it can create circular references,
#  so shared_functions is put inside functions only/as is need


class RAS2FIM_logger:
    CUSTOM_LOG_FILES_PATHS = {}
    LOG_SYSTEM_IS_SETUP = False

    LOG_DEFAULT_FOLDER = ""
    LOG_FILE_PATH = ""  # full path and file name
    LOG_WARNING_FILE_PATH = ""
    LOG_ERROR_FILE_PATH = ""

    LOG_SYS_NOT_SETUP_MSG = "******  Logging to the file system not yet setup.\n"
    "******  Sometimes this is not setup until after initial validation.\n"

    """
    Levels available for use
      trace - does not show in console but goes to the default log file
      lprint - goes to console and default log file
      success - goes to console and default log file
      warning - goes to console, log file and warning log file
      error - goes to console, log file and error log file. Normally used when the error
          does not kill the application
      critical - goes to console, log file and error log file. Normally used when the
          program is aborted

    NOTE: If you hardcode part of the file_path_and_name, make sure you add the "r"
    flag in front of the file path or use double backslashs to solve problems
    of double slashs in pathing. Using os.path.joins can fix that too.
    ie) r"C:\ras2fim_data\output_ras2fim\12030105_2276_231017\final\model_list.csv"
    or
    "C:\\ras2fim_data\\output_ras2fim\\12030105_2276_231017\\final\\model_list.csv"
    Special backslashs combo's like \r, \t, \n can get create problems.

    """

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
        """
        Note: for ras2fim.py or most files in the /src directory it is recommended that
        you use the unit output folder with the folder name of 'logs' before the file name:
            ie) C:\ras2fim_data\output_ras2fim\12030105_2276_231017\logs\ras2fim.log

        During this process, a second log file will be created as an error file which will
        duplicate all log message with the levels of ERROR, and CRITICAL.
        """

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
            os.makedirs(folder_path, exist_ok=True)

        self.LOG_DEFAULT_FOLDER = folder_path

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        self.__calc_warning_error_file_names(log_file_path)
        self.LOG_FILE_PATH = log_file_path

        # We need to remove the older ones if they already exist. Why? one attempt of running an script
        # might trigger and log file and an error file. So, it is correct, run again and now we have an
        # old invalid error file
        if os.path.isfile(log_file_path):
            os.remove(log_file_path)

        if os.path.isfile(self.LOG_ERROR_FILE_PATH):
            os.remove(self.LOG_ERROR_FILE_PATH)

        if os.path.isfile(self.LOG_WARNING_FILE_PATH):
            os.remove(self.LOG_WARNING_FILE_PATH)

        self.LOG_SYSTEM_IS_SETUP = True

    # -------------------------------------------------
    def MP_Log_setup(self, file_prefix, log_folder_path):
        """
        Overview:
            This method is sort of a wrapper in that it just manually creates a file name
            using a defined file path.
            The file name is calculated as such {file_prefix}-{date_with_milli and random key}.log()
            ie) produce_geocurves-231122_1407444333_1234.log

            Note: This works when ras2fim.py is running only and not for direct executions of any .py file.

        Inputs:
            file_prefix (str): a value to prepend to the file names. Often is the name of the function
               that called this method. Note: Later, when a person has these MP_Logs cleaned up
               they will use this file_prefix again to search and remove the temp MP_log files as they
               get rolled back up to the master RLOG file.
            log_folder_path (str): folder location for the files to be created. Note: it has to be in
               the same folder as the master RLOG file.
        """

        # Has to be put here instead of global to this script to avoid circular references
        import shared_functions as sf

        # -----------------
        # validation
        if not os.path.exists(log_folder_path):
            raise Exception(
                f"log folder path value of {log_folder_path} does not exist."
                " It should be where the ras2fim log is."
            )

        # -----------------
        file_id = sf.get_date_with_milli()
        log_file_name = f"{file_prefix}-{file_id}.log"
        log_file_path = os.path.join(log_folder_path, log_file_name)

        self.setup(log_file_path)

    # -------------------------------------------------
    def __calc_warning_error_file_names(self, log_file_and_path):
        """
        Process:
            Parses the log_file_and_path to add either the name of _warnings or _errors
            into the file name.
            Why not update LOG_WARNING_FILE_PATH and LOG_ERROR_FILE_PATH
        Input:
            log_file_and_path: ie) C:\ras2fim_data\output_ras2fim\12090301_2277_231101\logs\ras2fim.log
        Output:
            Updates LOG_WARNING_FILE_PATH and LOG_ERROR_FILE_PATH variables
        """

        folder_path = os.path.dirname(log_file_and_path)
        log_file_name = os.path.basename(log_file_and_path)

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        # now calc the warning log file
        self.LOG_WARNING_FILE_PATH = os.path.join(
            folder_path, file_name_parts[0] + "_warnings" + file_name_parts[1]
        )

        # now calc the error log file
        self.LOG_ERROR_FILE_PATH = os.path.join(
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
            log_warning_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_warnings*"))
            if len(log_warning_file_list) > 0:
                log_warning_file_list.sort()
                with open(self.LOG_WARNING_FILE_PATH, 'a') as warning_log:
                    # Iterate through list
                    for temp_log_file in log_warning_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            warning_log.write(infile.read())

            # now the warning files if there are any
            log_error_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_errors*"))
            if len(log_error_file_list) > 0:
                log_error_file_list.sort()
                # doesn't yet exist, then create a blank one
                with open(self.LOG_ERROR_FILE_PATH, 'a') as error_log:
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
    def setup_custom_log(self, key_name, file_path_and_name):
        """
        Overview:
          - This goes to your custom log file only and not the default logger.

          - This is meant if you want to have a seperate file for some other
            purpose, maybe making a list of models to be used somewhere else.

          - You can have more than one custom logger at any given time. The trick
            is to use the correct 'key_name' when using the 'write_c_log'

        Inputs:
          - key_name: str: A name that will be used so you know what custom log file
            will be used. ie. model_list

          - file_path_and_name: the folder and file name to be used.
            ie) r"C:\ras2fim_data\output_ras2fim\12030105_2276_231017\final\model_list.csv".
            Note: The path must exist but the file does not need to.

        NOTE: If you hardcode part of the file_path_and_name, make sure you add the "r"
           flag in front of the file path or use double backslashs to solve problems
           of double slashs in pathing. Using os.path.joins can fix that too.
           ie) r"C:\ras2fim_data\output_ras2fim\12030105_2276_231017\final\model_list.csv"
           or
           "C:\\ras2fim_data\\output_ras2fim\\12030105_2276_231017\\final\\model_list.csv"
           Special backslashs combo's like \r, \t, \n can get create problems.

        NOTE: This has not yet been tested inside multi proc but shoudl work using the MP_LOG system

        """

        # -----------
        # Validation
        key_name = key_name.strip()
        if key_name == '':
            raise Exception("Internal Error: key_name can not be empty")

        # -----------
        # Check folder exists, folder must exist, but file does not need to.
        # If the file does exist, it will be deleted (and restarted later)

        file_path_and_name = file_path_and_name.strip()
        if file_path_and_name == '':
            raise Exception("Internal Error: file_path_and_name can not be empty")

        folder_path = os.path.dirname(file_path_and_name)
        if os.path.exists(folder_path) is False:
            raise Exception(
                "Internal Error: for the file_path_and_name argument, the file need not pre-exist"
                " but the folder path must. \nNote: Sometimes pathing can be wrong if you are not"
                " careful with backslashes (see code notes)"
            )

        # Ya ya.. a little weird. I need to make sure the custom one does not overwrite the
        # default file. I tried using file name and pathing, but backslashs were creating issues
        # during comparison. So, we will make sure the file name is not the same regardless of path.
        file_name = os.path.basename(file_path_and_name)
        def_file_name = os.path.basename(self.LOG_FILE_PATH)
        if def_file_name.lower() == file_name.lower():
            raise Exception(
                "Internal Error: the custom log file name you are creating"
                " already exists as the log system default file name, even if pathing"
                " is different. Please use a file name."
            )

        file_name_parts = os.path.splitext(file_path_and_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted file_path_and_name appears to be an invalid file name")

        if os.path.isfile(file_path_and_name):
            # Will be rebuilt later as needed. We don't want to concate to an pre-existing file
            os.remove(file_path_and_name)

        # -----------
        # Check to see if the key already exists in the dictionary.
        if key_name in self.CUSTOM_LOG_FILES_PATHS:
            raise Exception(f"Internal Error: Custom log key name of {key_name} already exists")

        # add to the global list
        self.CUSTOM_LOG_FILES_PATHS[key_name] = file_path_and_name

    # -------------------------------------------------
    def write_c_log(self, key_name, msg):
        """
        Overview:
          - setup_custom_log must be called exactly once to setup the key and file path so
            the key here will work.
          - It is fine to have an empty msg logged if you like.
        """
        # ie) RLOG.make_custom_log("model_list", "some path and file name")
        #     (or MP_LOG)
        # ie) RLOG.write_c_log("model_list", "hey there")
        #     (or MP_LOG)

        # -----------
        # Validation
        key_name = key_name.strip()
        if key_name == '':
            raise Exception("Internal Error: key_name can not be empty")

        # -----------
        # Check to see if the key exists in the dictionary.
        if key_name not in self.CUSTOM_LOG_FILES_PATHS:
            raise Exception(
                f"Internal Error: Custom log key name of {key_name} does not yet exist."
                " add_custom_log needs to be called first."
            )

        with open(self.CUSTOM_LOG_FILES_PATHS[key_name], "a") as f_log:
            f_log.write(msg + "\n")

    # -------------------------------------------------
    def trace(self, msg):
        # goes to file only, not console
        level = "TRACE   "  # keeps spacing the same
        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def lprint(self, msg):
        # goes to console and log file
        level = "LPRINT  "  # keeps spacing the same
        print(f"{msg} ")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def debug(self, msg):
        # goes to console and log file
        level = "DEBUG   "  # keeps spacing the same

        c_msg_type = f"{cl.fore.DODGER_BLUE_1}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def notice(self, msg):
        # goes to console and log file
        level = "NOTICE  "  # keeps spacing the same
        print(f"{cl.fore.TURQUOISE_2}{msg}{cl.style.RESET}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def success(self, msg):
        # goes to console and log file
        level = "SUCCESS "  # keeps spacing the same

        c_msg_type = f"{cl.fore.SPRING_GREEN_2B}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def warning(self, msg):
        # goes to console and log file and warning log file
        level = "WARNING "  # keeps spacing the same

        c_msg_type = f"{cl.fore.LIGHT_YELLOW}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to warning logs
        with open(self.LOG_WARNING_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def error(self, msg):
        # goes to console and log file and error log file
        level = "ERROR   "  # keeps spacing the same

        c_msg_type = f"{cl.fore.RED_1}<{level}>{cl.style.RESET}"
        print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.LOG_ERROR_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def critical(self, msg):
        level = "CRITICAL"  # keeps spacing the same

        c_msg_type = f"{cl.style.BOLD}{cl.fore.RED_3A}{cl.back.WHITE}{self.__get_dt()}"
        c_msg_type += f" <{level}>"
        print(f" {c_msg_type} : {msg} {cl.style.RESET}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.LOG_ERROR_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")
