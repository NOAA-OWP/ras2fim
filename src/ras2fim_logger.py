#!/usr/bin/env python3

import os
import sys

from loguru import logger


# Globally scoped
CUSTOM_LOG_FILES_PATHS = {}
LOG_DEFAULT_FOLDER = ""
LOG_DEFAULT_FILE_PATH = ""
LOG_DEFAULT_FILE_NAME = ""
LOG_SYSTEM_IS_SETUP = False


# -------------------------------------------------
@logger.catch(level="CRITICAL", message="A critical error has been caught by the logging module")
class RAS2FIM_logger:
    # Simply uses the logger to be accessible to all who use this class.
    # You can call logger directly but might loose some features (well.. sort of)

    """
    Notes:
      - All uses of this class write to one or more log files, assuming that the "setup" function
        has been called. When you wish an output to screen only and not to a log, use the standard command.
        "print(...)"
      - Loguru is thread-safe but is not multi-proc safe. You can use the "enqueue=True" flag to help
         with this. Testing is required.

    """

    log = logger  # pointer to the logger.logger class

    # -------------------------------------------------
    def __init__(self):
        # remove() resets all basic defaulted sinks (add's) so we can add our own
        logger.remove()

        console_trace_format = " {message}"
        console_non_trace_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green>"
            " <level>({level: <8})</level>"
            " || <level>{message}</level>"
        )

        # DEBUG level and higher will use the enhanced loggin
        logger.add(sys.stderr, format=console_non_trace_format, level="DEBUG")

        # TRACE level uses a simpler format and it works for TRACE level only
        logger.add(
            sys.stdout,
            format=console_trace_format,
            level="TRACE",
            filter=lambda record: record["level"].name == "TRACE",
        )

    # -------------------------------------------------
    def setup(self, output_folder: str, log_file_name: str = "ras2fim.log"):
        """
        Note: for ras2fim.py or most files in the /src directory it is recommended that
        you use the unit output folder:  ie) C:\ras2fim_data\output_ras2fim\12030105_2276_231017

        Folder named "logs" will automatically added to the output folder name:
        eg. C:\ras2fim_data\output_ras2fim\12030105_2276_230928\logs

        Each Level stated below will include the stated level and all with a higher int.
        We have overridden some of the level behaviours.
        Levels Available are:
          TRACE (5):  default file and console but with different formatting
          DEBUG (10): default file and console
          INFO (20): default file and console
          SUCCESS (25): default file and console
          WARNING (30): default file and console
          ERROR (40): default file, console and also to "{log_file_name}_errors.log"
          CRITICAL (50): default file, console and also to "{log_file_name}_errors.log"

        NOTE: If you hardcode part of the file_path_and_name, make sure you add the "r"
        flag in front of the file path or use double backslashs to solve problems
        of double slashs in pathing. Using os.path.joins can fix that too.
        ie) r"C:\ras2fim_data\output_ras2fim\12030105_2276_231017\final\model_list.csv"
        or
        "C:\\ras2fim_data\\output_ras2fim\\12030105_2276_231017\\final\\model_list.csv"
        Special backslashs combo's like \r, \t, \n can get create problems.

        """

        # We are setting up file level logging. Console level logging was setup
        # in the __init__ method

        # Allows us to write the app wide global variables
        global LOG_DEFAULT_FOLDER, LOG_DEFAULT_FILE_PATH
        global LOG_DEFAULT_FILE_NAME, LOG_SYSTEM_IS_SETUP

        file_logger_format = "{time:YYYY-MM-DD > HH:mm:ss!UTC} ({level}) || {message}"

        # TODO: how do we tell the user that the default has already been setup
        # ie) ras2fim.py can set it up and it auto proprogates to all children
        # However, a child might attempt to setup its own default and it will not be
        # honored unless the child script is running by it's own.

        # -----------
        # Validation
        if output_folder is None:
            raise ValueError("Error: unit output folder path not defined")

        output_folder = output_folder.strip()

        if output_folder == "":
            raise ValueError("Error: unit output folder path can not be empty")

        if os.path.exists(output_folder) is False:
            raise ValueError(f"Error: unit_output_path of {output_folder} does not exist")

        log_file_name = log_file_name.strip()

        if log_file_name == "":
            raise ValueError("Error: log_file_name can not be empty")

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        # Shouldn't be changed once set. Hard to enforce as someone can call directly
        # to the variable instead of via "setup"
        if LOG_DEFAULT_FOLDER != "":
            raise Exception("The default log folder and name has already been setup")

        # -----------
        # processing
        try:
            # We are setting up file level logging. Console level logging was setup
            # in the __init__ method
            LOG_DEFAULT_FOLDER = os.path.join(output_folder, "logs")
            LOG_DEFAULT_FILE_NAME = log_file_name

            # Shouldn't be changed once set. Hard to enforce as someone can call directly
            # to the variable instead of via "setup"
            # but.. does not have to be ras2fim.log. ie). maybe get_models_catalog wants to use it,
            # it might set it's default to somethign other than ras2fim.log
            def_log_file = os.path.join(LOG_DEFAULT_FOLDER, log_file_name)
            LOG_DEFAULT_FILE_PATH = def_log_file

            # Default behaviour for loguru is to file log only DEBUG and higher.
            # As we are highjacking TRACE via lprint, we want it all to go to the log
            # file but the log files will all share the same format

            logger.add(def_log_file, format=file_logger_format, level="TRACE", enqueue=True, mode="w")
            # logger.add(def_log_file, format=file_logger_format, level="TRACE", mode="w")

            # ---------------
            # For levels of ERROR and CRITICAL, they will get logged in the standard log file, but
            # also to a second log files specificall for errors and critical messages.
            # The log file name is the root file name plus "_errors.log". ie) ras2fim_errors.log
            error_log_file = os.path.join(LOG_DEFAULT_FOLDER, file_name_parts[0] + "_errors.log")

            logger.add(
                error_log_file,
                format=file_logger_format,
                level="ERROR",
                backtrace=True,
                enqueue=True,
                mode="w",
            )

            """
            logger.add(
                error_log_file,
                format=file_logger_format,
                level="ERROR",
                backtrace=True,
                mode="w",
            )
            """

            LOG_SYSTEM_IS_SETUP = True

        except Exception as ex:
            print("An internal error occurred while setting up the logging system")
            print(ex)
            logger.exception(ex)
            raise (ex)

    # -------------------------------------------------
    def setup_custom_log(self, key_name, file_path_and_name):
        """
        Overview:
          - This goes to a log file only and not the default logger.

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

        """

        global CUSTOM_LOG_FILES_PATHS, LOG_DEFAULT_FILE_NAME

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
        if LOG_DEFAULT_FILE_NAME.lower() == file_name.lower():
            raise Exception(
                "Internal Error: the custom log file name you are creating"
                " already exists as the log system default file name, even if pathing"
                " is different. Please use a file name."
            )

        if os.path.isfile(file_path_and_name):
            # Will be rebuilt later as needed. We don't want to concate to an pre-existing file
            os.remove(file_path_and_name)

        # -----------
        # Check to see if the key already exists in the dictionary.
        if key_name in CUSTOM_LOG_FILES_PATHS:
            raise Exception(f"Internal Error: Custom log key name of {key_name} already exists")

        # add to the global list
        CUSTOM_LOG_FILES_PATHS[key_name] = file_path_and_name

    # -------------------------------------------------
    def write_c_log(self, key_name, msg):
        """
        Overview:
          - setup_custom_log must be called exactly once to setup the key and file path so
            the key here will work.
          - It is fine to have an empty msg logged if you like.
        """
        # ie) RLOG.make_custom_log("model_list", "some path and file name")
        # ie) RLOG.write_c_log("model_list", "hey there")

        global CUSTOM_LOG_FILES_PATHS

        # -----------
        # Validation
        key_name = key_name.strip()
        if key_name == '':
            raise Exception("Internal Error: key_name can not be empty")

        # -----------
        # Check to see if the key exists in the dictionary.
        if key_name not in CUSTOM_LOG_FILES_PATHS:
            raise Exception(
                f"Internal Error: Custom log key name of {key_name} does not yet exist."
                " add_custom_log needs to be called first."
            )

        with open(CUSTOM_LOG_FILES_PATHS[key_name], "a") as log_file:
            log_file.write(msg + "\n")

    # -------------------------------------------------
    # methods to extend standard logging properties
    # Makes it simplier for usage calls
    # And yes.. we renamed 'trace' to 'lprint', so people can use print or lprint
    def lprint(self, msg):
        logger.trace(msg)

    def debug(self, msg):
        logger.debug(msg)

    def info(self, msg):
        logger.info(msg)

    def success(self, msg):
        logger.success(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)

    def critical(self, msg):
        logger.critical(msg)
