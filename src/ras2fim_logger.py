#!/usr/bin/env python3

import os
import sys
from loguru import logger


# Globally scoped
LOG_FILES_PATHS = []
LOG_DEFAULT_FOLDER = ""
LOG_DEFAULT_FILE_PATH = ""

# This catchs any exceptions not caught in other places and logs them.
@logger.catch(level="CRITICAL", message="A critical error has been caught by the logging module")
class RAS2FIM_logger():

  # Simply uses the logger to be accessible to all who use this class.
  # You can call logger directly but might loose some features (well.. sort of)

  """
  Notes:
    All uses of this class write to one or more log files, assuming that the "setup" function has been called.
    When you wish an output to screen only and not to a log, use the standard "print(...)" command.
  """

  log = logger  # pointer to the logger.logger class

  # -------------------------------------------------
  def __init__(self):    

    console_logger_format = (
          "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green>"
          " <level>({level: <8})</level>"
          " || <level>{message}</level>"
    )

    # remove() resets all basic defaulted sinks (add's) so we can add our own
    logger.remove()

    # This means DEBUG and higher will be printed to console, but not TRACE 
    # which is used for output logs only
    logger.add(sys.stderr, format=console_logger_format, level="DEBUG")

  # -------------------------------------------------
  def setup(self, unit_output_folder: str, log_file_name:str = "ras2fim.log"):

    """
      # folder named "logs" will automatically added to the output folder name:
      # eg. C:\ras2fim_data\output_ras2fim\12030105_2276_230928\logs

      Each Level stated below will include the stated level and all with a higher int.
      We have overridden some of the level behaviours.
      Levels Available are:
        TRACE (5):  to file only
        DEBUG (10): default file and console
        INFO (20): default file and console
        SUCCESS (25): default file and console
        WARNING (30): default file and console
        ERROR (40): default file, console and also to "{log_file_name}_errors.log"
        CRITICAL (50): default file, console and also to "{log_file_name}_errors.log"
    """

    file_logger_format = ("{time:YYYY-MM-DD > HH:mm:ss!UTC} ({level}) || {message}")
   
    # TODO: how do we tell the user that the default has already been setup
    # ie) ras2fim.py can set it up and it auto proprogates to all children
    # However, a child might attempt to setup its own default and it will not be
    # honored unless the child script is running by it's own.

    # -----------
    # Validation
    if unit_output_folder is None:
      raise ValueError("Error: unit output folder path not defined")
           
    unit_output_folder = unit_output_folder.strip()
    
    if unit_output_folder == "":
      raise ValueError("Error: unit output folder path can not be empty")
    
    if os.path.exists(unit_output_folder) is False:
      raise ValueError(f"Error: unit_output_path of {unit_output_folder} does not exist")

    log_file_name = log_file_name.strip()

    if log_file_name == "":
      raise ValueError("Error: log_file_name can not be empty")
    
    # pull out the file name without extension
    file_name_parts = os.path.splitext(log_file_name)
    if len(file_name_parts) != 2:
      raise ValueError("The submitted log_file_name appears to be an invalid file name")

    # -----------
    # processing
    try:
      
      # Allows us to write the app wide global variables
      global LOG_DEFAULT_FOLDER, LOG_DEFAULT_FILE_PATH, LOG_FILES_PATHS

      #print(f"LOG_DEFAULT_FILE_PATH is {LOG_DEFAULT_FILE_PATH}")
      # Shouldn't be changed once set. Hard to enforce as someone can call directly 
      # to the variable instead of via "setup"
      if LOG_DEFAULT_FOLDER != "": 

        # TODO: Do we raise an error sayign setup has been added already and they need
        # to use add_log_code and path?
        return

      LOG_DEFAULT_FOLDER = os.path.join(unit_output_folder, "logs")

      # Shouldn't be changed once set. Hard to enforce as someone can call directly 
      # to the variable instead of via "setup"
      # but.. does not have to be ras2fim.log. ie). maybe get_models_catalog wants to use it,
      # it might set it's default to somethign other than ras2fim.log
      def_log_file = os.path.join(LOG_DEFAULT_FOLDER, log_file_name)
      LOG_DEFAULT_FILE_PATH = def_log_file
      LOG_FILES_PATHS.append(def_log_file)

      # By default, file and console logs only level "DEBUG" (10) and higher,
      # and for file logs, we want it all. (see __init__ were we setup the "debug" default level)
      logger.add(def_log_file, format=file_logger_format, level="TRACE", enqueue=True, mode="w")

      # For levels of ERROR and CRITICAL, they will get logged in the standard log file, but
      # also to a second log files specificall for errors and critical messages.
      # The log file name is the root file name plus "_errors.log". ie) ras2fim_errors.log
      error_log_file = os.path.join(LOG_DEFAULT_FOLDER, file_name_parts[0] + "_errors.log")
      logger.add(error_log_file, format=file_logger_format, level="ERROR", backtrace=True, enqueue=True, mode="w")

    except Exception as ex:
        print("An internal error occurred while setting up the logging system")
        print(ex)
        logger.exception(ex)
        raise(ex)


  # -------------------------------------------------
  #def add_log_folder_paths(self, log_file_name: str, log_subfolder_name:str = ""):
    """
    This will check to see if a path has been added
    """
    # -----------
    # Validation

    # ensure log_file_name is not empty

    # Add subfolder if not empty and not already there

    # strip

    # -----------
    """
    if log_subfolder_name == "":
      new_file_path = os.path.join(FIM_logger.LOG_DEFAULT_FOLDER, log_file_name)
    else:
      new_file_path = os.path.join(FIM_logger.LOG_DEFAULT_FOLDER, log_subfolder_name, log_file_name)

    if new_file_path not in FIM_logger.LOG_FILES_PATHS:
        logger.add(new_file_path, format = FIM_logger.__logger_format_file, level="TRACE", enqueue=True, mode="w")        
        FIM_logger.LOG_FILES_PATHS.append(new_file_path)
    """

  # -------------------------------------------------
  #def remove_log_folder_paths(self, log_file_name: str, log_subfolder_name:str = ""):
