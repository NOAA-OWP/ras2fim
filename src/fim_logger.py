#!/usr/bin/env python3

import os
import sys
from loguru import logger

"""
This class extends the loguru logger to change some properties and methods.
Things you can do:
- You can log a message to a default log path and file name.
- You can log a message to a default log path and file name AND print to console.
  (PS... if you want stictly console, you the standard "print" command)
- You can add extra log files (and path), on demand if you like too

"""

# Globally scoped
LOG_FILES_PATHS = ()
LOG_DEFAULT_FILE_PATH = ""

@logger.catch(level="CRITICAL", message="A critical error has been caught by the logging module")
class FIM_logger(logger):

  __logger_format_file = ("{time:YYYY-MM-DD > HH:mm:ss!UTC} ({level}) || {message}")

  # -------------------------------------------------
  def __init__(self):

    logger_format_console = (
          "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green>"
          " <level>({level: <8})</level>"
          " || <level>{message}</level>"
    )

    # This means DEBUG and higher will be printed to console, but not TRACE 
    # which is used for output logs only
    logger.add(sys.stderr, format=logger_format_console, level="DEBUG", enqueue=True)


  # -------------------------------------------------
  # TODO: pull out level "ERROR" and "CRITICAL" to addition file
  def set_default_logfile(self, unit_output_folder: str, log_file_name = "log.txt"):

    """
      # folder named "logs" will automatically added to the output folder name:
      # eg. C:\ras2fim_data\output_ras2fim\12030105_2276_230928\logs

    """

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

    if log_file_name.strip() == "":
      raise ValueError("Error: unit output folder path can not be empty")

    # -----------
    # processing
    try:
      
      # TODO: 
      # Check to see if the default folder path exists

      # TODO: Paths versus files names ??
      # We default one, but what if they want something unique for a module or set.
      def_log_folder = os.path.join(unit_output_folder, "logs")
      def_log_file = os.path.join(def_log_folder, log_file_name)

      FIM_logger.LOG_FILES_PATHS.add(unit_output_folder)
      FIM_logger.LOG_DEFAULT_FILE_PATH = def_log_file

      logger.remove()

      # By default, file and console logs only level "DEBUG" (10) and higher,
      # and for file logs, we want it all.
      logger.add(def_log_file, format=FIM_logger.__logger_format_file, level="TRACE", enqueue=True, mode="w")

    except Exception as ex:
        print("An internal error occurred while setting up the logging system")
        print(ex)
        logger.exception(ex)
        raise(ex)


  def add_log_folder_paths(self, log_file):
      """
      This will check to see if a path has been added
      """

      if log_file not in LOG_FILES_PATHS:
         LOG_FILES_PATHS.append(log_file)

