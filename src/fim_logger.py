#!/usr/bin/env python3

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
class FIM_logger(logger):

    LOG_FILES = []

    def __init__(self):

        """
        When the class is instantiated (setup up if you will), it will automatically
        set up a shared Log foler and log file. It creates the log file as messages come in
        """

    def add_more_log_folder_paths(self, addition_log_file):
        """
        While we need the default for all scenarios, you can add extra log
        """
