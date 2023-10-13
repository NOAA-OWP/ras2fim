
import sys

#import fim_logger as fl

# ---------------
# starts up the default logging system for this page.
#fl.FIM_logger()
# ---------------

from loguru import logger


@logger.catch(level="CRITICAL", message="An error caught in test()")

# -------------------------------------------------
def run_logging_test():

    # Many of the details about how this system works can be found at
    # https://betterstack.com/community/guides/logging/loguru/

    try:
        print()
        print("hey there")

        logger_format_console = (
            "<green>{time:YYYY-MM-DD HH:mm:ss!UTC}</green>"
            " <level>({level: <8})</level>"
            " || <level>{message}</level>"
        )
        logger_format_file = ("{time:YYYY-MM-DD > HH:mm:ss!UTC} ({level}) || {message}")

        logger.remove()

        # By default, file and console logs only level "DEBUG" (10) and higher

        # console output settings
        logger.add(sys.stderr, format=logger_format_console, level="DEBUG", enqueue=True)

        # Once we have a log file name
        # How do we allow for multiple log files in code if we need to?  Maybe new class starting with 
        # log name, then message and level? Code has to keep track of its own path?  
        logger.add(r"C:\Temp\logs\my_err_file.log", format=logger_format_file, level="TRACE", enqueue=True, mode="w")

        # This send debug to logs but not to screen, but not formatted the way I want
        # add console sink
        #logger.configure(handlers=[{"sink": sys.stderr,
        #                            "level": "DEBUG", 
        #                            "format" : "{time:YYYY-MM-DD > HH:mm:ss!UTC} ({level}) || {message}",
        #                            "enqueue" : "True"}
        #                            ])
        # look into backtrace for some levels 

        #logger.add(r"C:\Temp\logs\my_err_file.log", mode="w")

        logger.trace("Happy logging with Loguru!")
        logger.debug("Now we are debugging")

        # Defaults to DEBUG, but this now defaults to INFO
        #logger.remove(0)
        #logger.add(sys.stderr, level="INFO")

        # TODO: 
        # current sample output
        # 2023-09-29 16:00:47.067 | SUCCESS  | __main__:run_logging_test:24 - Yay Baby
        # change "__main__" to the file name.

        # change to UTC


        logger.trace("More tracing")
        logger.debug("Sup")
        logger.info("oohh... some info")
        logger.success("Yay Baby")
        logger.warning("Careful now")

        #myvar = 50/0

        #logger.success("oh no.. after divide by zero")

        #logger.remove()


        logger.debug("are we good to file this?")


    except Exception as ex:
        print("zooiks")
        print(ex)
        logger.exception(ex)
        print("now what")


# -------------------------------------------------
if __name__ == "__main__":

    run_logging_test()