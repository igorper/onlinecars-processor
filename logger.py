import os
import logging
import sys


class LoggerFramework:

    def configure_logger(logger_name):
        logs_folder_exists = os.path.exists("logs")
        if not logs_folder_exists:
            os.makedirs("logs")

        logger = logging.getLogger(logger_name)
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s |  %(levelname)s: %(message)s')
        logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        info_handler = logging.FileHandler(filename="logs/info.log")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        debug_handler = logging.FileHandler(filename="logs/debug.log")
        debug_handler.setFormatter(formatter)
        debug_handler.setLevel(logging.DEBUG)
        logger.addHandler(stream_handler)
        logger.addHandler(debug_handler)
        logger.addHandler(info_handler)

        if not logs_folder_exists:
            logger.debug("Created 'logs' folder")

        return logger
