# src/utils/logger.py
import logging
import os
from pathlib import Path

def get_logger(logger_name=__name__, log_dir="logs", log_file="generic.log"):
    """
        Sets up and returns a logger instance
        Args:
            logger_name (str): Name of the logger
            log_dir (str): Directory to store log files
            log_file (str): Log file name
        Returns:
            logging.Logger: Configured logger instance
    """
    
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    LOG_DIR = BASE_DIR / log_dir
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_PATH = os.path.join(LOG_DIR, log_file)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_PATH, mode='a')
        formatter = logging.Formatter(
            "[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger