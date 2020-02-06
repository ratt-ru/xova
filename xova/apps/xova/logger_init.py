# -*- coding: utf-8 -*-

import logging
import sys


from loguru import logger
from daskms.columns import ColumnMetadataError


class InterceptHandler(logging.Handler):
    """Intercept log messages are reroute them to the loguru logger."""

    def emit(self, record):
        exc_info = record.exc_info

        # Filter out daskms ColumnMetadataError
        if (record.levelno == logging.WARNING and exc_info
                and exc_info[0] == ColumnMetadataError):
            return

        # Retrieve context where the logging call occurred, this happens to be
        # in the 7th frame upward.
        logger_opt = logger.opt(depth=7, exception=exc_info)
        logger_opt.log(record.levelname, record.getMessage())


logging.basicConfig(handlers=[InterceptHandler()], level="WARNING")

# Put together a formatting string for the logger.
# Split into pieces to improve legibility.
tim_fmt = "<green>{time:YYYY-MM-DD HH:mm:ss}</green>"
lvl_fmt = "<level>{level: <8}</level>"
src_fmt = "<cyan>{module}</cyan>:<cyan>{function}</cyan>"
msg_fmt = "<level>{message}</level>"

fmt = " | ".join([tim_fmt, lvl_fmt, src_fmt, msg_fmt])

config = {
    "handlers": [
        {"sink": sys.stderr,
         "level": "INFO",
         "format": fmt},
        # {"sink": "{time:YYYYMMDD_HHmmss}_xova.log",
        #  "level": "DEBUG",
        #  "format": fmt,
        #  }
    ],
}

logger.configure(**config)
