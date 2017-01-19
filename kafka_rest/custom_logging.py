"""Adds TRACE level logging, which is below DEBUG."""

import logging
import types

TRACE_LEVEL = 5

logging.addLevelName(TRACE_LEVEL, 'TRACE')

def _trace(self, msg, *args, **kwargs):
    self.log(TRACE_LEVEL, msg, *args, **kwargs)

def getLogger(name):
    logger = logging.getLogger(name)

    logger.trace = types.MethodType(_trace, logger)
    return logger
