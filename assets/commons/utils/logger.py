import logging
import sys
import os
import time
from contextvars import ContextVar

import psutil
from tqdm import tqdm

# Disable tqdm's TMonitor used for maxinterval. Thread remains open after tqdm closes,
# which is a tad misleading when the application hangs. Since we don't use maxinterval,
# we have no need for that thread.
# Relevant:
# - https://github.com/tqdm/tqdm/issues/1564
# - https://github.com/tqdm/tqdm#monitoring-thread-intervals-and-miniters
tqdm.monitor_interval = 0

try:
    # Only works on Unix
    import resource
    can_measure_maxrss = True
except ImportError:
    can_measure_maxrss = False


class CustomFormatter(logging.Formatter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._psutil = psutil.Process(os.getpid())

    def format(self, record):
        string = super().format(record)
        memstring = f"{str(int(self._psutil.memory_info().rss / 1024 ** 2))}Mb"
        if can_measure_maxrss:
            memstring += f"/{str(int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))}Mb"
        return (
            f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {memstring} {string}"
        )


class MaxLevelFilter(logging.Filter):
    '''Filters (lets through) all messages with level < LEVEL'''

    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno < self.level  # "<" instead of "<=": since logger.setLevel is inclusive, this should be exclusive


logging_ctx_name: ContextVar = ContextVar("name_ctx", default=None)


class NameContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):
        ctx = logging_ctx_name.get()
        if ctx is not None:
            record.name = f"{record.name}[{ctx}]"
        return True


def makeCustomLogger(
    name=None, min_level=logging.INFO, format="%(name)s %(levelname)s - %(message)s"
):
    # Create formatter
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = CustomFormatter(format)
    context_filter = NameContextFilter()

    # Create handlers
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))  # messages lower than WARNING go to stdout
    stdout_handler.setLevel(min_level)
    stdout_handler.addFilter(context_filter)
    stdout_handler.setFormatter(formatter)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.addFilter(context_filter)
    stderr_handler.setFormatter(formatter)

    # create logger
    LOGGER = logging.getLogger(name)
    LOGGER.handlers.clear()
    LOGGER.setLevel(min_level)
    LOGGER.addHandler(stdout_handler)
    LOGGER.addHandler(stderr_handler)
    LOGGER.propagate = False

    return LOGGER


LOGGER = makeCustomLogger(__name__)


class logging_tqdm(tqdm):
    def __init__(
        self, *args, leave: bool = False, logger: logging.Logger = None, **kwargs
    ):
        self._logger = logger
        self._last_log_n = -1
        super().__init__(*args, leave=leave, **kwargs)

    @property
    def logger(self):
        if self._logger is not None:
            return self._logger
        return LOGGER

    def display(self, msg=None, pos=None):
        if not self.n:
            # skip progress bar before having processed anything
            return
        if self.n == self._last_log_n:
            # avoid logging for the same progress multiple times
            return
        self._last_log_n = self.n
        if msg is None:
            msg = self.__str__()
        if not msg:
            msg = self.__str__()
        self.logger.info('%s', msg)