import sys
from datetime import datetime

import logging
logging.basicConfig(
    level=logging.WARNING,
    format='[{t}] %(message)s'.format(t=datetime.now().strftime('%H:%M:%S'))
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# abstract class attributes are complicated https://stackoverflow.com/a/45250114
# use a descriptor instead that fails _upon use_: https://docs.python.org/3.5/howto/descriptor.html
class AbstractAttribute:
    """TODO"""
    def __get__(self, obj, type=None):
        raise NotImplementedError('Class "{c}" has an undefined abstract class attribute'.format(c=type.__name__))


class BiodbError(RuntimeError):
    pass


class ProgrammingError(BiodbError):
    pass
