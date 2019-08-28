import sys


# abstract class attributes are complicated https://stackoverflow.com/a/45250114
# use a descriptor instead that fails _upon use_: https://docs.python.org/3.5/howto/descriptor.html
class AbstractAttribute:
    """TODO"""
    def __get__(self, obj, type=None):
        raise NotImplementedError('Class "{c}" has an undefined abstract class attribute'.format(c=type.__name__))


def log(msg, file=sys.stderr, asline=True, header='biodb'):
    if asline:
        msg = '[{h}] {m}\n'.format(h=header, m=msg)
    file.write(msg)


class BiodbError(RuntimeError):
    pass


class ProgrammingError(BiodbError):
    pass
