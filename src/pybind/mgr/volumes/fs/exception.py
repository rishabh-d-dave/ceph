from errno import errorcode
from logging import getLogger
from traceback import format_stack


log = getLogger(__name__)


class VolumeException(Exception):
    '''
    Generic exception for CephFS volumes plugin.
    '''

    def __init__(self, errno=None, errmsg=None, exception=None):
        assert exception or (errno and errmsg)
        assert not (exception and (errno or errmsg))

        self.exception = exception
        self.errmsg = errmsg

        self.errno = errno
        if self.errno > 0:
            self.errno = -self.errno
        if self.errno:
            self.errname = errorcode.get(abs(self.errno), "UNKNOWN_ERROR")

        # to be logged when exceptions are left unhandled beyond bounds of
        # volumes plugin or when error is returned/printed due to an exception.
        self.traceback = 'Traceback -\n' + ''.join(format_stack())

        log.debug(str(self).replace(self.traceback, ''))

    def to_tuple(self):
        return self.errno, "", self.errmsg

    def __str__(self):
        return (f'ERROR: {self.__class__.__name__}: errno = {self.errno}, '
                f'errname={self.errname} errmsg="{self.errmsg}"\n'
                f'self.exception = {self.exception}')


class InvalidUuidError(VolumeException):
    def __init__(self, errno, errmsg):
        super(InvalidUuidError, self).__init__(errno, errmsg)


class MetadataMgrException(VolumeException):
    def __init__(self, errno, errmsg):
        super(MetadataMgrException, self).__init__(errno, errmsg)


class IndexException(VolumeException):
    def __init__(self, errno, errmsg):
        super(IndexException, self).__init__(errno, errmsg)


class OpSmException(VolumeException):
    def __init__(self, errno, errmsg):
        super(OpSmException, self).__init__(errno, errmsg)


class SubvolUpgradeError(VolumeException):
    '''
    Raised when subvolume can't be auto-upgraded.
    '''

    def __init__(self, errno, errmsg):
        super(SubvolUpgradeError, self).__init__(errno, errmsg)


class EvictionError(VolumeException):
    pass


class ClusterTimeout(Exception):
    """
    Exception indicating that we timed out trying to talk to the Ceph cluster,
    either to the mons, or to any individual daemon that the mons indicate ought
    to be up but isn't responding to us.
    """
    pass

class ClusterError(Exception):
    """
    Exception indicating that the cluster returned an error to a command that
    we thought should be successful based on our last knowledge of the cluster
    state.
    """
    def __init__(self, action, result_code, result_str):
        self._action = action
        self._result_code = result_code
        self._result_str = result_str

    def __str__(self):
        return (f'Error: {self.__class__.__name__} {self._result_code} '
                f'"{self._result_str}" while {self._action}')
