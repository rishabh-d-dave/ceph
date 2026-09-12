import errno as module_errno
from logging import getLogger


log = getLogger(__name__)


class VolumeException(Exception):
    def __init__(self, errno, errmsg):
        self.errno = errno
        self.errname = module_errno.errorcode[self.errno]
        self.errmsg = errmsg
        log.info(f'{self}')

    def to_tuple(self):
        return self.errno, "", self.errmsg

    def __str__(self):
        return (f'ERROR: {self.__class__.__name__}: errno={self.errno}, '
                f'errname={self.errname} errmsg="{self.errmsg}"')


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

class NotImplementedException(Exception):
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

class EvictionError(Exception):
    pass
