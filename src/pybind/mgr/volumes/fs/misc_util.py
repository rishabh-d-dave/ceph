import errno
import stat
from os.path import join as os_path_join
from logging import getLogger


import cephfs
from .exception import VolumeException


log = getLogger(__name__)


def get_size_h(num):
    '''
    Convert size to human-readable format.
    '''
    size = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')

    if num < 1024:
        return f'{num} {size[0]}'

    i = 0
    while True:
        num = num / 1024
        i += 1
        if i >= len(size):
            log.error(f"Biggest suffix we have is '{len(size)-1}' "
                       'but this is not sufficient in this case. '
                      f'Current size = {num}')

        x, y = str(num).split('.')
        if len(x) <= 3:
            if y:
                y = '' if y == '0' else y[:4]

            return f'{x}.{y} {size[i]}' if y else f'{x} {size[i]}'


def get_num_h(num):
    '''
    Convert number to human-readable format.
    '''
    size = ('', 'K', 'M', 'B', 'T')

    if num < 1000:
        return f'{num}'

    i = 0
    while True:
        num = num / 1000
        i += 1
        if i >= len(size):
            log.error(f"Biggest suffix we have is '{len(size)-1}' "
                       'but this is not sufficient in this case. '
                      f'Current size = {num}')

        x, y = str(num).split('.')
        if len(x) <= 3:
            if y:
                y = '' if y == '0' else y[:4]

            return f'{x}.{y} {size[i]}' if y else f'{x} {size[i]}'


class Stats:
    '''
    Store statistics of the given path that'll be copied/deleted/purged (or
    handled, in short) in an organized manner.
    '''
    def __init__(self, op):
        self.size_t = 0     # total size
        self.rfiles_t = 0   # total regfiles
        self.dirs_t = 0     # total directories
        self.slinks_t = 0   # total symlinks

        # handled = copied, delete/purged, etc.
        self.size_h = 0     # size handled
        self.rfiles_h = 0   # num of regfiles handled
        self.dirs_h = 0     # num of dirs handled
        self.slinks_h = 0   # num of symlinks handled

        self.op = op
        # creating self.oped so that we can pring log messages accordingly.
        self.oped_str = self._get_operated_str() # oped = operated

    @property
    def percent(self):
        if self.size_t == 0:
            return 100 if self.size_h == 0 else 0
        return ((self.size_h/self.size_t) * 100)

    @property
    def progress_fraction(self):
        '''
        For progress MGR module (mgr/progress), value of attribute "progress"
        should be in range of 0.0 to 1.0. 0.0 represents 0% progress and 1.0
        represents 100% progress.
        '''
        return round(self.percent, 2)/100

    def _get_operated_str(self):
        '''
        Return past tense form of the operation for which this instance is
        being used.
        '''
        if self.op == 'copy':
            return 'copied'
        elif self.op == 'purge':
            return 'purged'
        else:
            log.critical('Class Stats is being used for an operation it '
                         'doesn\'t recognize')

    def get_size_stat(self, human=True):
        if human:
            x, y = get_size_h(self.size_h), get_size_h(self.size_t)
        else:
            x, y = self.size_h, self.size_t

        return f'{x}/{y}'

    def get_rfiles_stat(self, human=True):
        if human:
            x, y = get_num_h(self.rfiles_h), get_num_h(self.rfiles_t)
        else:
            x, y = self.rfiles_h, self.rfiles_t

        return f'{x}/{y}'

    def get_dirs_stat(self, human=True):
        if human:
            x, y = get_num_h(self.dirs_h), get_num_h(self.dirs_t)
        else:
            x, y = self.dirs_h, self.dirs_t

        return f'{x}/{y}'

    def get_slinks_stat(self, human=True):
        if human:
            x, y = get_num_h(self.slinks_h), get_num_h(self.slinks_t)
        else:
            x, y = self.slinks_h, self.slinks_t

        return f'{x}/{y}'

    def get_progress_report(self, human=True):
        return {
            'percentage completed': self.percent,
            f'amount {self.oped_str}': self.get_size_stat(human=human),
            f'regfiles {self.oped_str}': self.get_rfiles_stat(human=human),
            f'dirs {self.oped_str}': self.get_dirs_stat(human=human),
            f'symlinks {self.oped_str}': self.get_slinks_stat(human=human)
        }

    def get_progress_report_str(self):
        progress_report_str = ''
        for k, v in self.get_progress_report().items():
            progress_report_str += f'{k} = {v}\n'

        # remove extra new line character at end, since print and log.xxx()
        # methods automatically add a newline at the end.
        return progress_report_str[:-1]

    def log_handled_amount(self):
        log.info('Following are statistics for amount handled vs. total '
                 'amount -')
        for k, v in self.get_progress_report().items():
            log.info(f'{k} = {v}')

    def log_total_amount(self):
        log.info('Following are statistics for source path -')
        log.info(f'    total size = {get_size_h(self.size_t)}')
        log.info(f'    total number of regular files = {get_num_h(self.rfiles_t)}')
        log.info(f'    total number of directories = {get_num_h(self.dirs_t)}')
        log.info(f'    total number of symbolic links = {get_num_h(self.slinks_t)}')


def get_statistics_for_path(given_path, fsh, op, should_cancel):
    '''
    Get statistics like of total number of files and total size of these the
    files at the given path.

    This method was originally written to provide a progress report to user.
    '''
    def traverse_tree(given_path, stats):
        try:
            with fsh.opendir(given_path) as dir_handle:
                d = fsh.readdir(dir_handle)
                while d and not should_cancel():
                    if d.d_name not in (b".", b".."):
                        d_full_src = os_path_join(given_path, d.d_name)
                        stx = fsh.statx(d_full_src, cephfs.CEPH_STATX_MODE  |
                                                    cephfs.CEPH_STATX_SIZE,
                                                    cephfs.AT_SYMLINK_NOFOLLOW)
                        if stat.S_ISDIR(stx["mode"]):
                            stats.dirs_t += 1
                            traverse_tree(d_full_src, stats)
                        elif stat.S_ISLNK(stx["mode"]):
                            stats.slinks_t += 1
                        elif stat.S_ISREG(stx["mode"]):
                            stats.rfiles_t += 1
                            stats.size_t += stx['size']
                    d = fsh.readdir(dir_handle)
        except cephfs.Error as e:
            if not e.args[0] == errno.ENOENT:
                raise VolumeException(-e.args[0], e.args[1])

        return stats

    stats = traverse_tree(given_path, Stats(op=op))
    stats.log_total_amount()
    if should_cancel():
        raise VolumeException(-errno.EINTR, "user interrupted clone operation")
    return stats
