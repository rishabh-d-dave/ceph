import os
import uuid
import logging
from contextlib import contextmanager

import cephfs

from .template import GroupTemplate
from ..exception import VolumeException
from ..fs_util import listdir_by_ctime_order

log = logging.getLogger(__name__)

class Trash(GroupTemplate):
    GROUP_NAME = "_deleting"

    def __init__(self, fs, vol_spec):
        self.fs = fs
        self.vol_spec = vol_spec
        self.groupname = Trash.GROUP_NAME

    @property
    def path(self):
        return os.path.join(self.vol_spec.base_dir.encode('utf-8'), self.groupname.encode('utf-8'))

    @property
    def unique_trash_path(self):
        """
        return a unique trash directory entry path
        """
        return os.path.join(self.path, str(uuid.uuid4()).encode('utf-8'))

    def _get_single_dir_entry(self, exclude_list=[]):
        exclude_list.extend((b".", b".."))
        try:
            with self.fs.opendir(self.path) as d:
                entry = self.fs.readdir(d)
                while entry:
                    if entry.d_name not in exclude_list:
                        return entry.d_name
                    entry = self.fs.readdir(d)
            return None
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def get_trash_entry(self, exclude_list):
        """
        get a trash entry excluding entries provided.

        :praram exclude_list: entries to exclude
        :return: trash entry
        """
        return self._get_single_dir_entry(exclude_list)

    def get_trash_entries_by_ctime_order(self):
        """
        get all trash entries.

        :return: list containing trash entries
        """
        return listdir_by_ctime_order(self.fs, self.path)

    def purge(self, trashpath, should_cancel):
        """
        purge a trash entry.

        :praram trash_entry: the trash entry to purge
        :praram should_cancel: callback to check if the purge should be aborted
        :return: None
        """
        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            try:
                with self.fs.opendir(root_path) as dir_handle:
                    d = self.fs.readdir(dir_handle)
                    while d and not should_cancel():
                        if d.d_name not in (b".", b".."):
                            d_full = os.path.join(root_path, d.d_name)
                            if d.is_dir():
                                rmtree(d_full)
                            else:
                                self.fs.unlink(d_full)
                        d = self.fs.readdir(dir_handle)
            except cephfs.ObjectNotFound:
                return
            except cephfs.Error as e:
                raise VolumeException(-e.args[0], e.args[1])
            # remove the directory only if we were not asked to cancel
            # (else we would fail to remove this anyway)
            if not should_cancel():
                self.fs.rmdir(root_path)

        # catch any unlink errors
        try:
            rmtree(trashpath)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def dump(self, path):
        """
        move an filesystem entity to trash can.

        :praram path: the filesystem path to be moved
        :return: None
        """
        try:
            self.fs.rename(path, self.unique_trash_path)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def link(self, path, bname):
        pth = os.path.join(self.path, bname)
        try:
            self.fs.symlink(path, pth)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def delink(self, bname):
        pth = os.path.join(self.path, bname)
        try:
            self.fs.unlink(pth)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def _get_stats(self, path, num_of_files, num_of_subvols, iter_level):
        with self.fs.opendir(path) as dir_handle:
            d = self.fs.readdir(dir_handle)
            while d:
                if d.d_name not in (b'.', b'..'):
                    if d.is_dir():
                        d_full = os.path.join(path, d.d_name)
                        iter_level += 1
                        num_of_files, num_of_subvols = self._get_stats(d_full,
                            num_of_files, num_of_subvols, iter_level)
                        iter_level -= 1

                    num_of_files += 1
                    if iter_level == 0:
                        num_of_subvols += 1

                try:
                    d = self.fs.readdir(dir_handle)
                # this try-except is inside the loop so that looping can
                # "continue" in ObjectNotFound
                except cephfs.ObjectNotFound as e:
                    log.debug(f'Exception "{e}" occurred, perhaps the file '
                              'purged by purge threads that are running '
                              'simultaneously')
                    continue

        return num_of_files, num_of_subvols

    def get_stats(self):
        try:
            num_of_files, num_of_subvols = self._get_stats(self.path, 0, 0, 0)
        # this try-except ensure separate handling when trash dir goes
        # missing
        except cephfs.ObjectNotFound as e:
            log.debug(f'Exception "{e}" ocurred.')
            return
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        if num_of_files:
            return {'subvols_left': num_of_subvols, 'files_left': num_of_files}
        else:
            return {}

def create_trashcan(fs, vol_spec):
    """
    create a trash can.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :return: None
    """
    trashcan = Trash(fs, vol_spec)
    try:
        fs.mkdirs(trashcan.path, 0o700)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

@contextmanager
def open_trashcan(fs, vol_spec):
    """
    open a trash can. This API is to be used as a context manager.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :return: yields a trash can object (subclass of GroupTemplate)
    """
    trashcan = Trash(fs, vol_spec)
    try:
        fs.stat(trashcan.path)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    yield trashcan
