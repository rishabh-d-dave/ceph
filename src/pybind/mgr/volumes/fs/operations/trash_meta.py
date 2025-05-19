'''
Meta file for trashcan to track the statistics required for reporting the
progress made by purge threads.

The meta file contains 3 keys: total_subvols (total number of subvolumes moved
to trash directory), total_files (total number of files moved to the trash
directory) and total_size (size of data that has been moved to trash directory).

Following are some notes -

1. Number of files in the subvolume and size of data in the subvolume is
   collected just before the subvolume is trashed. These statistics are then
   used to figure out the number of subvolumes, number of files and size of the
   data moved to trash.

2. Reason behind saving these statistics on disk is to not lose them in case
   the MGR crashes. If these are lost, it becomes impossible to report the
   progress made by purge threads (via purge status command's output as well as
   via progress bar for purge threads) since there is no record of how much
   data was moved to trash directory.

3. This meta file is not located in the trash directory (/volumes/_deleting)
   because purge threads pick and purge all files placed in there.

4. This code could've been placed in trash.py but that is not possible since
   importing MetadataManager in it will lead to circular dependency. Thus
   placing this code separately.
'''
import errno
from os.path import join
from contextlib import contextmanager
from logging import getLogger

from .versions.metadata_manager import MetadataManager, MetadataMgrException


log = getLogger(__name__)


class TrashMeta(MetadataManager):

    def __init__(self, fs, volspec):
        self.fs = fs
        self.volspec = volspec

        super().__init__(self.fs, self.path, 0o640)
        self.SECTION_NAME = 'trash stats'

    @property
    def path(self):
        return join(self.volspec.base_dir, '.meta-for-trash').encode('utf-8')

    def add_default_section(self):
        return self.add_section(self.SECTION_NAME)

    def created(self):
        try:
            self.refresh()
            return True
        except MetadataMgrException as e:
            if e.errno == -errno.ENOENT:
                # in case the trash meta file doesn't eixst, return False
                return False
            else:
                raise

    def has_default_section(self):
        return self.config.has_section(self.SECTION_NAME)

    def get_stats(self):
        self.refresh()

        total_subvols = int(self.get_option(self.SECTION_NAME, 'total_subvols'))
        total_files = int(self.get_option(self.SECTION_NAME, 'total_files'))
        total_size = int(self.get_option(self.SECTION_NAME, 'total_size'))

        return total_subvols, total_files, total_size

    def write_metadata(self, total_subvols, total_files, total_size):
        self.update_section(self.SECTION_NAME, 'total_subvols', total_subvols)
        self.update_section(self.SECTION_NAME, 'total_files', total_files)
        self.update_section(self.SECTION_NAME, 'total_size', total_size)

        self.flush()

    def clear(self):
        self.remove_section(self.SECTION_NAME)

        self.flush()


@contextmanager
def open_trashcan_meta(fs, volspec):
    yield TrashMeta(fs, volspec)


def get_trashcan_stats(fs, volspec):
    with open_trashcan_meta(fs, volspec) as trashcan_meta:
        return trashcan_meta.get_stats()


def update_trashcan_meta(fs, volspec, NUM_OF_SUBVOLS, NUM_OF_SUBVOL_FILES,
                         SUBVOL_SIZE):
    with open_trashcan_meta(fs, volspec) as trashcan_meta:
        if trashcan_meta.created():
            assert trashcan_meta.has_default_section()

            prev_total_subvols, prev_total_files, prev_total_size = \
                trashcan_meta.get_stats()

            total_trashed_subvols = prev_total_subvols + NUM_OF_SUBVOLS
            total_trashed_files = prev_total_files + NUM_OF_SUBVOL_FILES
            total_trash_size = prev_total_size + SUBVOL_SIZE

            log.debug('trash meta file already existed along with section '
                      f'"{trashcan_meta.SECTION_NAME}", updating it with '
                       'following values - '
                      f'total_trashed_subvols = {total_trashed_subvols} '
                      f'total_trashed_files = {total_trashed_files} '
                      f'total_trash_size = {total_trash_size}')
            trashcan_meta.write_metadata(total_trashed_subvols,
                                         total_trashed_files, total_trash_size)
        else:
            assert not trashcan_meta.has_default_section()

            log.debug('trash meta file does not exist, adding section '
                      '"{trashcan.SECTION_NAME}" and writing the following '
                      'values to it - '
                      f'NUM_OF_SUBVOLS = {NUM_OF_SUBVOLS} '
                      f'NUM_OF_SUBVOL_FILES = {NUM_OF_SUBVOL_FILES} '
                      f'SUBVOL_SIZE = {SUBVOL_SIZE}')
            trashcan_meta.add_default_section()
            trashcan_meta.write_metadata(NUM_OF_SUBVOLS, NUM_OF_SUBVOL_FILES,
                                         SUBVOL_SIZE)


def clear_trashcan_meta(fs, volspec):
    with open_trashcan_meta(fs, volspec) as trashcan_meta:
        trashcan_meta.clear()
