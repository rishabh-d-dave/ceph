from errno import *
from os.path import basename, dirname
from logging import getLogger
from typing import Final

from cephfs import Error, InvalidValue, ObjectNotFound

from .subvolume_v2 import SubvolumeV2
from .subvolume_attrs import SubvolumeStates
from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from ..trash import create_trashcan, open_trashcan
from ...utils import gen_uuid, verify_uuid, safe_join, to_utf8, to_str
from ...fs_util import (listdir, path_exists, listsnaps, statx, is_dir_empty,
                        get_all_xattrs, set_all_xattrs)
from ...exception import (VolumeException, MetadataMgrException,
                          V2UpgradeError)


log = getLogger(__name__)


class PreV3Helper:
    '''
    Attritbutes/methods that makes SubvolumeV3 code compatible with SubvolumeV2,
    SubvolumeV1 and SubvolumeBase.
    '''

    @property
    def vol_spec(self):
        return self.spec

    @property
    def subvolname(self):
        return self.name

    @property
    def metadata_mgr(self):
        return self.md

    @property
    def auth_mdata_mgr(self):
        return self.auth_md

    @property
    def config_path(self):
        return self.meta_path

    @property
    def retained(self):
        return self.is_retained()

    @property
    def trash_dir(self):
        raise RuntimeError('method trash_dir() shouldn\'t be called in '
                           'subvol v3 codebase, since it doesn\'t have a '
                           'in-subvol trash dir (which is named ".trash" in'
                           'subvol v2)')

    def create_trashcan(self):
        raise RuntimeError('method create_trashcan() shouldn\'t be called in '
                           'subvol v3 codebase, since it doesn\'t have a '
                           'in-subvol trash dir (which is named ".trash" in'
                           'subvol v2)')

    def snapshot_base_path(self):
        return self.get_incar_snap_base_path()

    # simply returns snap path, regardless of whether it exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_path(self, snap_name):
        return self.get_incar_snap_path(snap_name)

    # returns snap path if the snap exists.
    #
    # unfortunately, v1/v2 has this strange naming which doesn't tell enough.
    # adhering with it for now to avoid unrelated group of changes to codebase
    # in this patch series.
    def snapshot_data_path(self, snap_name):
        if snap_path := self.get_snap_path(snap_name):
            return snap_path

        # TODO
        # v2 raises exception if the snapshot path do not exist so do the same
        # to prevent any bugs due to difference in behaviour.
        #
        # not raising exception indeed leads to a bug: the volumes plugin fails
        # when exception is not raised by this method when it is calld by
        # do_clone() method of async_cloner.py. this is made to happen by a test
        # by deleting snapshot after running the snapshot clone cmd but before
        # the clone operation actually begins. this is done by a adding a delay
        # using mgr/volumes/snapshot_clone_delay config option.
        raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

    def list_snapshots(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        return self.get_snap_names()


class SubvolHelper:
    '''
    Convenient helpers for fs_util.py functions.
    '''

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def path_exists(self, path, follow_symlink=True):
        return path_exists(self.fs, path)

    def list_snaps(self, path):
        return listsnaps(self.fs, self.spec, path)

    def statx(self, path, fields=None):
        return statx(self.fs, path, fields)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def get_all_xattrs(self, path):
        return get_all_xattrs(self.fs, path)

    def set_all_xattrs(self, path, xattrs):
        return set_all_xattrs(self.fs, path, xattrs)


class V2Helper(SubvolHelper, PreV3Helper):
    '''
    For helping SubvoolV3 with its v2 incarnation
    '''

    def __init__(self, fs=None, spec=None, base_path=None):
        self.fs = fs
        self.spec = spec
        self.base_path = base_path

        self.uuid: Final = self.fetch_uuid()
        if self.uuid is None:
            self.meta_path: Final = None
            self.uuid_path: Final = None
            self.snap_base_path: Final = None
            return

        verify_uuid(self.uuid)
        self.meta_path: Final = safe_join(self.base_path, '.meta')
        self.uuid_path: Final = safe_join(self.base_path, self.uuid)
        self.snap_base_path: Final = safe_join(self.uuid_path, self.spec.snap_base_dir)

    def fetch_uuid(self):
        # v2 incar will not exist if (v3) subvol is being created
        if not self.path_exists(self.base_path):
            return

        dentries = self.list_dirs(self.base_path)
        if to_utf8('roots') in dentries:
            dentries.remove(to_utf8('roots'))

        if dentries:
            verify_uuid(dentries[0])
            return dentries[0]

    def get_snap_names(self):
        if not self.uuid:
            return []

        return self.list_snaps(self.snap_base_path)

    def get_snap_path(self, snap_name):
        if not self.uuid:
            return None

        if snap_name in self.get_snap_names():
            return safe_join(self.snap_base_path, snap_name)

    def has_snap(self, snap_name=None, uuid=None):
        if not self.uuid:
            return False

        if uuid and uuid != self.uuid:
            return False

        if snap_name:
            return snap_name in self.get_snap_names()
        else:
            if not self.path_exists(self.snap_base_path):
                return False
            return not self.is_dir_empty(self.snap_base_path)


class SubvolumeV3(SubvolHelper, PreV3Helper, SubvolumeV2):
    '''
    Code for v3 subvolume.

    /volumes/_nogroup/subvol123/roots/<UUIDs>/mnt
                                                ^ self.get_incar_mnt_path()
                                         ^ self.get_incar_uuid_path()
                                 ^ self.roots_path
                         ^ self.base_path

    /volumes/_nogroup/subvol123/.meta
                                 ^ self.meta_symlink_path, points to current
                                   incar's meta

    /volumes/_nogroup/subvol123/.meta.<UUID>
                                  ^ self.meta_path

    /volumes/_nogroup/subvol123/roots/<UUIDs>/.snap/snap123
                                                      ^ self.get_incar_snap_path()
                                                ^ self.get_incar_snap_base_path()

    /volumes/_nogroup/subvol123/<UUID>/.snap/snap123
                                              ^ self.v2.get_snap_path()
                                        ^ self.v2.snap_base_path
    '''

    @staticmethod
    def version():
        # this way the chance of accidentally modifying the version number is
        # literally zero.
        return 3


    # ----- init and its helpers methods -----


    def __init__(self, mgr, fs, spec, group, name, uuid=None,
                 disc_version=None):
        self.mgr = mgr
        self.fs = fs
        self.spec = spec
        self.group = group

        self.name = name
        self.uuid = uuid
        if self.uuid:
            verify_uuid(self.uuid)

        self.has_incar = True

        # TODO: check if we can do without self.creating without too much
        # hassle?
        self.creating = (disc_version is None)

        if disc_version is None:
            pass
        elif disc_version == self.version():
            assert self.uuid
            self._init_basic_paths()
            self._init_md_attrs()
            self._init_v2_helper()
        elif disc_version < self.version():
            self._auto_upgrade(disc_version)
        else:
            msg = f'disc_version = {disc_version}'
            log.error(msg)
            assert False, msg

    def _init_basic_paths(self):
        self.base_path: Final = safe_join(self.spec.subvol_base_path,
                                          self.group.name, self.name)

        self.meta_symlink_path: Final = safe_join(self.base_path, '.meta')
        self.roots_path: Final = safe_join(self.base_path, 'roots')

        self.meta_file_name: Final = to_utf8(f'.meta.{to_str(self.uuid)}')
        self.meta_path: Final = safe_join(self.base_path, self.meta_file_name)

    def _init_md_attrs(self):
        log.debug(f'for subvol {self.name} loading meta {self.meta_path}')
        self.md = MetadataManager(self.fs, self.meta_path, 0o640)
        log.debug(f'meta of subvol {self.name}, self.md = {self.md}')
        self.auth_md = AuthMetadataManager(self.fs)


        if not self.creating:
            # can be removed?
            self.md.refresh()

    def _init_v2_helper(self):
        self.v2 = V2Helper(self.fs, self.spec, self.base_path)


    # ----- basic helper methods for v3 incars -----


    # incar = incarnation
    def get_incar_path(self, uuid=None):
        if self.v2.UUID and self.v2.UUID == uuid:
            return self.v2.uuid_path

        uuid = uuid if uuid else self.uuid
        return safe_join(self.roots_path, uuid)

    def get_incar_mnt_path(self, uuid=None):
        if self.v2.UUID and self.v2.UUID == uuid:
            assert False, 'v2 incar can\'t have mnt dir/path'

        return safe_join(self.get_incar_path(uuid), 'mnt')

    # deact = deactivated
    def get_incar_deact_path(self, uuid=None):
        if self.v2.UUID and self.v2.UUID == uuid:
            assert False, 'v2 incar can\'t have deactivate dir/path'

        return safe_join(self.get_incar_path(uuid), '.deactivated')

    def get_incar_snap_base_path(self, uuid=None):
        if self.v2.UUID and self.v2.UUID == uuid:
            return self.v2.snap_base_path

        return safe_join(self.get_incar_path(uuid), self.spec.snap_base_dir)

    def get_incar_snap_path(self, snap_name, uuid=None):
        if self.v2.UUID and self.v2.UUID == uuid:
            return self.v2.get_snap_path(snap_name)

        return safe_join(self.get_incar_snap_base_path(uuid), snap_name)

    def get_v3_incars(self):
        return self.list_dirs(self.roots_path)


    # ----- methods for subvol creation, opening and removal -----


    def is_retained(self):
        if self.creating:
            return False
        else:
            return SubvolumeV2.retained.fget(self)

    def _remove_on_failure(self, retained):
        if not retained:
            log.info(f'removing subvol: {self.name}')
            self.remove(internal_cleanup=True)

        try:
            # current incarnation path
            curr_incar_path = self.get_incar_path()
            log.info(f'removing subvol incar path: {to_str(curr_incar_path)}')
            self.fs.rmdir(curr_incar_path)
        except Error as e:
            raise VolumeException(e)

    @property
    def has_pending_purges(self):
        # since v3 does not have in-subvol ".trash" dir, this method must always
        # return False.
        return False

    def mark_subvolume(self):
        '''
        Set vxattr ceph.dir.subvolume to 1 on the incar/UUID path.
        '''
        # subvolume vxattr can't be set since there is no incar ATM.
        if not self.path_exists(self.get_incar_path()):
            return

        xattr = 'ceph.dir.subvolume'
        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.get_incar_path(), xattr, '1', 0)
        except InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for "{xattr}"')
        except Error as e:
            raise VolumeException(e)

    def set_meta_symlink(self):
        if not self.path_exists(self.meta_path):
            assert False, \
                (f'meta file for current incar is missing, it is to be created '
                 f'first. self.meta_path = {self.meta_path}')

        assert self.uuid in self.meta_file_name, \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.uuid = {self.uuid}')

        assert self.meta_file_name == basename(self.meta_path), \
            (f'self.meta_file_name = {self.meta_file_name} '
             f'self.meta_path = {self.meta_path}')

        if self.path_exists(self.meta_symlink_path, follow_symlink=False):
            self.fs.unlink(self.meta_symlink_path)
        self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])

    def create_or_update_meta_file(self, subvol_type):
        SubvolumeV2.create_or_update_meta_file(self, self.get_incar_mnt_path(),
                                               subvol_type)

        self.set_meta_symlink()

    def _create(self, mode, attrs, subvol_type, auth=True):
        assert self.creating
        assert self.uuid is None
        self.uuid = gen_uuid()
        self._init_basic_paths()
        self._init_md_attrs()
        self._init_v2_helper()

        if not self.path_exists(self.group.path):
            self.fs.mkdirs(self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.get_incar_mnt_path(), mode)

        self.mark_subvolume()
        self.set_attrs(self.get_incar_mnt_path(), attrs)

        self.create_or_update_meta_file(subvol_type)
        self._init_md_attrs()
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name,
                                                        self.name)

    def trash_base_dir(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            if len(self.get_v3_incars()) > 1:
                trashcan.dump(self.get_incar_path())
            else:
                trashcan.dump(self.base_path)

    def trash_uuid_dir(self, uuid):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.get_incar_path(uuid))


    # ----- helper methods for snap code -----


    def get_v3_snap_names(self, uuid=None):
        if uuid:
            path = self.get_incar_snap_base_path(uuid)
            return self.list_snaps(path)
        else:
            snap_names = []
            for uuid in self.get_v3_incars():
                path = self.get_incar_snap_base_path(uuid)
                snap_names += self.list_snaps(path)
            return snap_names

    def get_snap_names(self, uuid=None):
        if uuid:
            if uuid in self.get_v3_incars():
                return self.get_v3_snap_names(uuid)
            elif self.v2.UUID == uuid:
                return self.v2.get_snap_names()
        else:
            snap_names = []
            snap_names += self.v2.get_snap_names()
            snap_names += self.get_v3_snap_names()
            return snap_names

    # Listing all snaps can be expensive due to possibility of multiple snaps in
    # multiple incarnations. This methods returns as soon as it finds the given
    # snap instead of listing all snaps and then finding the given snap in the list.
    # So, use this method, instead, for finding a snap.
    def has_v3_snap(self, snap_name=None, uuid=None):
        if snap_name and uuid:
            path = self.get_incar_snap_path(snap_name, uuid)
            return self.path_exists(path)
        elif snap_name and not uuid:
            uuid = self.get_v3_incar_for_snap_name(snap_name)
            path = self.get_incar_snap_path(snap_name, uuid)
            return self.path_exists(path)
        elif not snap_name and uuid:
            path = self.get_incar_snap_base_path(uuid)
            return not self.is_dir_empty(path)
        elif not snap_name and not uuid:
            for uuid in self.get_v3_incars():
                path = self.get_incar_snap_base_path(uuid)
                return not self.is_dir_empty(path)
        else:
            return False

    def has_snap(self, snap_name=None, uuid=None):
        if self.v2.has_snap(snap_name, uuid):
            return True
        elif self.has_v3_snap(snap_name, uuid):
            return True
        else:
            return False

    def get_v3_incar_for_snap_name(self, snap_name):
        for uuid in self.get_v3_incars():
            path = self.get_incar_snap_base_path(uuid)
            if snap_name in self.list_snaps(path):
                return uuid
        return None


    # ----- methods for snaps -----


    def get_snap_path(self, snap_name):
        '''
        Gets snap path regardless of where it's present, v3 or v2.
        '''
        snap_name = to_utf8(snap_name)

        if snap_path := self.v2.get_snap_path(snap_name):
            pass
        elif uuid := self.get_v3_incar_for_snap_name(snap_name):
            snap_path = self.get_incar_snap_path(snap_name, uuid)
        # none of the incarnations had a snap with this name.
        else:
            return None

        return snap_path if self.path_exists(snap_path) else None

    def create_snapshot(self, snap_name):
        snap_name = to_utf8(snap_name)

        if self.has_snap(snap_name):
            raise VolumeException(EEXIST, f'snap "{snap_name}" already exists')

        SubvolumeV2.create_snapshot(self, snap_name)

    # TOOD,v3: remove this method? subvol snap rm cmd needs to tested before
    # that.
    @property
    def purgeable(self):
        return False if not self.retained or self.list_snapshots() else True

    def stat_subvol_data_path(self):
        '''
        stat() path where data of subvol users is stored.

        This method overrides the v2 method, it must raise
        cephfs.ObjectNotFound if path(s) don't exists.
        '''
        try:
            self.fs.stat(self.get_incar_mnt_path())
        except ObjectNotFound:
            # when subvol is present but it has no incars
            self.fs.stat(self.base_path)

    def remove_snapshot(self, snap_name, force):
        snap_name = to_utf8(snap_name)

        # UUID can be none if snap is absent but don't raise any exception in
        # this case since command's behaviour is expected to be idempotent.
        if not (snap_path := self.get_snap_path(snap_name)):
            raise VolumeException(ENOENT, f'snap "{snap_name}" does not exist')

        SubvolumeV2.remove_snapshot(self, snap_name, force)
        if self.is_retained():
            if not self.has_snap():
                self.trash_base_dir()
                raise VolumeException(ESTALE, 'release lock and queue async '
                                              'purge job')
        else:
            uuid = basename(dirname(dirname(snap_path)))
            if not self.v2.has_snap(uuid):
                if self.has_v3_snap():
                    self.trash_uuid_dir(uuid)
                    self.md.remove_global_option('has_v2_snaps')
                    self.md.flush()
                else:
                    self.trash_base_dir(uuid)
                self._init_v2_helper()
                raise VolumeException(ESTALE, 'release lock and queue async '
                                              'purge job')
            elif not self.has_v3_snap(uuid):
                if self.has_v3_snap():
                    self.trash_uuid_dir(uuid)
                else:
                    self.trash_base_dir(uuid)
                raise VolumeException(ESTALE, 'release lock and queue async '
                                              'purge job')
        return False


    # ----- methods for subvol removal while retaining snaps -----


    def deact_curr_incar(self):
        self.fs.rename(self.get_incar_mnt_path(), self.get_incar_deac_path())

    def update_meta_file_after_retain(self):
        self.md.remove_section(self.md.USER_METADATA_SECTION)

        self.md.update_global_section('key', self.get_incar_deact_path())
        self.md.update_global_section('state',
                                      SubvolumeStates.STATE_RETAINED.value)

        self.md.flush()

    def remove_but_retain_snaps(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.deact_curr_incar()
            self.auth_md.delete_subvolume_metadata_file(self.group.name,
                                                        self.name)
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(e)


    # ----- methods for subvol auto-upgrade -----


    def _auto_upgrade(self, disc_version):
        if disc_version == 2:
            self._upgrade_from_v2()
        else:
            assert False, \
                f'disc_version = {disc_version}'

    def set_prev_path(self, prev_path):
        prev_path = to_str(prev_path)
        # caps will have '/' prefixing path, so ensure prev_path also has it.
        if prev_path[0] != '/':
            prev_path = '/' + prev_path

        # XXX: needs to be done as **early** as possible to keep the possibility
        # of client IO interruption minimum, keeping subvol upgrade seamless.
        #
        # XXX: applying prevpath vxattr on v2 UUID path as it would be renamed
        # renamed to mnt path. setting it after renaming v2 UUID path to mnt
        # path create a minute period between them during which MDS will reject
        # client requests, causing client IO interruption.
        self.fs.setxattr(prev_path, 'ceph.dir.subvolume.prevpath', prev_path)


    # ----- methods for subvol auto-upgrade from v2 -----


    def _upgrade_v2_to_v3_layout(self):
        log.info(f'upgrading to v3 layout for subvol {self.name}...')

        try:
            uid, gid, mode = self.statx(self.v2.uuid_path, ('uid', 'gid',
                                                            'mode'))
            sv_xattrs = self.get_all_xattrs(self.v2.uuid_path)

            self.fs.mkdirs(self.get_incar_path(), 0o755)
            v3_mnt_path = self.get_incar_mnt_path()
            if self.v2.has_snap():
                self.fs.mkdir(v3_mnt_path, mode)
            else:
                self.fs.rename(self.v2.uuid_path, v3_mnt_path)
                self.fs.chmod(v3_mnt_path, mode)

            self.fs.chown(v3_mnt_path, uid, gid)
            if sv_xattrs:
                self.set_all_xattrs(v3_mnt_path, sv_xattrs)

            self.fs.rename(self.v2.meta_path, self.meta_path)
            self.fs.symlink(self.meta_file_name, self.meta_symlink_path[1:])

            for path in (self.meta_path, self.meta_symlink_path):
                self.fs.chown(path, 0, 0)
                self.fs.chmod(path, 644)
        except Error as e:
            raise V2UpgradeError(e)

        log.info(f'layout upgrade for subvol {self.name} was successful')

    def _update_v2_to_v3_meta(self):
        log.info(f'updating meta file of subvol {self.name}...')

        try:
            self.md.refresh()

            self.md.update_global_section('version', self.version())
            self.md.update_global_section('path', self.get_incar_mnt_path())
            if self.v2.has_snap():
                self.md.update_global_section('has_v2_snaps', True)

            self.md.flush()
        except MetadataMgrException as e:
            raise V2UpgradeError(e)

        log.info(f'meta file update for subvol {self.name} was successful')

    def _upgrade_from_v2(self):
        log.info(f'initiating v2 to v3 auto-upgrade for subvol {self.name}')

        self._init_basic_paths()
        self._init_v2_helper()
        assert self.v2.UUID

        self.set_prev_path(self.v2.uuid_path)
        if self.v2.has_snap():
            self.uuid = gen_uuid()
            self._init_basic_paths()

        self._upgrade_v2_to_v3_layout()
        self._init_md_attrs()
        self._update_v2_to_v3_meta()

        # re-define v2 helper if v2 incar is moved.
        if self.uuid == self.v2.UUID:
            self._init_v2_helper()
        self.clean_stale_snapshot_metadata()

        log.info(f'v2 to v3 auto-upgrade for subvol {self.name} was successful')
