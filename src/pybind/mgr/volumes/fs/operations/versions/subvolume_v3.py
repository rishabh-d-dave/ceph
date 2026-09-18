from errno import *
from os.path import basename
from logging import getLogger
from uuid import uuid4
from functools import wraps

import cephfs

from .metadata_manager import MetadataManager
from .auth_metadata import AuthMetadataManager
from .subvolume_attrs import SubvolumeStates
from .subvolume_v2 import SubvolumeV2
from ..trash import create_trashcan, open_trashcan
from ...utils import ensure_uuid_is_valid, safe_join, to_bytes
from ...fs_util import (listdir, create_base_dir, listsnaps, path_exists,
                        is_dir_empty)
from ...exception import VolumeException, MetadataMgrException


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
    def base_path(self):
        return self.subvol_path

    def snapshot_path(self, snap_name):
        '''
        Path to a specific snapshot named 'snap_name'.
        '''
        return self.get_incar_snap_path(snap_name)

    def snapshot_base_path(self):
        return self.snap_base_path

    def list_snapshots(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        return self.get_all_snap_names()

    def snapshot_data_path(self, snap_name):
        return self.get_incar_snap_path(snap_name)


def has_snaps(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.has_snapshots():
            return func(self, *args, **kwargs)
    return wrapper


class SubvolHelper:
    '''
    Basic stuff for subvolumes
    '''

    def list_dirs(self, path):
        return listdir(self.fs, path)

    def is_dir_empty(self, path):
        return is_dir_empty(self.fs, path)

    def list_snaps(self, path):
        return listsnaps(self.fs, self.spec, path)


class V2Helper(SubvolHelper, PreV3Helper):
    '''
    For helping SubvoolV3 with its v2 incarnation
    '''

    def __init__(self, fs, spec, subvol_path, md):
        self.fs = fs
        self.spec = spec
        self.subvol_path =  subvol_path
        self.md = md

        self._init_cache()

    def _init_cache(self):
        self._uuid = None
        self._has_snaps = None
        self._snap_base_path = None

    def has_snapshots(self):
        if self._has_snaps in (True, False):
            return self._has_snaps

        assert self._has_snaps is None
        self._has_snaps = bool(self.md.get_global_option('has_v2_snaps'))
        assert self._has_snaps in (True, False)
        return self._has_snaps

    @property
    def snap_base_path(self):
        if self._snap_base_path is None:
            self._snap_base_path = safe_join(self.subvol_path, self.uuid,
                                              self.spec.snap_base_dir)
        else:
            assert isinstance(self._snap_base_path, str)
        return self._snap_base_path

    def fetch_uuid(self):
        dentries = self.list_dirs(self.subvol_path)
        dentries.remove(b'roots')
        assert len(dentries) == 1
        self.ensure_uuid_is_valid(dentries[0])
        return dentries[0]

    @property
    def uuid(self):
        if not self._uuid:
            self.uuid = self._fetch_uuid()
        return self._uuid

    @has_snaps
    def get_snap_path(self, snap_name):
        return safe_join(self.snap_base_path, snap_name)

    @has_snaps
    def get_snap_names(self):
        '''
        :returns: list of v2 snap names
        :rtype: list of str
        '''
        return self.list_snaps(self.snap_base_path)


# TODO: revise this comment to reflect latest updates once layout is approved
# on the PR.
class SubvolumeV3(SubvolHelper, PreV3Helper, SubvolumeV2):
    '''
    Code for v3 subvolume.

    /volumes/_nogroup/subvol123/roots/<UUIDs>/mnt
                                                ^ get_incar_mnt_path()
                                         ^ get_incar_uuid_path()
                                 ^ roots_path
                         ^ subvol_path

    /volumes/_nogroup/subvol123/roots/<UUIDs>/.snap/snap123
                                                      ^ get_v3_snap_path()
                                                ^ get_incar_snap_base_path()

    /volumes/_nogroup/subvol123/.meta
                                 ^ meta slink path

    /volumes/_nogroup/subvol123/.meta.<UUID>
                                  ^ meta path

    /volumes/_nogroup/subvol123/<UUID>/.snap/snap123
                                              ^ v2.get_snap_path()
                                        ^ v2.snap_base_path
    '''

    _VERSION = 3

    def __init__(self, mgr, fs, spec, group, name, uuid=None):
        self.mgr = mgr
        self.group = group
        self.fs = fs
        self.spec = spec

        self.name = name
        self.uuid = uuid
        if self.uuid:
            ensure_uuid_is_valid(self.uuid)
        else:
            self.uuid = str(uuid4())

        self._define_std_paths()

        log.debug(f'loading meta {self.meta_path}')
        self.md = MetadataManager(self.fs, self.meta_path, 0o640)
        self.auth_md = AuthMetadataManager(self.fs)

        self.v2 = V2Helper(self.fs, self.spec, self.subvol_path, self.md)

    def _define_std_paths(self):
        self.subvol_path = safe_join(self.spec.subvol_base_path,
                                     self.group.name, self.name)

        self.meta_slink_path = safe_join(self.subvol_path, '.meta')
        self.meta_file_name = f'.meta.{self.uuid}'.encode('utf-8')
        self.meta_path = safe_join(self.subvol_path, self.meta_file_name)

        self.roots_path = safe_join(self.subvol_path, 'roots')
        self.uuid_path = safe_join(self.roots_path, self.uuid)
        self.mnt_path = safe_join(self.uuid_path, 'mnt')
        self.unlinked_path = safe_join(self.uuid_path, '.unlinked')
        self.snap_base_path = safe_join(self.uuid_path, self.spec.snap_base_dir)


    # ----- basic v3 stuff -----


    def get_v3_snap_path(self, snap_name):
        return safe_join(self.snap_base_path, snap_name)

    def get_incar_uuids(self):
        return self.list_dirs(self.roots_path)

    def get_incar_uuid_path(self, uuid):
        return safe_join(self.roots_path, uuid)

    def get_incar_mnt_path(self, uuid):
        return safe_join(self.get_incar_uuid_path(uuid), 'mnt')

    def get_incar_unlinked_path(self, uuid):
        return safe_join(self.get_incar_uuid_path(uuid), '.unlinked')

    def get_incar_snap_base_path(self, uuid):
        return safe_join(self.get_incar_uuid_path(uuid), self.spec.snap_base_dir)

    def get_v3_incar_snap_path(self, snap_name, uuid):
        return safe_join(self.get_incar_snap_base_path(uuid), snap_name)


    # ----- basic stuff for a subvol -----


    @staticmethod
    def version():
        return SubvolumeV3._VERSION


    # ----- methods that helps subvol creation and opening and discovery -----


    def set_subvol_xattr(self):
        subvol_xattr = 'ceph.dir.subvolume'

        try:
            # MDS treats this as a no-op for already marked subvolume
            self.fs.setxattr(self.uuid_path, subvol_xattr, b'1', 0)
        except cephfs.InvalidValue:
            raise VolumeException(EINVAL, f'invalid value for {subvol_xattr}')
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def _create_v3_layout(self, mode):
        create_base_dir(self.fs, self.group.path, self.spec.DEFAULT_MODE)
        self.fs.mkdirs(self.mnt_dir, mode)

    def set_meta_for_curr_incar(self):
        assert self.uuid == basename(self.meta_path).replace('.meta.', '')
        self.fs.unlink(self.meta_slink_path)

        self.fs.symlink(self.meta_path, self.meta_slink_path[1:])

    def create_or_update_meta_file(self, subvol_type):
        super(SubvolumeV3, self).create_or_update_meta_file(subvol_type)

        self.set_meta_for_curr_incar()

    def _create(self, mode, attrs, subvol_type, auth=True):
        self._create_v3_layout(mode)

        self.set_subvol_xattr()
        self.set_attrs(self.mnt_dir, attrs)

        self.create_or_update_meta_file(subvol_type)
        if auth:
            # Create the subvolume metadata file which manages auth-ids if it
            # doesn't exist
            self.auth_md.create_subvolume_metadata_file(self.group.name, self.name)


    # ----- methods that for subvol management -----


    # TODO: base dir should be deleted in subvol v3 too when no snaps are
    # retained on any incarnation, right?
    def trash_base_dir(self):
        # code under _trash_subvol_path can be move here technically but this
        # extra layer of call has been added to indicate that in subvol v3
        # terms
        self.trash_subvol_path()

    # since there is not in-subvol ".trash" dir in subvol v3, this method
    # should always return False
    @property
    def has_pending_purges(self):
        return False

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

    def trash_subvol_path(self):
        create_trashcan(self.fs, self.spec)

        with open_trashcan(self.fs, self.spec) as trashcan:
            trashcan.dump(self.subvol_path)

    # in subvol v3, self.mnt_dir (AKA data dir) is renamed to ".unlinked" if
    # subvol is deleted but snapshots are retained.
    def trash_incarnation_dir(self):
        self.fs.rename(self.mnt_dir, self.unlinked_dir)

    def update_meta_file_after_retain(self):
        self.md.remove_section(MetadataManager.USER_METADATA_SECTION)
        self.md.update_section(MetadataManager.GLOBAL_SECTION,
                                         MetadataManager.GLOBAL_META_KEY_PATH,
                                         self.unlinked_dir.decode('utf-8'))
        self.md.update_global_section(
            MetadataManager.GLOBAL_META_KEY_STATE,
            SubvolumeStates.STATE_RETAINED.value)
        self.md.flush()


    # ----- methods that help snap creation -----


    # XXX: self.uuid can be none if snap is absent but don't raise any exception
    # in this case since command's behaviour is expected to be idempotent.
    def get_snap_path(self, snap_name, uuid=None, check_exists=True,
                      should_raise=True):
        '''
        Gets snap path regardless of where it'spresent, v3 incars or v2 or v1.
        '''
        snap_path = self.get_v3_incar_snap_path(snap_name, uuid=uuid)
        if not snap_path:
            snap_path = self.v2.get_snap_path(snap_name)

        if check_exists:
            if self.path_exists(snap_path):
                return snap_path
            else:
                if should_raise:
                    # v2 raises exception if the snapshot path do not exist so do the
                    # same to prevent any bugs due to difference in behaviour.
                    #
                    # not raising exception indeed leads to a bug: the volumes plugin
                    # fails when exception is not raised by this method when it is
                    # called by do_clone() method of async_cloner.py. this is made to
                    # happen by a test by deleting snapshot after running the snapshot
                    # clone command but before the clone operation actually begins. this
                    # is done by adding a delay using mgr/volumes/snapshot_clone_delay
                    # config option.
                    raise VolumeException(-ENOENT,
                                          f'snapshot {snap_name} does not exist')
                else:
                    return None

    def has_snaps(self, uuid=None):
        '''
        Avoid O(n^2) comlexity by prefer this method over list_snapshosts() ot
        get_snap_names(). v3 can have many incarnations and it's very
        ineffecient and redundant to use methods since getting all/listing all
        snap names in every incarnation in this particular case.
        '''
        if uuid:
            path = self.get_incar_snap_base_path(uuid)
            return True if not self.is_dir_empty(path) else False

        for uuid in self.get_incar_uuids():
            path = self.get_incar_snap_base_path(uuid)
            if not self.is_dir_empty(path):
                return True
        return False

    def get_snap_names(self, uuid=None):
        path = self.get_incar_snap_base_path(uuid)
        return self.list_dirs(path)

    def get_v3_uuid_for_snap(self, snap_name):
        '''
        :returns: UUID of incarnation in which the snap is found.
        :rtype: str or None
        '''
        for uuid in self.get_incar_uuids():
            if snap_name in self.get_snap_names(uuid):
                return uuid

    def get_uuid_for_snap(self, snap_name):
        '''
        :returns: UUID of incarnation in which the snap is found.
        :rtype: str or None
        '''
        uuid = self.get_v3_uuid_for_snap(snap_name)
        if uuid is None:
            if self.v2.snap_exists(snap_name):
                uuid = self.v2.uuid
        return uuid

    def create_snapshot(self, snap_name):
        if self.get_uuid_for_snap(snap_name) != None:
            raise VolumeException(EEXIST,
                                  f'subvolume {snap_name} already exists')

        super(SubvolumeV3, self).create_snapshot(snap_name)

    # TODO, XXX: check if v2 incar snap was deleted and therefore v2 incar has
    # been deleted completely
    def remove_snapshot(self, snap_name, force):
        snap_path = self.get_snapshot_path(snap_name)

        super(SubvolumeV3, self).remove_snapshot(snap_name, force=force,
                                                 snap_path=snap_path)

    def get_v3_snap_names(self):
        '''
        :returns: list of names of every snapshots of every incarnations.
        :rtype: list of str
        '''
        all_snap_names = []

        for uuid in self.get_incar_uuids():
            snap_base_path = self.get_incar_snap_base_path(uuid)
            snap_names = self.list_snaps(snap_base_path)
            all_snap_names.extend(snap_names)

        return all_snap_names

    def get_all_snap_names(self):
        '''
        :return: list of snap names
        :rtype: list of str
        '''
        snap_names = []
        snap_names.extend(self.get_v3_snap_names())
        snap_names.extend(self.v2.get_snap_names())
        return snap_names

    def remove_but_retain_snaps(self):
        assert self.state != SubvolumeStates.STATE_RETAINED

        try:
            self.update_meta_file_after_retain()
            self.trash_incarnation_dir()

            # Delete the volume meta file, if it's not already deleted
            self.auth_md.delete_subvolume_metadata_file(self.group.name, self.name)
        except MetadataMgrException as e:
            log.error(f"failed to write config: {e}")
            raise VolumeException(e.args[0], e.args[1])


    # ----- methods for subvol cloning -----


    @property
    def purgeable(self):
        return False if not self.retained or self.list_snapshots() else True
