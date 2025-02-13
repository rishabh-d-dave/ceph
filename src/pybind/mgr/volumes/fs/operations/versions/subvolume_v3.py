import errno
from os import path as os_path
from logging import getLogger
from uuid import uuid4

import cephfs

from .subvolume_v2 import SubvolumeV2
from ...exception import VolumeException
from ...fs_util import listdir


log = getLogger(__name__)


class SubvolumeV3(SubvolumeV2):
    '''
    1. Following is layout of subvol v3 directories.

        /volumes/<group>/<subvol>/roots/<uuid1>/mnt
                                                ^ mount dir
                                        ^ data dir/uuid dir
                                    ^ roots dir
                         ^ subvol dir

    2. Following is the path of meta file -

    /volumes/<group>/<subvol>/.meta.<uuid3>

    3. UUID dir of current incarnation can be found through the symlink -

    /volumes/<group>/<subvol>/roots/{.meta -> .meta.<uuid3>}

    NOTE: Absence of ".meta" implies that subvolume has been deleted, taking
    snapshots will not be possible then.

    4. Subvolume metadata (.fscrypt and .snap for example) lives in UUID dir -

    /volumes/<group>/<subvol>/roots/<uuid1>/.fscrypt
    /volumes/<group>/<subvol>/roots/<uuid1>/.snap

    5. This is how a subvol will look with previous incarnations -

    /volumes/<group>/<subvol>/roots/<uuid1>
    /volumes/<group>/<subvol>/roots/<uuid2>
    /volumes/<group>/<subvol>/roots/<uuid3>
    /volumes/<group>/<subvol>/roots/.meta.<uuid1>
    /volumes/<group>/<subvol>/roots/.meta.<uuid2>
    /volumes/<group>/<subvol>/roots/.meta.<uuid3>
    '''

    VERSION = 3

    def __init__(self, mgr, fs, vol_spec, group, subvolname):
        # XXX: this needs to be defined beforehand since __init__() below calls
        # __init__() from previous versions and previous versions needs
        # self.base_path to be defined. self.subvol_dir in v3 is same
        # self.base_path in older versions.
        self.subvol_dir = f'/volumes/{group.groupname}/{subvolname}'

        # XXX: both of these needs to be defined beforehand because __init__()
        # below will initialize metadata manager too which results in
        # self.config_path() being called. and self.config_path() needs this
        # variable (see the method's comment for the reason behind it).
        self.uuid = uuid4()
        self.meta = f'{self.subvol_dir}/.meta.{self.uuid}'

        # encode these variables since they'll be used in __init__() below and
        # all its underlying calls.
        self.meta = self.meta.encode('utf-8')
        self.subvol_dir = self.subvol_dir.encode('utf-8')

        super(SubvolumeV3, self).__init__(mgr, fs, vol_spec, group, subvolname)

        self.TRASH_DIR = '/volumes/.deleting'.encode('utf-8')

        # decoding it so that rest of the paths can be built using this path.
        # It must be encoded again before this method ends since rest of the
        # class needs it in encoded form.
        self.subvol_dir = self.subvol_dir.decode('utf-8')

        # contains data dir for all incarnations
        self.roots_dir = f'{self.subvol_dir}/roots'
        # meta file for the current subvolume's incarnation
        self.current_meta = f'{self.subvol_dir}/.meta'

        self.data_dir = f'{self.roots_dir}/{self.uuid}'
        self.mount_dir = f'{self.data_dir}/mnt'
        self.snap_dir = f'{self.data_dir}/.snap'
        self.fscrypt_dir = f'{self.data_dir}/.fscrypt'

        self.subvol_dir = self.subvol_dir.encode('utf-8')
        self.roots_dir = self.roots_dir.encode('utf-8')
        self.current_meta = self.current_meta.encode('utf-8')
        # encoded already before calling __init__(), keeping this comment to
        # prevent accidental re-encoding in future.
        #self.meta = self.meta.encode('utf-8')

        self.data_dir = self.data_dir.encode('utf-8')
        self.mount_dir = self.mount_dir.encode('utf-8')
        self.snap_dir = self.snap_dir.encode('utf-8')
        self.fscrypt_dir = self.fscrypt_dir.encode('utf-8')

    @property
    def base_path(self):
        return self.subvol_dir

    @property
    def config_path(self):
        '''
        Path to meta file for current incarnation of the subvolume.

        NOTE: overriding method from class SubvolumeBase, since meta file's name
        now contains UUID in it and there UUID can't be accessed in class
        SubvolumeBase
        '''
        return self.meta

    # NOTE: overriding it to delete it since subvol v3 doesn't have in-subvol
    # trash dir.
    @property
    def trash_dir(self):
        pass

    # overriding to delete it, subvol v3 doesn't have in-subvol trash dir
    def create_trashcan(self):
        pass

    def set_subvol_xattr(self):
        # set subvolume attr, on subvolume root, marking it as a CephFS subvolume
        # subvolume root is where snapshots would be taken, and hence is the base_path for v2 subvolumes
        try:
            # MDS treats this as a noop for already marked subvolume
            self.fs.setxattr(self.data_dir, 'ceph.dir.subvolume', b'1', 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid value specified for ceph.dir.subvolume")
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def _create_v3_layout(self, mode):
        #for path in (self.subvol_dir, self.roots_dir, self.data_dir, self.mount_dir):
        #    self.fs.mkdir(path, mode)

        self.fs.mkdirs(self.mount_dir.decode('utf-8'), mode)

    def create_or_update_meta_file(self):
        super(SubvolumeV3, self).create_or_update_meta_file()

        self.fs.symlink(os_path.basename(self.meta), self.current_meta)

    # TODO: make sure "fs subvolume create" for v3 is idempotent.
    def _create(self, uid, gid, pool, mode, isolate_namespace, size, earmark):
        self._create_v3_layout(mode)

        self.set_subvol_xattr()
        self.set_attrs_on_subvol(self.subvol_dir, uid, gid, pool,
                                 isolate_namespace, size, earmark)

        self.create_or_update_meta_file()
        # Create the subvolume metadata file which manages auth-ids if it
        # doesn't exist
        self.auth_mdata_mgr.create_subvolume_metadata_file(
            self.group.groupname, self.subvolname)

    def trash_incarnation_dir(self):
        uuid = os_path.basename(self.data_dir)
        DST_PATH = os_path.join(self.TRASH_DIR, uuid)
        self.fs.rename(self.data_dir, DST_PATH)

    # NOTE: to ensure subvol v2 code can be re-used here
    def snapshot_base_path(self):
        return self.snap_dir

    @property
    def has_pending_purges(self):
        try:
            return not listdir(self.fs, self.TRASH_DIR) == []
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return False
            raise
