import errno
import logging
import importlib
from os.path import dirname, basename

import cephfs

from .subvolume_base import SubvolumeBase
from .subvolume_attrs import SubvolumeTypes
from .subvolume_v1 import SubvolumeV1
from .subvolume_v2 import SubvolumeV2
from .subvolume_v3 import SubvolumeV3
from .metadata_manager import MetadataManager
from .op_sm import SubvolumeOpSm
from ..template import SubvolumeOpType
from ...fs_util import statx_path, get_all_xattrs, set_all_xattrs
from ...exception import (MetadataMgrException, OpSmException, VolumeException,
                          SubvolUpgradeError)

log = logging.getLogger(__name__)

class SubvolumeLoader(object):
    INVALID_VERSION = -1

    SUPPORTED_MODULES = ['subvolume_v1.SubvolumeV1', 'subvolume_v2.SubvolumeV2',
                         'subvolume_v3.SubvolumeV3']

    def __init__(self):
        self.max_version = SubvolumeLoader.INVALID_VERSION
        self.versions = {}

    def _load_module(self, mod_cls):
        mod_name, cls_name = mod_cls.split('.')
        mod = importlib.import_module('.versions.{0}'.format(mod_name), package='volumes.fs.operations')
        return getattr(mod, cls_name)

    def _load_supported_versions(self):
        for mod_cls in SubvolumeLoader.SUPPORTED_MODULES:
            cls = self._load_module(mod_cls)
            log.info("loaded v{0} subvolume".format(cls.version()))
            if self.max_version is not None or cls.version() > self.max_version:
                self.max_version = cls.version()
                self.versions[cls.version()] = cls
        if self.max_version == SubvolumeLoader.INVALID_VERSION:
            raise VolumeException(-errno.EINVAL, "no subvolume version available")
        log.info("max subvolume version is v{0}".format(self.max_version))

    def get_subvolume_class(self, version):
        try:
            return self.versions[version]
        except KeyError:
            raise VolumeException(-errno.EINVAL, "subvolume class v{0} does not exist".format(version))

    def get_subvolume_object_max(self, mgr, fs, vol_spec, group, subvolname):
        return self.get_subvolume_class(self.max_version)(mgr, fs, vol_spec, group, subvolname)

    def allow_subvol_upgrade_from_v1_to_v2(self, subvolume):
        asu = True
        try:
            opt = subvolume.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_ALLOW_SUBVOLUME_UPGRADE)
            asu = False if opt == "0" else True
        except MetadataMgrException:
            # this key is injected for QA testing and will not be available in
            # production
            pass

        return asu

    def upgrade_to_v3_layout(self, base_subvol):
        log.info(f'upgrading subvol {base_subvol.name} from v2 to v3, '
                 'upgrading its layout...')

        fs = base_subvol.fs
        v2_sv_uuid_path = base_subvol.metadata_mgr.get_global_option('path')
        sv_path = dirname(v2_sv_uuid_path)
        sv_uuid = basename(v2_sv_uuid_path)

        try:
            uid, gid, mode = statx_path(fs, v2_sv_uuid_path,
                                        ('uid', 'gid', 'mode'))

            sv_xattrs = get_all_xattrs(fs, v2_sv_uuid_path)
            log.info(f'mark123 sv_xattrs = {sv_xattrs}')
            log.info(f'mark123 v2_sv_uuid_path = {v2_sv_uuid_path}')

            has_v2_snaps = False
            if statx_path(fs, f'{v2_sv_uuid_path}/.snap'):
                has_v2_snaps = True

            v3_subvol_mnt_path = f'{sv_path}/roots/{sv_uuid}/mnt'
            v3_subvol_uuid_path = f'{sv_path}/roots/{sv_uuid}'
            sv_meta_path = f'{sv_path}/.meta'
            sv_incar_meta_path = f'{sv_path}/.meta.{sv_uuid}'

            fs.mkdirs(v3_subvol_uuid_path, 0o755)
            if has_v2_snaps:
                fs.mkdir(v3_subvol_mnt_path, 0o755)
            else:
                fs.rename(v2_sv_uuid_path, v3_subvol_mnt_path)

            fs.chown(v3_subvol_mnt_path, uid, gid)
            fs.chmod(v3_subvol_mnt_path, mode)

            if sv_xattrs:
                set_all_xattrs(fs, v3_subvol_mnt_path, sv_xattrs)

            fs.rename(sv_meta_path, sv_incar_meta_path)
            fs.symlink(f'.meta.{sv_uuid}', sv_meta_path)
            fs.chown(sv_incar_meta_path, 0, 0)
            fs.chmod(sv_incar_meta_path, 644)
        except cephfs.Error as e:
            raise SubvolUpgradeError(-e.args[0],
                                     f'error upgrading subvol {base_subvol.name} '
                                     'from v2 to v3')

        log.info(f'layout upgrade for subvol {base_subvol.name} was '
                 'successful, updating its metadata file...')

        v3_subvol = SubvolumeV3(mgr=base_subvol.mgr, fs=base_subvol.fs,
                            vol_spec=base_subvol.vol_spec, group=base_subvol.group,
                            name=base_subvol.name, uuid=sv_uuid)
        return v3_subvol, has_v2_snaps

    def upgrade_sv_md_to_v3(self, v3_subvol, has_v2_snaps):
        v3_subvol.md.refresh()

        log.info(f'upgrade for subvol {v3_subvol.name} from v2 to v3 is complete')
        try:
            # meta file path for v3 is different, hence we need v3 subvol obj
            v3_subvol.md.update_global_section('version', v3_subvol.version())
            v3_subvol.md.update_global_section('path', v3_subvol.mnt_dir.decode('utf-8'))

            if has_v2_snaps:
                v3_subvol.md.update_global_section('has_v2_snaps', 'True')

            v3_subvol.metadata_mgr.flush()
        except MetadataMgrException as e:
            raise VolumeException(-e.args[0],
                                  'error updating subvol metadata during '
                                  'subvol upgrade from v2 to v3')

    def upgrade_subvol_from_v2_to_v3(self, base_subvol, version):
        v3_subvol, has_v2_snaps = self.upgrade_to_v3_layout(base_subvol)
        self.upgrade_sv_md_to_v3(v3_subvol, has_v2_snaps)
        return v3_subvol

    def upgrade_subvol_to_v3(self, base_subvol, version):
        assert version != SubvolumeV3.version()

        if version == 2:
            return self.upgrade_subvol_from_v2_to_v3(base_subvol, version)
        elif version == 1:
            assert False
        elif version == 0:
            assert False
        else:
            assert False

    def upgrade_to_v2_subvolume(self, base_subvol, version):
        if base_subvol.legacy_mode:
            raise SubvolUpgradeError(errno.ENOTSUP,
                                     f'legacy subvol {base_subvol.name} '
                                     'cant be upgraded to v2')
        assert version == SubvolumeV2.version()

        if not self.allow_subvol_upgrade_from_v1_to_v2(base_subvol):
            raise SubvolUpgradeError(errno.ENOTSUP,
                                     f'v1 subvol {base_subvol.name} cant '
                                     'be upgraded to v2')

        log.info(f'upgrading subvol {base_subvol.name} from v1 to v2...')
        v1_subvol = SubvolumeV1(base_subvol.mgr, base_subvol.fs,
                                base_subvol.vol_spec, base_subvol.group,
                                base_subvol.name)
        try:
            v1_subvol.open(SubvolumeOpType.SNAP_LIST)
        except VolumeException as ve:
            if ve.errno == -errno.EAGAIN:
                raise SubvolUpgradeError(errno.EAGAIN,
                                         f'v1 subvol {base_subvol.name} '
                                         'isnt ready for snapshot listing yet, '
                                         'upgrading it to v2 isnt possible at '
                                         'the moment')
            raise

        if v1_subvol.list_snapshots():
                raise SubvolUpgradeError(errno.ENOTSUP,
                                         f'v1 subvol {base_subvol.name} '
                                         'has snapshots, upgrading it to v2 '
                                         'isnt possible at the moment')

        base_subvol.md.update_global_section(MetadataManager.GLOBAL_META_KEY_VERSION, SubvolumeV2.version())
        base_subvol.md.flush()

        return SubvolumeV2(base_subvol.mgr, base_subvol.fs,
                           base_subvol.vol_spec, base_subvol.group,
                           base_subvol.name,
                           legacy=base_subvol.legacy_mode)

    def upgrade_legacy_subvolume(self, fs, subvolume):
        assert subvolume.legacy_mode
        try:
            fs.mkdirs(subvolume.legacy_dir, 0o700)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], "error accessing subvolume")
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")
        qpath = subvolume.base_path.decode('utf-8')
        # legacy is only upgradable to v1
        subvolume.init_config(SubvolumeV1.version(), subvolume_type, qpath, initial_state)

    def upgrade_subvol(self, base_subvol, version):
        assert not base_subvol.legacy_mode

        assert version != 3

        if version == 2:
            subvol = self.upgrade_subvol_to_v3(base_subvol, version)
        elif version == 1:
            subvol = self.upgrade_to_v2_subvolume(base_subvol, version)
        elif version == 0:
            # TODO: move logic for legacy subvolume here eventually
            assert False
        else:
            assert False

        return subvol

    def get_subvolume_object(self, mgr, fs, vol_spec, group, subvolname,
                             upgrade=True):
        base_subvol = SubvolumeBase(mgr, fs, vol_spec, group, subvolname)

        try:
            version = base_subvol.discover()
            log.info(f'version of discovered subvol is {version}')
            if version == 3:
                sv_path = base_subvol.metadata_mgr.get_global_option('path')
                sv_uuid = basename(dirname(sv_path))
                subvol = SubvolumeV3(base_subvol.mgr, base_subvol.fs,
                                     base_subvol.vol_spec, base_subvol.group,
                                     base_subvol.name, uuid=sv_uuid)
            else:
                subvol = self.upgrade_subvol(base_subvol, version)

            subvol.metadata_mgr.refresh()
            subvol.clean_stale_snapshot_metadata()
            return subvol
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT and upgrade:
                self.upgrade_legacy_subvolume(fs, base_subvol)
                return self.get_subvolume_object(mgr, fs, vol_spec, group, subvolname, upgrade=False)
            else:
                # log the actual error and generalize error string returned to user
                log.error("error accessing subvolume metadata for '{0}' ({1})".format(subvolname, me))
                raise VolumeException(-errno.EINVAL, "error accessing subvolume metadata")

loaded_subvolumes = SubvolumeLoader()
loaded_subvolumes._load_supported_versions()
