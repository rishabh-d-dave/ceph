import errno
import json

from logging import getLogger

from tasks.cephfs.volumes.hepler import VolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestRename(VolumeHelper):
    def test_volume_rename(self):
        """
        That volume, its file system and pools, can be renamed.
        """
        for m in self.mounts:
            m.umount_wait()
        oldvolname = self.volname
        newvolname = self._gen_vol_name()
        new_data_pool, new_metadata_pool = f"cephfs.{newvolname}.data", f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        # volume name changed
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        # pool names changed
        self.fs.get_pool_names(refresh=True)
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        self.assertEqual(new_data_pool, self.fs.get_data_pool_name())

    def test_volume_rename_idempotency(self):
        """
        That volume rename is idempotent.
        """
        for m in self.mounts:
            m.umount_wait()
        oldvolname = self.volname
        newvolname = self._gen_vol_name()
        new_data_pool, new_metadata_pool = f"cephfs.{newvolname}.data", f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        self._fs_cmd("volume", "rename", oldvolname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        self.fs.get_pool_names(refresh=True)
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        self.assertEqual(new_data_pool, self.fs.get_data_pool_name())

    def test_volume_rename_fails_without_confirmation_flag(self):
        """
        That renaming volume fails without --yes-i-really-mean-it flag.
        """
        newvolname = self._gen_vol_name()
        try:
            self._fs_cmd("volume", "rename", self.volname, newvolname)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                "invalid error code on renaming a FS volume without the "
                "'--yes-i-really-mean-it' flag")
        else:
            self.fail("expected renaming of FS volume to fail without the "
                      "'--yes-i-really-mean-it' flag")

    def test_volume_rename_for_more_than_one_data_pool(self):
        """
        That renaming a volume with more than one data pool does not change
        the name of the data pools.
        """
        for m in self.mounts:
            m.umount_wait()
        self.fs.add_data_pool('another-data-pool')
        oldvolname = self.volname
        newvolname = self._gen_vol_name()
        self.fs.get_pool_names(refresh=True)
        orig_data_pool_names = list(self.fs.data_pools.values())
        new_metadata_pool = f"cephfs.{newvolname}.meta"
        self._fs_cmd("volume", "rename", self.volname, newvolname,
                     "--yes-i-really-mean-it")
        volumels = json.loads(self._fs_cmd('volume', 'ls'))
        volnames = [volume['name'] for volume in volumels]
        # volume name changed
        self.assertIn(newvolname, volnames)
        self.assertNotIn(oldvolname, volnames)
        self.fs.get_pool_names(refresh=True)
        # metadata pool name changed
        self.assertEqual(new_metadata_pool, self.fs.get_metadata_pool_name())
        # data pool names unchanged
        self.assertCountEqual(orig_data_pool_names, list(self.fs.data_pools.values()))

    def test_rename_when_fs_is_online(self):
        for m in self.mounts:
            m.umount_wait()
        newvolname = self._gen_vol_name()

        self.run_ceph_cmd(f'fs set {self.volname} refuse_client_session true')
        self.negtest_ceph_cmd(
            args=(f'fs volume rename {self.volname} {newvolname} '
                   '--yes-i-really-mean-it'),
            errmsgs=(f"CephFS '{self.volname}' is not offline. Before "
                      "renaming a CephFS, it must be marked as down. See "
                      "`ceph fs fail`."),
            retval=errno.EPERM)
        self.run_ceph_cmd(f'fs set {self.volname} refuse_client_session false')

    def test_rename_when_clients_arent_refused(self):
        newvolname = self._gen_vol_name()
        for m in self.mounts:
            m.umount_wait()

        self.run_ceph_cmd(f'fs fail {self.volname}')
        self.negtest_ceph_cmd(
            args=(f'fs volume rename {self.volname} {newvolname} '
                   '--yes-i-really-mean-it'),
            errmsgs=(f"CephFS '{self.volname}' doesn't refuse clients. "
                      "Before renaming a CephFS, flag "
                      "'refuse_client_session' must be set. See "
                      "`ceph fs set`."),
            retval=errno.EPERM)


