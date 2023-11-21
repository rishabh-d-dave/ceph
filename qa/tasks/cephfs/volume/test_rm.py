import errno
import json

from logging import getLogger

from tasks.cephfs.volumes.hepler import VolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestRm(VolumeHelper):
    def test_volume_rm(self):
        """
        That the volume can only be removed when --yes-i-really-mean-it is used
        and verify that the deleted volume is not listed anymore.
        """
        for m in self.mounts:
            m.umount_wait()
        try:
            self._fs_cmd("volume", "rm", self.volname)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EPERM:
                raise RuntimeError("expected the 'fs volume rm' command to fail with EPERM, "
                                   "but it failed with {0}".format(ce.exitstatus))
            else:
                self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

                #check if it's gone
                volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
                if (self.volname in [volume['name'] for volume in volumes]):
                    raise RuntimeError("Expected the 'fs volume rm' command to succeed. "
                                       "The volume {0} not removed.".format(self.volname))
        else:
            raise RuntimeError("expected the 'fs volume rm' command to fail.")

    def test_volume_rm_arbitrary_pool_removal(self):
        """
        That the arbitrary pool added to the volume out of band is removed
        successfully on volume removal.
        """
        for m in self.mounts:
            m.umount_wait()
        new_pool = "new_pool"
        # add arbitrary data pool
        self.fs.add_data_pool(new_pool)
        vol_status = json.loads(self._fs_cmd("status", self.volname, "--format=json-pretty"))
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

        #check if fs is gone
        volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
        volnames = [volume['name'] for volume in volumes]
        self.assertNotIn(self.volname, volnames)

        #check if osd pools are gone
        pools = json.loads(self._raw_cmd("osd", "pool", "ls", "--format=json-pretty"))
        for pool in vol_status["pools"]:
            self.assertNotIn(pool["name"], pools)

    def test_volume_rm_when_mon_delete_pool_false(self):
        """
        That the volume can only be removed when mon_allowd_pool_delete is set
        to true and verify that the pools are removed after volume deletion.
        """
        for m in self.mounts:
            m.umount_wait()
        self.config_set('mon', 'mon_allow_pool_delete', False)
        try:
            self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "expected the 'fs volume rm' command to fail with EPERM, "
                             "but it failed with {0}".format(ce.exitstatus))
        vol_status = json.loads(self._fs_cmd("status", self.volname, "--format=json-pretty"))
        self.config_set('mon', 'mon_allow_pool_delete', True)
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

        #check if fs is gone
        volumes = json.loads(self._fs_cmd("volume", "ls", "--format=json-pretty"))
        volnames = [volume['name'] for volume in volumes]
        self.assertNotIn(self.volname, volnames,
                         "volume {0} exists after removal".format(self.volname))
        #check if pools are gone
        pools = json.loads(self._raw_cmd("osd", "pool", "ls", "--format=json-pretty"))
        for pool in vol_status["pools"]:
            self.assertNotIn(pool["name"], pools,
                             "pool {0} exists after volume removal".format(pool["name"]))


