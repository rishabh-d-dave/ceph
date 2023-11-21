import json
import collections

from logging import getLogger

from tasks.cephfs.volumes.hepler import VolumeHelper


log = getLogger(__name__)


class TestVolumeBasic(VolumeHelper):
    """
    Tests for FS volume operations.
    """

    def test_volume_create(self):
        """
        That the volume can be created and then cleans up
        """
        volname = self._gen_vol_name()
        self._fs_cmd("volume", "create", volname)
        volumels = json.loads(self._fs_cmd("volume", "ls"))

        if not (volname in ([volume['name'] for volume in volumels])):
            raise RuntimeError("Error creating volume '{0}'".format(volname))

        # check that the pools were created with the correct config
        pool_details = json.loads(self._raw_cmd("osd", "pool", "ls", "detail", "--format=json"))
        pool_flags = {}
        for pool in pool_details:
            pool_flags[pool["pool_id"]] = pool["flags_names"].split(",")

        volume_details = json.loads(self._fs_cmd("get", volname, "--format=json"))
        for data_pool_id in volume_details['mdsmap']['data_pools']:
            self.assertIn("bulk", pool_flags[data_pool_id])
        meta_pool_id = volume_details['mdsmap']['metadata_pool']
        self.assertNotIn("bulk", pool_flags[meta_pool_id])

        # clean up
        self._fs_cmd("volume", "rm", volname, "--yes-i-really-mean-it")

    def test_volume_ls(self):
        """
        That the existing and the newly created volumes can be listed and
        finally cleans up.
        """
        vls = json.loads(self._fs_cmd("volume", "ls"))
        volumes = [volume['name'] for volume in vls]

        #create new volumes and add it to the existing list of volumes
        volumenames = self._gen_vol_name(2)
        for volumename in volumenames:
            self._fs_cmd("volume", "create", volumename)
        volumes.extend(volumenames)

        # list volumes
        try:
            volumels = json.loads(self._fs_cmd('volume', 'ls'))
            if len(volumels) == 0:
                raise RuntimeError("Expected the 'fs volume ls' command to list the created volumes.")
            else:
                volnames = [volume['name'] for volume in volumels]
                if collections.Counter(volnames) != collections.Counter(volumes):
                    raise RuntimeError("Error creating or listing volumes")
        finally:
            # clean up
            for volume in volumenames:
                self._fs_cmd("volume", "rm", volume, "--yes-i-really-mean-it")
