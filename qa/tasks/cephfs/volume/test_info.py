import json

from tasks.cephfs.volumes.hepler import VolumeHelper

from logging import getLogger


log = getLogger(__name__)


class TestInfo(VolumeHelper):

    def test_volume_info(self):
        """
        Tests the 'fs volume info' command
        """
        vol_fields = ["pools", "used_size", "pending_subvolume_deletions", "mon_addrs"]
        group = self._gen_subvol_grp_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        self.assertEqual(vol_info["used_size"], 0,
                         "Size should be zero when volumes directory is empty")

    def test_volume_info_pending_subvol_deletions(self):
        """
        Tests the pending_subvolume_deletions in 'fs volume info' command
        """
        subvolname = self._gen_subvol_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--mode=777")
        # create 3K zero byte files
        self._do_subvolume_io(subvolname, number_of_files=3000, file_size=0)
        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        self.assertNotEqual(vol_info['pending_subvolume_deletions'], 0,
                            "pending_subvolume_deletions should be 1")
        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_volume_info_without_subvolumegroup(self):
        """
        Tests the 'fs volume info' command without subvolume group
        """
        vol_fields = ["pools", "mon_addrs"]
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        self.assertNotIn("used_size", vol_info,
                         "'used_size' should not be present in absence of subvolumegroup")
        self.assertNotIn("pending_subvolume_deletions", vol_info,
                         "'pending_subvolume_deletions' should not be present in absence"
                         " of subvolumegroup")

    def test_volume_info_with_human_readable_flag(self):
        """
        Tests the 'fs volume info --human_readable' command
        """
        vol_fields = ["pools", "used_size", "pending_subvolume_deletions", "mon_addrs"]
        group = self._gen_subvol_grp_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname, "--human_readable"))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
        assert vol_info["used_size"][-1] in units, "unit suffix in used_size is absent"
        assert vol_info["pools"]["data"][0]["avail"][-1] in units, "unit suffix in avail data is absent"
        assert vol_info["pools"]["data"][0]["used"][-1] in units, "unit suffix in used data is absent"
        assert vol_info["pools"]["metadata"][0]["avail"][-1] in units, "unit suffix in avail metadata is absent"
        assert vol_info["pools"]["metadata"][0]["used"][-1] in units, "unit suffix in used metadata is absent"
        self.assertEqual(int(vol_info["used_size"]), 0,
                         "Size should be zero when volumes directory is empty")

    def test_volume_info_with_human_readable_flag_without_subvolumegroup(self):
        """
        Tests the 'fs volume info --human_readable' command without subvolume group
        """
        vol_fields = ["pools", "mon_addrs"]
        # get volume metadata
        vol_info = json.loads(self._get_volume_info(self.volname, "--human_readable"))
        for md in vol_fields:
            self.assertIn(md, vol_info,
                          f"'{md}' key not present in metadata of volume")
        units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
        assert vol_info["pools"]["data"][0]["avail"][-1] in units, "unit suffix in avail data is absent"
        assert vol_info["pools"]["data"][0]["used"][-1] in units, "unit suffix in used data is absent"
        assert vol_info["pools"]["metadata"][0]["avail"][-1] in units, "unit suffix in avail metadata is absent"
        assert vol_info["pools"]["metadata"][0]["used"][-1] in units, "unit suffix in used metadata is absent"
        self.assertNotIn("used_size", vol_info,
                         "'used_size' should not be present in absence of subvolumegroup")
        self.assertNotIn("pending_subvolume_deletions", vol_info,
                         "'pending_subvolume_deletions' should not be present in absence"
                         " of subvolumegroup")


