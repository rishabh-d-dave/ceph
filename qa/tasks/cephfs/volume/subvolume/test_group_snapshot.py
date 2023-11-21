import errno
import json
import collections
import unittest

from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestSubvolumeGroupSnapshots(SubvolumeHelper):
    """
    Tests for FS subvolume group snapshot operations.
    """

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_nonexistent_subvolume_group_snapshot_rm(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove snapshot
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup snapshot rm' command to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_create_and_rm(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_idempotence(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot group
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # try creating snapshot w/ same snapshot name -- shoule be idempotent
        self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        # remove snapshot
        self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_ls(self):
        # tests the 'fs subvolumegroup snapshot ls' command

        snapshots = []

        # create group
        group = self._gen_subvol_grp_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumegroup snapshots
        snapshots = self._gen_subvol_snap_name(3)
        for snapshot in snapshots:
            self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)

        subvolgrpsnapshotls = json.loads(self._fs_cmd('subvolumegroup', 'snapshot', 'ls', self.volname, group))
        if len(subvolgrpsnapshotls) == 0:
            raise RuntimeError("Expected the 'fs subvolumegroup snapshot ls' command to list the created subvolume group snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolgrpsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                raise RuntimeError("Error creating or listing subvolume group snapshots")

    @unittest.skip("skipping subvolumegroup snapshot tests")
    def test_subvolume_group_snapshot_rm_force(self):
        # test removing non-existing subvolume group snapshot with --force
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()
        # remove snapshot
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "rm", self.volname, group, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup snapshot rm --force' command to succeed")

    def test_subvolume_group_snapshot_unsupported_status(self):
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # snapshot group
        try:
            self._fs_cmd("subvolumegroup", "snapshot", "create", self.volname, group, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOSYS, "invalid error code on subvolumegroup snapshot create")
        else:
            self.fail("expected subvolumegroup snapshot create command to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)



