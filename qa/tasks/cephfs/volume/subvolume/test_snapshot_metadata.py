import os
import errno
import json

from io import StringIO
from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestMetadata(SubvolumeHelper):

    def test_subvolume_snapshot_metadata_set(self):
        """
        Set custom metadata for subvolume snapshot.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_set_idempotence(self):
        """
        Set custom metadata for subvolume snapshot (Idempotency).
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        # set same metadata again for subvolume.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed because it is idempotent operation")

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get(self):
        """
        Get custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get_for_nonexisting_key(self):
        """
        Get custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # try to get value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, "key_nonexist", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_get_for_nonexisting_section(self):
        """
        Get custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # try to get value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, "key", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_update(self):
        """
        Update custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # update metadata against key.
        new_value = "new_value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, new_value, group)

        # get metadata for specified key of snapshot.
        try:
            ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(new_value, ret)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_list(self):
        """
        List custom metadata for subvolume snapshot.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, k, v, group)

        # list metadata
        try:
            ret_dict = json.loads(self._fs_cmd("subvolume", "snapshot", "metadata", "ls", self.volname, subvolname, snapshot, group))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata ls' command to succeed")

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_list_if_no_metadata_set(self):
        """
        List custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # list metadata
        try:
            ret_dict = json.loads(self._fs_cmd("subvolume", "snapshot", "metadata", "ls", self.volname, subvolname, snapshot, group))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata ls' command to succeed")

        # compare output with expected output
        empty_dict = {}
        self.assertDictEqual(ret_dict, empty_dict)

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove(self):
        """
        Remove custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, key, snapshot, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_for_nonexisting_key(self):
        """
        Remove custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # try to remove value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, "key_nonexist", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_for_nonexisting_section(self):
        """
        Remove custom metadata for subvolume snapshot if metadata is not added for subvolume snapshot.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # try to remove value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, "key", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_force(self):
        """
        Forcefully remove custom metadata for a specified key in subvolume snapshot metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key with --force option.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_remove_force_for_nonexisting_key(self):
        """
        Forcefully remove custom metadata for subvolume snapshot if specified key not exist in metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        # again remove metadata against already removed key with --force option.
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "rm", self.volname, subvolname, snapshot, key, group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata rm' (with --force) command to succeed")

        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_metadata_after_snapshot_remove(self):
        """
        Verify metadata removal of subvolume snapshot after snapshot removal.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)

        # get value for specified key.
        ret = self._fs_cmd("subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group)

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        # remove subvolume snapshot.
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)

        # try to get metadata after removing snapshot.
        # Expecting error ENOENT with error message of snapshot does not exist
        cmd_ret = self.run_ceph_cmd(
            args=["fs", "subvolume", "snapshot", "metadata", "get", self.volname, subvolname, snapshot, key, group], check_status=False, stdout=StringIO(),
            stderr=StringIO())
        self.assertEqual(cmd_ret.returncode, errno.ENOENT, "Expecting ENOENT error")
        self.assertIn(f"snapshot '{snapshot}' does not exist", cmd_ret.stderr.getvalue(),
                f"Expecting message: snapshot '{snapshot}' does not exist ")

        # confirm metadata is removed by searching section name in .meta file
        meta_path = os.path.join(".", "volumes", group, subvolname, ".meta")
        section_name = "SNAP_METADATA_" + snapshot

        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError as e:
            self.assertNotEqual(e.exitstatus, 0)
        else:
            self.fail("Expected non-zero exist status because section should not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_clean_stale_subvolume_snapshot_metadata(self):
        """
        Validate cleaning of stale subvolume snapshot metadata.
        """
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, group)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot, group)

        # set metadata for snapshot.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "snapshot", "metadata", "set", self.volname, subvolname, snapshot, key, value, group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume snapshot metadata set' command to succeed")

        # save the subvolume config file.
        meta_path = os.path.join(".", "volumes", group, subvolname, ".meta")
        tmp_meta_path = os.path.join(".", "volumes", group, subvolname, ".meta.stale_snap_section")
        self.mount_a.run_shell(['sudo', 'cp', '-p', meta_path, tmp_meta_path], omit_sudo=False)

        # Delete snapshot, this would remove user snap metadata
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot, group)

        # Copy back saved subvolume config file. This would have stale snapshot metadata
        self.mount_a.run_shell(['sudo', 'cp', '-p', tmp_meta_path, meta_path], omit_sudo=False)

        # Verify that it has stale snapshot metadata
        section_name = "SNAP_METADATA_" + snapshot
        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError:
            self.fail("Expected grep cmd to succeed because stale snapshot metadata exist")

        # Do any subvolume operation to clean the stale snapshot metadata
        _ = json.loads(self._get_subvolume_info(self.volname, subvolname, group))

        # Verify that the stale snapshot metadata is cleaned
        try:
            self.mount_a.run_shell(f"sudo grep {section_name} {meta_path}", omit_sudo=False)
        except CommandFailedError as e:
            self.assertNotEqual(e.exitstatus, 0)
        else:
            self.fail("Expected non-zero exist status because stale snapshot metadata should not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()
        # Clean tmp config file
        self.mount_a.run_shell(['sudo', 'rm', '-f', tmp_meta_path], omit_sudo=False)
