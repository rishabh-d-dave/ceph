import os
import errno
import json

from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestMetadata(SubvolumeHelper):

    def test_subvolume_user_metadata_set_idempotence(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed")

        # set same metadata again for subvolume.
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed because it is idempotent operation")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get_for_nonexisting_key(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # try to get value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key_nonexist", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_get_for_nonexisting_section(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # try to get value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_update(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # update metadata against key.
        new_value = "new_value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, new_value, "--group_name", group)

        # get metadata for specified key of subvolume.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(new_value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, k, v, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        ret_dict = json.loads(ret)

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list_if_no_metadata_set(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # compare output with expected output
        # expecting empty json/dictionary
        self.assertEqual(ret, "{}")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_for_nonexisting_key(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # try to remove value for nonexisting key
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key_nonexist", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because 'key_nonexist' does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_for_nonexisting_section(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # try to remove value for nonexisting key (as section does not exist)
        # Expecting ENOENT exit status because key does not exist
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because section does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_force(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key with --force option.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_remove_force_for_nonexisting_key(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key does not exist")

        # again remove metadata against already removed key with --force option.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, key, "--group_name", group, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' (with --force) command to succeed")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_set_and_get_for_legacy_subvolume(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolname)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # set metadata for subvolume.
        key = "key"
        value = "value"
        try:
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, key, value, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata set' command to succeed")

        # get value for specified key.
        try:
            ret = self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, key, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata get' command to succeed")

        # remove '\n' from returned value.
        ret = ret.strip('\n')

        # match received value with expected value.
        self.assertEqual(value, ret)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()

    def test_subvolume_user_metadata_list_and_remove_for_legacy_subvolume(self):
        subvolname = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolname)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # set metadata for subvolume.
        input_metadata_dict =  {f'key_{i}' : f'value_{i}' for i in range(3)}

        for k, v in input_metadata_dict.items():
            self._fs_cmd("subvolume", "metadata", "set", self.volname, subvolname, k, v, "--group_name", group)

        # list metadata
        try:
            ret = self._fs_cmd("subvolume", "metadata", "ls", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata ls' command to succeed")

        ret_dict = json.loads(ret)

        # compare output with expected output
        self.assertDictEqual(input_metadata_dict, ret_dict)

        # remove metadata against specified key.
        try:
            self._fs_cmd("subvolume", "metadata", "rm", self.volname, subvolname, "key_1", "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume metadata rm' command to succeed")

        # confirm key is removed by again fetching metadata
        try:
            self._fs_cmd("subvolume", "metadata", "get", self.volname, subvolname, "key_1", "--group_name", group)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT)
        else:
            self.fail("Expected ENOENT because key_1 does not exist")

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()
