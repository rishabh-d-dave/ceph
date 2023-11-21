import os
import errno
import json
import collections

from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError

log = getLogger(__name__)


class TestCreate(SubvolumeHelper):

    def test_subvolume_create_and_rm(self):
        # create subvolume
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # make sure it exists
        subvolpath = self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        self.assertNotEqual(subvolpath, None)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        # make sure its gone
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume getpath' command to fail. Subvolume not removed.")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_default_uid_gid_subvolume(self):
        subvolume = self._gen_subvol_name()
        expected_uid = 0
        expected_gid = 0

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # check subvolume's uid and gid
        stat = self.mount_a.stat(subvol_path)
        self.assertEqual(stat['st_uid'], expected_uid)
        self.assertEqual(stat['st_gid'], expected_gid)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


    def test_subvolume_create_and_rm_in_group(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_create_idempotence(self):
        # create subvolume
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name -- should be idempotent
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_resize(self):
        # create subvolume
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name with size -- should set quota
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "1000000000")

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertEqual(subvol_info["bytes_quota"], 1000000000)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_mode(self):
        # default mode
        default_mode = "755"

        # create subvolume
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        actual_mode_1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_1, default_mode)

        # try creating w/ same subvolume name with --mode 777
        new_mode = "777"
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", new_mode)

        actual_mode_2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_2, new_mode)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_without_passing_mode(self):
        # create subvolume
        desired_mode = "777"
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", desired_mode)

        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        actual_mode_1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_1, desired_mode)

        # default mode
        default_mode = "755"

        # try creating w/ same subvolume name without passing --mode argument
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        actual_mode_2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode_2, default_mode)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_isolated_namespace(self):
        """
        Create subvolume in separate rados namespace
        """

        # create subvolume
        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--namespace-isolated")

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        self.assertNotEqual(len(subvol_info), 0)
        self.assertEqual(subvol_info["pool_namespace"], "fsvolumens_" + subvolume)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_auto_cleanup_on_fail(self):
        subvolume = self._gen_subvol_name()
        data_pool = "invalid_pool"
        # create subvolume with invalid data pool layout fails
        with self.assertRaises(CommandFailedError):
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)

        # check whether subvol path is cleaned up
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of non-existent subvolume")
        else:
            self.fail("expected the 'fs subvolume getpath' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_data_pool_layout_in_group(self):
        subvol1, subvol2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

        # create group. this also helps set default pool layout for subvolumes
        # created within the group.
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group.
        self._fs_cmd("subvolume", "create", self.volname, subvol1, "--group_name", group)
        subvol1_path = self._get_subvolume_path(self.volname, subvol1, group_name=group)

        default_pool = self.mount_a.getfattr(subvol1_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # create subvolume specifying the new data pool as its pool layout
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group,
                     "--pool_layout", new_pool)
        subvol2_path = self._get_subvolume_path(self.volname, subvol2, group_name=group)

        desired_pool = self.mount_a.getfattr(subvol2_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_mode(self):
        subvol1 = self._gen_subvol_name()

        # default mode
        default_mode = "755"
        # desired mode
        desired_mode = "777"

        self._fs_cmd("subvolume", "create", self.volname, subvol1,  "--mode", "777")

        subvol1_path = self._get_subvolume_path(self.volname, subvol1)

        # check subvolumegroup's mode
        subvol_par_path = os.path.dirname(subvol1_path)
        group_path = os.path.dirname(subvol_par_path)
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', group_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, default_mode)
        # check /volumes mode
        volumes_path = os.path.dirname(group_path)
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', volumes_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode2, default_mode)
        # check subvolume's  mode
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', subvol1_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode3, desired_mode)

        self._fs_cmd("subvolume", "rm", self.volname, subvol1)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_mode_in_group(self):
        subvol1, subvol2, subvol3 = self._gen_subvol_name(3)

        group = self._gen_subvol_grp_name()
        # default mode
        expected_mode1 = "755"
        # desired mode
        expected_mode2 = "777"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvol1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvol2, "--group_name", group, "--mode", "777")
        # check whether mode 0777 also works
        self._fs_cmd("subvolume", "create", self.volname, subvol3, "--group_name", group, "--mode", "0777")

        subvol1_path = self._get_subvolume_path(self.volname, subvol1, group_name=group)
        subvol2_path = self._get_subvolume_path(self.volname, subvol2, group_name=group)
        subvol3_path = self._get_subvolume_path(self.volname, subvol3, group_name=group)

        # check subvolume's  mode
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', subvol1_path]).stdout.getvalue().strip()
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', subvol2_path]).stdout.getvalue().strip()
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', subvol3_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, expected_mode1)
        self.assertEqual(actual_mode2, expected_mode2)
        self.assertEqual(actual_mode3, expected_mode2)

        self._fs_cmd("subvolume", "rm", self.volname, subvol1, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol2, group)
        self._fs_cmd("subvolume", "rm", self.volname, subvol3, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_desired_uid_gid(self):
        """
        That the subvolume can be created with the desired uid and gid and its uid and gid matches the
        expected values.
        """
        uid = 1000
        gid = 1000

        # create subvolume
        subvolname = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--uid", str(uid), "--gid", str(gid))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # verify the uid and gid
        suid = int(self.mount_a.run_shell(['stat', '-c' '%u', subvolpath]).stdout.getvalue().strip())
        sgid = int(self.mount_a.run_shell(['stat', '-c' '%g', subvolpath]).stdout.getvalue().strip())
        self.assertEqual(uid, suid)
        self.assertEqual(gid, sgid)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_invalid_data_pool_layout(self):
        subvolume = self._gen_subvol_name()
        data_pool = "invalid_pool"
        # create subvolume with invalid data pool layout
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on create of subvolume with invalid pool layout")
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_with_invalid_size(self):
        # create subvolume with an invalid size -1
        subvolume = self._gen_subvol_name()
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--size", "-1")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on create of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_and_ls_providing_group_as_nogroup(self):
        """
        That a 'subvolume create' and 'subvolume ls' should throw
        permission denied error if option --group=_nogroup is provided.
        """

        subvolname = self._gen_subvol_name()

        # try to create subvolume providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume create' command to fail")

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolname)

        # try to list subvolumes providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail")

        # list subvolumes
        self._fs_cmd("subvolume", "ls", self.volname)

        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean.
        self._wait_for_trash_empty()


class TestRm(SubvolumeHelper):

    def test_async_subvolume_rm(self):
        subvolumes = self._gen_subvol_name(100)

        # create subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")
            self._do_subvolume_io(subvolume, number_of_files=10)

        self.mount_a.umount_wait()

        # remove subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        self.mount_a.mount_wait()

        # verify trash dir is clean
        self._wait_for_trash_empty(timeout=300)

    def test_nonexistent_subvolume_rm(self):
        # remove non-existing subvolume
        subvolume = "non_existent_subvolume"

        # try, remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume rm' command to fail")

    def test_subvolume_rm_force(self):
        # test removing non-existing subvolume with --force
        subvolume = self._gen_subvol_name()
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm --force' command to succeed")



    def test_subvolume_retain_snapshot_rm_idempotency(self):
        """
        ensure subvolume deletion of a subvolume which is already deleted with retain snapshots option passes.
        After subvolume deletion with retain snapshots, the subvolume exists until the trash directory (resides inside subvolume)
        is cleaned up. The subvolume deletion issued while the trash directory is not empty, should pass and should
        not error out with EAGAIN.
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=256)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # remove snapshots (removes retained volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume (check idempotency)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                self.fail(f"expected subvolume rm to pass with error: {os.strerror(ce.exitstatus)}")

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestLs(SubvolumeHelper):

    def test_subvolume_ls(self):
        # tests the 'fs subvolume ls' command

        subvolumes = []

        # create subvolumes
        subvolumes = self._gen_subvol_name(3)
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # list subvolumes
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        if len(subvolumels) == 0:
            self.fail("Expected the 'fs subvolume ls' command to list the created subvolumes.")
        else:
            subvolnames = [subvolume['name'] for subvolume in subvolumels]
            if collections.Counter(subvolnames) != collections.Counter(subvolumes):
                self.fail("Error creating or listing subvolumes")

        # remove subvolume
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_ls_with_groupname_as_internal_directory(self):
        # tests the 'fs subvolume ls' command when the default groupname as internal directories
        # Eg: '_nogroup', '_legacy', '_deleting', '_index'.
        # Expecting 'fs subvolume ls' will be fail with errno EINVAL for '_legacy', '_deleting', '_index'
        # Expecting 'fs subvolume ls' will be fail with errno EPERM for '_nogroup'

        # try to list subvolumes providing --group_name=_nogroup option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_nogroup")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EPERM' for _nogroup")

        # try to list subvolumes providing --group_name=_legacy option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_legacy")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _legacy")

        # try to list subvolumes providing --group_name=_deleting option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_deleting")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _deleting")

        # try to list subvolumes providing --group_name=_index option
        try:
            self._fs_cmd("subvolume", "ls", self.volname, "--group_name", "_index")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL)
        else:
            self.fail("expected the 'fs subvolume ls' command to fail with error 'EINVAL' for _index")

    def test_subvolume_ls_for_notexistent_default_group(self):
        # tests the 'fs subvolume ls' command when the default group '_nogroup' doesn't exist
        # prerequisite: we expect that the volume is created and the default group _nogroup is
        # NOT created (i.e. a subvolume without group is not created)

        # list subvolumes
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        if len(subvolumels) > 0:
            raise RuntimeError("Expected the 'fs subvolume ls' command to output an empty list.")


class TestExist(SubvolumeHelper):

    def test_subvolume_exists_with_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolume by specifying the name of subvolumegroup"""

        group = self._gen_subvol_grp_name()
        subvolume1 = self._gen_subvol_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "subvolume exists")
        # delete subvolume in group
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_exists_with_subvolumegroup_and_no_subvolume(self):
        """Test the presence of any subvolume specifying the name
            of subvolumegroup and no subvolumes"""

        group = self._gen_subvol_grp_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_exists_without_subvolumegroup_and_with_subvolume(self):
        """Test the presence of any subvolume without specifying the name
            of subvolumegroup"""

        subvolume1 = self._gen_subvol_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume1)
        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolume exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")

    def test_subvolume_exists_without_subvolumegroup_and_without_subvolume(self):
        """Test the presence of any subvolume without any subvolumegroup
            and without any subvolume"""

        ret = self._fs_cmd("subvolume", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")

    def test_subvolume_user_metadata_set(self):
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

        self._fs_cmd("subvolume", "rm", self.volname, subvolname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean.
        self._wait_for_trash_empty()


class TestPin(SubvolumeHelper):

    def test_subvolume_pin_export(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "export", "1")
        path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        path = os.path.dirname(path) # get subvolume path

        self._get_subtrees(status=status, rank=1)
        self._wait_subtrees([(path, 1)], status=status)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_pin_random(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_random', True)

        subvolume = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "random", ".01")
        # no verification

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()
