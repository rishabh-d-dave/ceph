class TestSubvolumes(TestVolumesHelper):
    """
    Tests for FS subvolume operations, except snapshot and snapshot clone.
    """

    def test_default_uid_gid_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
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

class TestRm:

    def test_async_subvolume_rm(self):
        subvolumes = self._generate_random_subvolume_name(100)

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
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

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


class TestCreate:

    def test_subvolume_create_and_rm(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
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

    def test_subvolume_create_and_rm_in_group(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # try creating w/ same subvolume name -- should be idempotent
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_create_idempotence_resize(self):
        # create subvolume
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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
        subvol1, subvol2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

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
        subvol1 = self._generate_random_subvolume_name()

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
        subvol1, subvol2, subvol3 = self._generate_random_subvolume_name(3)

        group = self._generate_random_group_name()
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
        subvolname = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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
        subvolume = self._generate_random_subvolume_name()
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

        subvolname = self._generate_random_subvolume_name()

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

class TestLs:

    def test_subvolume_ls(self):
        # tests the 'fs subvolume ls' command

        subvolumes = []

        # create subvolumes
        subvolumes = self._generate_random_subvolume_name(3)
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


class TestAuthorize:

    def test_authorize_deauthorize_legacy_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid = "alice"

        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # emulate a old-fashioned subvolume in a custom group
        createpath = os.path.join(".", "volumes", group, subvolume)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        mount_path = os.path.join("/", "volumes", group, subvolume)

        # authorize guest authID read-write access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id")

        # guest authID should exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(authid), existing_ids)

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, authid, key)

        # mount the subvolume, and write to it
        guest_mount.mount_wait(cephfs_mntpt=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # authorize guest authID read access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id", "--access_level", "r")

        # guest client sees the change in access level to read only after a
        # remount of the subvolume.
        guest_mount.umount_wait()
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # read existing content of the subvolume
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # cannot write into read-only subvolume
        with self.assertRaises(CommandFailedError):
            guest_mount.write_n_mb("rogue.bin", 1)

        # cleanup
        guest_mount.umount_wait()
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid,
                     "--group_name", group)
        # guest authID should no longer exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertNotIn("client.{0}".format(authid), existing_ids)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_deauthorize_subvolume(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid = "alice"

        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--mode=777")

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        mount_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume,
                                  "--group_name", group).rstrip()

        # authorize guest authID read-write access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id")

        # guest authID should exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(authid), existing_ids)

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, authid, key)

        # mount the subvolume, and write to it
        guest_mount.mount_wait(cephfs_mntpt=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # authorize guest authID read access to subvolume
        key = self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid,
                           "--group_name", group, "--tenant_id", "tenant_id", "--access_level", "r")

        # guest client sees the change in access level to read only after a
        # remount of the subvolume.
        guest_mount.umount_wait()
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # read existing content of the subvolume
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # cannot write into read-only subvolume
        with self.assertRaises(CommandFailedError):
            guest_mount.write_n_mb("rogue.bin", 1)

        # cleanup
        guest_mount.umount_wait()
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid,
                     "--group_name", group)
        # guest authID should no longer exist
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertNotIn("client.{0}".format(authid), existing_ids)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_multitenant_subvolumes(self):
        """
        That subvolume access can be restricted to a tenant.

        That metadata used to enforce tenant isolation of
        subvolumes is stored as a two-way mapping between auth
        IDs and subvolumes that they're authorized to access.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        guest_mount = self.mount_b

        # Guest clients belonging to different tenants, but using the same
        # auth ID.
        auth_id = "alice"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }
        guestclient_2 = {
            "auth_id": auth_id,
            "tenant_id": "tenant2",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Check that subvolume metadata file is created on subvolume creation.
        subvol_metadata_filename = "_{0}:{1}.meta".format(group, subvolume)
        self.assertIn(subvol_metadata_filename, guest_mount.ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'alice' and belonging to
        # 'tenant1', with 'rw' access to the volume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'alice', is
        # created on authorizing 'alice' access to the subvolume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different subvolumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Verify that the subvolume metadata file stores info about auth IDs
        # and their access levels to the subvolume, versioning details, etc.
        expected_subvol_metadata = {
            "version": 1,
            "compat_version": 1,
            "auths": {
                "alice": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }
        subvol_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(subvol_metadata_filename)))

        self.assertGreaterEqual(subvol_metadata["version"], expected_subvol_metadata["version"])
        del expected_subvol_metadata["version"]
        del subvol_metadata["version"]
        self.assertEqual(expected_subvol_metadata, subvol_metadata)

        # Cannot authorize 'guestclient_2' to access the volume.
        # It uses auth ID 'alice', which has already been used by a
        # 'guestclient_1' belonging to an another tenant for accessing
        # the volume.

        try:
            self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_2["auth_id"],
                         "--group_name", group, "--tenant_id", guestclient_2["tenant_id"])
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "Invalid error code returned on authorize of subvolume with same auth_id but different tenant_id")
        else:
            self.fail("expected the 'fs subvolume authorize' command to fail")

        # Check that auth metadata file is cleaned up on removing
        # auth ID's only access to a volume.

        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id,
                     "--group_name", group)
        self.assertNotIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Check that subvolume metadata file is cleaned up on subvolume deletion.
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self.assertNotIn(subvol_metadata_filename, guest_mount.ls("volumes"))

        # clean up
        guest_mount.umount_wait()
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_authorized_list(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        authid1 = "alice"
        authid2 = "guest1"
        authid3 = "guest2"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # authorize alice authID read-write access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid1,
                     "--group_name", group)
        # authorize guest1 authID read-write access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid2,
                     "--group_name", group)
        # authorize guest2 authID read access to subvolume
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, authid3,
                     "--group_name", group, "--access_level", "r")

        # list authorized-ids of the subvolume
        expected_auth_list = [{'alice': 'rw'}, {'guest1': 'rw'}, {'guest2': 'r'}]
        auth_list = json.loads(self._fs_cmd('subvolume', 'authorized_list', self.volname, subvolume, "--group_name", group))
        self.assertCountEqual(expected_auth_list, auth_list)

        # cleanup
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid1,
                     "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid2,
                     "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, authid3,
                     "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_auth_id_not_created_by_mgr_volumes(self):
        """
        If the auth_id already exists and is not created by mgr plugin,
        it's not allowed to authorize the auth-id by default.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # Create auth_id
        self.run_ceph_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        try:
            self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                         "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                             "Invalid error code returned on authorize of subvolume for auth_id created out of band")
        else:
            self.fail("expected the 'fs subvolume authorize' command to fail")

        # clean up
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_authorize_allow_existing_id_option(self):
        """
        If the auth_id already exists and is not created by mgr volumes,
        it's not allowed to authorize the auth-id by default but is
        allowed with option allow_existing_id.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # Create auth_id
        self.run_ceph_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Cannot authorize 'guestclient_1' to access the volume by default,
        # which already exists and not created by mgr volumes but is allowed
        # with option 'allow_existing_id'.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"], "--allow-existing-id")

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id,
                     "--group_name", group)
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_deauthorize_auth_id_after_out_of_band_update(self):
        """
        If the auth_id authorized by mgr/volumes plugin is updated
        out of band, the auth_id should not be deleted after a
        deauthorize. It should only remove caps associated with it.
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        subvol_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolume,
                                  "--group_name", group).rstrip()

        # Update caps for guestclient_1 out of band
        out = self.get_ceph_cmd_stdout(
            "auth", "caps", "client.guest1",
            "mds", "allow rw path=/volumes/{0}, allow rw path={1}".format(group, subvol_path),
            "osd", "allow rw pool=cephfs_data",
            "mon", "allow r",
            "mgr", "allow *"
        )

        # Deauthorize guestclient_1
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id, "--group_name", group)

        # Validate the caps of guestclient_1 after deauthorize. It should not have deleted
        # guestclient_1. The mgr and mds caps should be present which was updated out of band.
        out = json.loads(self.get_ceph_cmd_stdout("auth", "get", "client.guest1", "--format=json-pretty"))

        self.assertEqual("client.guest1", out[0]["entity"])
        self.assertEqual("allow rw path=/volumes/{0}".format(group), out[0]["caps"]["mds"])
        self.assertEqual("allow *", out[0]["caps"]["mgr"])
        self.assertNotIn("osd", out[0]["caps"])

        # clean up
        out = self.get_ceph_cmd_stdout("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_recover_auth_metadata_during_authorize(self):
        """
        That auth metadata manager can recover from partial auth updates using
        metadata files, which store auth info and its update status info. This
        test validates the recovery during authorize.
        """

        guest_mount = self.mount_b

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))
        expected_auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run authorize again.
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/false/true/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Authorize 'guestclient_1' to access the subvolume.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertEqual(auth_metadata_content, expected_auth_metadata_content)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_recover_auth_metadata_during_deauthorize(self):
        """
        That auth metadata manager can recover from partial auth updates using
        metadata files, which store auth info and its update status info. This
        test validates the recovery during deauthorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        guestclient_1 = {
            "auth_id": "guest1",
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume1.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))
        expected_auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))

        # Authorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run de-authorize.
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/false/true/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Deauthorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group)

        auth_metadata_content = self._auth_metadata_get(self.mount_a.read_file("volumes/{0}".format(auth_metadata_filename)))
        self.assertEqual(auth_metadata_content, expected_auth_metadata_content)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, "guest1", "--group_name", group)
        guest_mount.umount_wait()
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_update_old_style_auth_metadata_to_new_during_authorize(self):
        """
        CephVolumeClient stores the subvolume data in auth metadata file with
        'volumes' key as there was no subvolume namespace. It doesn't makes sense
        with mgr/volumes. This test validates the transparent update of 'volumes'
        key to 'subvolumes' key in auth metadata file during authorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is
        # created on authorizing 'guest1' access to the subvolume1.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Authorize 'guestclient_1' to access the subvolume2. This should transparently update 'volumes' to 'subvolumes'
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume1): {
                    "dirty": False,
                    "access_level": "rw"
                },
                "{0}/{1}".format(group,subvolume2): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, auth_id, "--group_name", group)
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_update_old_style_auth_metadata_to_new_during_deauthorize(self):
        """
        CephVolumeClient stores the subvolume data in auth metadata file with
        'volumes' key as there was no subvolume namespace. It doesn't makes sense
        with mgr/volumes. This test validates the transparent update of 'volumes'
        key to 'subvolumes' key in auth metadata file during deauthorize.
        """

        guest_mount = self.mount_b

        subvolume1, subvolume2 = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolumes in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--group_name", group)

        # Authorize 'guestclient_1' to access the subvolume1.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume1, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Authorize 'guestclient_1' to access the subvolume2.
        self._fs_cmd("subvolume", "authorize", self.volname, subvolume2, guestclient_1["auth_id"],
                     "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

        # Check that auth metadata file for auth ID 'guest1', is created.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, guest_mount.ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        guest_mount.run_shell(['sudo', 'sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)], omit_sudo=False)

        # Deauthorize 'guestclient_1' to access the subvolume2. This should update 'volumes' to subvolumes'
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume2, auth_id, "--group_name", group)

        expected_auth_metadata = {
            "version": 5,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "{0}/{1}".format(group,subvolume1): {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._auth_metadata_get(guest_mount.read_file("volumes/{0}".format(auth_metadata_filename)))

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # clean up
        self._fs_cmd("subvolume", "deauthorize", self.volname, subvolume1, auth_id, "--group_name", group)
        guest_mount.umount_wait()
        self.run_ceph_cmd("auth", "rm", "client.guest1")
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1, "--group_name", group)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


class TestResize:

    def test_subvolume_resize_fail_invalid_size(self):
        """
        That a subvolume cannot be resized to an invalid size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # try to resize the subvolume with an invalid size -10
        nsize = -10
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_fail_zero_size(self):
        """
        That a subvolume cannot be resized to a zero size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # try to resize the subvolume with size 0
        nsize = 0
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_quota_lt_used_size(self):
        """
        That a subvolume can be resized to a size smaller than the current used size
        and the resulting quota matches the expected size.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+1)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(subvolpath, "ceph.dir.rbytes"))
        susedsize = int(self.mount_a.run_shell(['stat', '-c' '%s', subvolpath]).stdout.getvalue().strip())
        if isinstance(self.mount_a, FuseMount):
            # kclient dir does not have size==rbytes
            self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
        except CommandFailedError:
            self.fail("expected the 'fs subvolume resize' command to succeed")

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_fail_quota_lt_used_size_no_shrink(self):
        """
        That a subvolume cannot be resized to a size smaller than the current used size
        when --no_shrink is given and the quota did not change.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+2)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(subvolpath, "ceph.dir.rbytes"))
        susedsize = int(self.mount_a.run_shell(['stat', '-c' '%s', subvolpath]).stdout.getvalue().strip())
        if isinstance(self.mount_a, FuseMount):
            # kclient dir does not have size==rbytes
            self.assertEqual(usedsize, susedsize)

        # shrink the subvolume
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize), "--no_shrink")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolume with invalid size")
        else:
            self.fail("expected the 'fs subvolume resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_expand_on_full_subvolume(self):
        """
        That the subvolume can be expanded from a full subvolume and future writes succeed.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*10
        # create subvolume of quota 10MB and make sure it exists
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of size 10MB and write
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+3)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        # create a file of size 5MB and try write more
        file_size=file_size // 2
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+4)
        try:
            self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
        except CommandFailedError:
            # Not able to write. So expand the subvolume more and try writing the 5MB file again
            nsize = osize*2
            self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))
            try:
                self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with {1} file of size {2}MB"
                                   "to succeed".format(subvolname, number_of_files, file_size))
        else:
            self.fail("expected filling subvolume {0} with {1} file of size {2}MB"
                               "to fail".format(subvolname, number_of_files, file_size))

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_infinite_size(self):
        """
        That a subvolume can be resized to an infinite size by unsetting its quota.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size",
                     str(self.DEFAULT_FILE_SIZE*1024*1024))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # resize inf
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_resize_infinite_size_future_writes(self):
        """
        That a subvolume can be resized to an infinite size and the future writes succeed.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size",
                     str(self.DEFAULT_FILE_SIZE*1024*1024*5), "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # resize inf
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # create one file of 10MB and try to write
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+5)

        try:
            self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)
        except CommandFailedError:
            self.fail("expected filling subvolume {0} with {1} file of size {2}MB "
                               "to succeed".format(subvolname, number_of_files, file_size))

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_expand(self):
        """
        That a subvolume can be expanded in size and its quota matches the
        expected size.
        """
        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # expand the subvolume
        nsize = osize*2
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_shrink(self):
        """
        That a subvolume can be shrinked in size and its quota matches the expected size.
        """

        # create subvolume
        subvolname = self._generate_random_subvolume_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize))

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # shrink the subvolume
        nsize = osize // 2
        self._fs_cmd("subvolume", "resize", self.volname, subvolname, str(nsize))

        # verify the quota
        size = int(self.mount_a.getfattr(subvolpath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestExist:

    def test_subvolume_exists_with_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolume by specifying the name of subvolumegroup"""

        group = self._generate_random_group_name()
        subvolume1 = self._generate_random_subvolume_name()
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

        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        ret = self._fs_cmd("subvolume", "exist", self.volname, "--group_name", group)
        self.assertEqual(ret.strip('\n'), "no subvolume exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_exists_without_subvolumegroup_and_with_subvolume(self):
        """Test the presence of any subvolume without specifying the name
            of subvolumegroup"""

        subvolume1 = self._generate_random_subvolume_name()
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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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


class TestMetadata:

    def test_subvolume_user_metadata_set_idempotence(self):
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

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


class TestPin:

    def test_subvolume_pin_export(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        subvolume = self._generate_random_subvolume_name()
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

        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "pin", self.volname, subvolume, "random", ".01")
        # no verification

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestMisc:

    def test_subvolume_info(self):
        # tests the 'fs subvolume info' command

        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features", "state"]

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # get subvolume metadata
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should be set to undefined if quota is not set")
        self.assertEqual(subvol_info["bytes_quota"], "infinite", "bytes_quota should be set to infinite if quota is not set")
        self.assertEqual(subvol_info["pool_namespace"], "", "expected pool namespace to be empty")
        self.assertEqual(subvol_info["state"], "complete", "expected state to be complete")

        self.assertEqual(len(subvol_info["features"]), 3,
                         msg="expected 3 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect', 'snapshot-retention']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))

        # get subvolume metadata after quota set
        subvol_info = json.loads(self._get_subvolume_info(self.volname, subvolume))
        for md in subvol_md:
            self.assertIn(md, subvol_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertNotEqual(subvol_info["bytes_pcent"], "undefined", "bytes_pcent should not be set to undefined if quota is not set")
        self.assertEqual(subvol_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))
        self.assertEqual(subvol_info["type"], "subvolume", "type should be set to subvolume")
        self.assertEqual(subvol_info["state"], "complete", "expected state to be complete")

        self.assertEqual(len(subvol_info["features"]), 3,
                         msg="expected 3 features, found '{0}' ({1})".format(len(subvol_info["features"]), subvol_info["features"]))
        for feature in ['snapshot-clone', 'snapshot-autoprotect', 'snapshot-retention']:
            self.assertIn(feature, subvol_info["features"], msg="expected feature '{0}' in subvolume".format(feature))

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_marked(self):
        """
        ensure a subvolume is marked with the ceph.dir.subvolume xattr
        """
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # getpath
        subvolpath = self._get_subvolume_path(self.volname, subvolume)

        # subdirectory of a subvolume cannot be moved outside the subvolume once marked with
        # the xattr ceph.dir.subvolume, hence test by attempting to rename subvol path (incarnation)
        # outside the subvolume
        dstpath = os.path.join(self.mount_a.mountpoint, 'volumes', '_nogroup', 'new_subvol_location')
        srcpath = os.path.join(self.mount_a.mountpoint, subvolpath)
        rename_script = dedent("""
            import os
            import errno
            try:
                os.rename("{src}", "{dst}")
            except OSError as e:
                if e.errno != errno.EXDEV:
                    raise RuntimeError("invalid error code on renaming subvolume incarnation out of subvolume directory")
            else:
                raise RuntimeError("expected renaming subvolume incarnation out of subvolume directory to fail")
            """)
        self.mount_a.run_python(rename_script.format(src=srcpath, dst=dstpath), sudo=True)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_evict_client(self):
        """
        That a subvolume client can be evicted based on the auth ID
        """

        subvolumes = self._generate_random_subvolume_name(2)
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # mounts[0] and mounts[1] would be used as guests to mount the volumes/shares.
        for i in range(0, 2):
            self.mounts[i].umount_wait()
        guest_mounts = (self.mounts[0], self.mounts[1])
        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create two subvolumes. Authorize 'guest' auth ID to mount the two
        # subvolumes. Mount the two subvolumes. Write data to the volumes.
        for i in range(2):
            # Create subvolume.
            self._fs_cmd("subvolume", "create", self.volname, subvolumes[i], "--group_name", group, "--mode=777")

            # authorize guest authID read-write access to subvolume
            key = self._fs_cmd("subvolume", "authorize", self.volname, subvolumes[i], guestclient_1["auth_id"],
                               "--group_name", group, "--tenant_id", guestclient_1["tenant_id"])

            mount_path = self._fs_cmd("subvolume", "getpath", self.volname, subvolumes[i],
                                      "--group_name", group).rstrip()
            # configure credentials for guest client
            self._configure_guest_auth(guest_mounts[i], auth_id, key)

            # mount the subvolume, and write to it
            guest_mounts[i].mount_wait(cephfs_mntpt=mount_path)
            guest_mounts[i].write_n_mb("data.bin", 1)

        # Evict client, guest_mounts[0], using auth ID 'guest' and has mounted
        # one volume.
        self._fs_cmd("subvolume", "evict", self.volname, subvolumes[0], auth_id, "--group_name", group)

        # Evicted guest client, guest_mounts[0], should not be able to do
        # anymore metadata ops.  It should start failing all operations
        # when it sees that its own address is in the blocklist.
        try:
            guest_mounts[0].write_n_mb("rogue.bin", 1)
        except CommandFailedError:
            pass
        else:
            raise RuntimeError("post-eviction write should have failed!")

        # The blocklisted guest client should now be unmountable
        guest_mounts[0].umount_wait()

        # Guest client, guest_mounts[1], using the same auth ID 'guest', but
        # has mounted the other volume, should be able to use its volume
        # unaffected.
        guest_mounts[1].write_n_mb("data.bin.1", 1)

        # Cleanup.
        guest_mounts[1].umount_wait()
        for i in range(2):
            self._fs_cmd("subvolume", "deauthorize", self.volname, subvolumes[i], auth_id, "--group_name", group)
            self._fs_cmd("subvolume", "rm", self.volname, subvolumes[i], "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


