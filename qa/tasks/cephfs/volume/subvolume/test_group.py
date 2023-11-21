class TestSubvolumeGroups(TestVolumesHelper):
    """Tests for FS subvolume group operations."""
    def test_default_uid_gid_subvolume_group(self):
        group = self._generate_random_group_name()
        expected_uid = 0
        expected_gid = 0

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        group_path = self._get_subvolume_group_path(self.volname, group)

        # check group's uid and gid
        stat = self.mount_a.stat(group_path)
        self.assertEqual(stat['st_uid'], expected_uid)
        self.assertEqual(stat['st_gid'], expected_gid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_nonexistent_subvolume_group_create(self):
        subvolume = self._generate_random_subvolume_name()
        group = "non_existent_group"

        # try, creating subvolume in a nonexistent group
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume create' command to fail")

    def test_nonexistent_subvolume_group_rm(self):
        group = "non_existent_group"

        # try, remove subvolume group
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup rm' command to fail")

    def test_subvolume_group_create_with_auto_cleanup_on_fail(self):
        group = self._generate_random_group_name()
        data_pool = "invalid_pool"
        # create group with invalid data pool layout
        with self.assertRaises(CommandFailedError):
            self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", data_pool)

        # check whether group path is cleaned up
        try:
            self._fs_cmd("subvolumegroup", "getpath", self.volname, group)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup getpath' command to fail")

    def test_subvolume_group_create_with_desired_data_pool_layout(self):
        group1, group2 = self._generate_random_group_name(2)

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)
        group1_path = self._get_subvolume_group_path(self.volname, group1)

        default_pool = self.mount_a.getfattr(group1_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # create group specifying the new data pool as its pool layout
        self._fs_cmd("subvolumegroup", "create", self.volname, group2,
                     "--pool_layout", new_pool)
        group2_path = self._get_subvolume_group_path(self.volname, group2)

        desired_pool = self.mount_a.getfattr(group2_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        self._fs_cmd("subvolumegroup", "rm", self.volname, group1)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group2)

    def test_subvolume_group_create_with_desired_mode(self):
        group1, group2 = self._generate_random_group_name(2)
        # default mode
        expected_mode1 = "755"
        # desired mode
        expected_mode2 = "777"

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group2, f"--mode={expected_mode2}")
        self._fs_cmd("subvolumegroup", "create", self.volname, group1)

        group1_path = self._get_subvolume_group_path(self.volname, group1)
        group2_path = self._get_subvolume_group_path(self.volname, group2)
        volumes_path = os.path.dirname(group1_path)

        # check group's mode
        actual_mode1 = self.mount_a.run_shell(['stat', '-c' '%a', group1_path]).stdout.getvalue().strip()
        actual_mode2 = self.mount_a.run_shell(['stat', '-c' '%a', group2_path]).stdout.getvalue().strip()
        actual_mode3 = self.mount_a.run_shell(['stat', '-c' '%a', volumes_path]).stdout.getvalue().strip()
        self.assertEqual(actual_mode1, expected_mode1)
        self.assertEqual(actual_mode2, expected_mode2)
        self.assertEqual(actual_mode3, expected_mode1)

        self._fs_cmd("subvolumegroup", "rm", self.volname, group1)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group2)

    def test_subvolume_group_create_with_desired_uid_gid(self):
        """
        That the subvolume group can be created with the desired uid and gid and its uid and gid matches the
        expected values.
        """
        uid = 1000
        gid = 1000

        # create subvolume group
        subvolgroupname = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, subvolgroupname, "--uid", str(uid), "--gid", str(gid))

        # make sure it exists
        subvolgrouppath = self._get_subvolume_group_path(self.volname, subvolgroupname)
        self.assertNotEqual(subvolgrouppath, None)

        # verify the uid and gid
        suid = int(self.mount_a.run_shell(['stat', '-c' '%u', subvolgrouppath]).stdout.getvalue().strip())
        sgid = int(self.mount_a.run_shell(['stat', '-c' '%g', subvolgrouppath]).stdout.getvalue().strip())
        self.assertEqual(uid, suid)
        self.assertEqual(gid, sgid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, subvolgroupname)

    def test_subvolume_group_create_with_invalid_data_pool_layout(self):
        group = self._generate_random_group_name()
        data_pool = "invalid_pool"
        # create group with invalid data pool layout
        try:
            self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolumegroup create' command to fail")

    def test_subvolume_group_create_with_size(self):
        # create group with size -- should set quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        self.assertEqual(group_info["bytes_quota"], 1000000000)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_info(self):
        # tests the 'fs subvolumegroup info' command

        group_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "uid"]

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        for md in group_md:
            self.assertIn(md, group_info, "'{0}' key not present in metadata of group".format(md))

        self.assertEqual(group_info["bytes_pcent"], "undefined", "bytes_pcent should be set to undefined if quota is not set")
        self.assertEqual(group_info["bytes_quota"], "infinite", "bytes_quota should be set to infinite if quota is not set")
        self.assertEqual(group_info["uid"], 0)
        self.assertEqual(group_info["gid"], 0)

        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))

        # get group metadata after quota set
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        for md in group_md:
            self.assertIn(md, group_info, "'{0}' key not present in metadata of subvolume".format(md))

        self.assertNotEqual(group_info["bytes_pcent"], "undefined", "bytes_pcent should not be set to undefined if quota is set")
        self.assertEqual(group_info["bytes_quota"], nsize, "bytes_quota should be set to '{0}'".format(nsize))

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name -- should be idempotent
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_mode(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name with mode -- should set mode
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--mode=766")

        group_path = self._get_subvolume_group_path(self.volname, group)

        # check subvolumegroup's  mode
        mode = self.mount_a.run_shell(['stat', '-c' '%a', group_path]).stdout.getvalue().strip()
        self.assertEqual(mode, "766")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_uid_gid(self):
        desired_uid = 1000
        desired_gid = 1000

        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume group name with uid/gid -- should set uid/gid
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--uid", str(desired_uid), "--gid", str(desired_gid))

        group_path = self._get_subvolume_group_path(self.volname, group)

        # verify the uid and gid
        actual_uid = int(self.mount_a.run_shell(['stat', '-c' '%u', group_path]).stdout.getvalue().strip())
        actual_gid = int(self.mount_a.run_shell(['stat', '-c' '%g', group_path]).stdout.getvalue().strip())
        self.assertEqual(desired_uid, actual_uid)
        self.assertEqual(desired_gid, actual_gid)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_data_pool(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        group_path = self._get_subvolume_group_path(self.volname, group)

        default_pool = self.mount_a.getfattr(group_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)

        # add data pool
        newid = self.fs.add_data_pool(new_pool)

        # try creating w/ same subvolume group name with new data pool -- should set pool
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--pool_layout", new_pool)
        desired_pool = self.mount_a.getfattr(group_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_create_idempotence_resize(self):
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # try creating w/ same subvolume name with size -- should set quota
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # get group metadata
        group_info = json.loads(self._get_subvolume_group_info(self.volname, group))
        self.assertEqual(group_info["bytes_quota"], 1000000000)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_quota_mds_path_restriction_to_group_path(self):
        """
        Tests subvolumegroup quota enforcement with mds path restriction set to group.
        For quota to be enforced, read permission needs to be provided to the parent
        of the directory on which quota is set. Please see the tracker comment [1]
        [1] https://tracker.ceph.com/issues/55090#note-8
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # Create auth_id
        authid = "client.guest1"
        user = json.loads(self.get_ceph_cmd_stdout(
            "auth", "get-or-create", authid,
            "mds", "allow rw path=/volumes",
            "mgr", "allow rw",
            "osd", "allow rw tag cephfs *=*",
            "mon", "allow r",
            "--format=json-pretty"
            ))

        # Prepare guest_mount with new authid
        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, "guest1", user[0]["key"])

        # mount the subvolume
        mount_path = os.path.join("/", subvolpath)
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # create 99 files of 1MB
        guest_mount.run_shell_payload("mkdir -p dir1")
        for i in range(99):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            guest_mount.write_n_mb(os.path.join("dir1", filename), self.DEFAULT_FILE_SIZE)
        try:
            # write two files of 1MB file to exceed the quota
            guest_mount.run_shell_payload("mkdir -p dir2")
            for i in range(2):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
            # For quota to be enforced
            time.sleep(60)
            # create 400 files of 1MB to exceed quota
            for i in range(400):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
                # Sometimes quota enforcement takes time.
                if i == 200:
                    time.sleep(60)
        except CommandFailedError:
            pass
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # clean up
        guest_mount.umount_wait()

        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_mds_path_restriction_to_subvolume_path(self):
        """
        Tests subvolumegroup quota enforcement with mds path restriction set to subvolume path
        The quota should not be enforced because of the fourth limitation mentioned at
        https://docs.ceph.com/en/latest/cephfs/quota/#limitations
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        mount_path = os.path.join("/", subvolpath)

        # Create auth_id
        authid = "client.guest1"
        user = json.loads(self.get_ceph_cmd_stdout(
            "auth", "get-or-create", authid,
            "mds", f"allow rw path={mount_path}",
            "mgr", "allow rw",
            "osd", "allow rw tag cephfs *=*",
            "mon", "allow r",
            "--format=json-pretty"
            ))

        # Prepare guest_mount with new authid
        guest_mount = self.mount_b
        guest_mount.umount_wait()

        # configure credentials for guest client
        self._configure_guest_auth(guest_mount, "guest1", user[0]["key"])

        # mount the subvolume
        guest_mount.mount_wait(cephfs_mntpt=mount_path)

        # create 99 files of 1MB to exceed quota
        guest_mount.run_shell_payload("mkdir -p dir1")
        for i in range(99):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            guest_mount.write_n_mb(os.path.join("dir1", filename), self.DEFAULT_FILE_SIZE)
        try:
            # write two files of 1MB file to exceed the quota
            guest_mount.run_shell_payload("mkdir -p dir2")
            for i in range(2):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
            # For quota to be enforced
            time.sleep(60)
            # create 400 files of 1MB to exceed quota
            for i in range(400):
                filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
                guest_mount.write_n_mb(os.path.join("dir2", filename), self.DEFAULT_FILE_SIZE)
                # Sometimes quota enforcement takes time.
                if i == 200:
                    time.sleep(60)
        except CommandFailedError:
            self.fail(f"Quota should not be enforced, expected filling subvolume {subvolname} with 400 files of size 1MB to succeed")

        # clean up
        guest_mount.umount_wait()

        # Delete the subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_exceeded_subvolume_removal(self):
        """
        Tests subvolume removal if it's group quota is exceeded
        """
        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB to exceed quota
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 400 files of 1MB to exceed quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=400)
        except CommandFailedError:
            # Delete subvolume when group quota is exceeded
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_exceeded_subvolume_removal_retained_snaps(self):
        """
        Tests retained snapshot subvolume removal if it's group quota is exceeded
        """
        group = self._generate_random_group_name()
        subvolname = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)

        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB to exceed quota
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot1, "--group_name", group)
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolname, snapshot2, "--group_name", group)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced. Just wait for 100 seconds because the worst case in kclient the
            # dirty caps will be held for 60 seconds, and also the MDS may defer updating the dir rstat
            # for 5 seconds, which is per tick, maybe longer if need to wait for mdlog to flush.
            time.sleep(100)
            # create 400 files of 1MB to exceed quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir2', number_of_files=400)
        except CommandFailedError:
            # remove with snapshot retention
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group, "--retain-snapshots")
            # remove snapshot1
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot1, "--group_name", group)
            # remove snapshot2 (should remove volume)
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolname, snapshot2, "--group_name", group)
            # verify subvolume trash is clean
            self._wait_for_subvol_trash_empty(subvolname, group=group)
        else:
            self.fail(f"expected filling subvolume {subvolname} with 400 files of size 1MB to fail")

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_subvolume_removal(self):
        """
        Tests subvolume removal if it's group quota is set.
        """
        # create group with size -- should set quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--group_name", group)

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_legacy_subvolume_removal(self):
        """
        Tests legacy subvolume removal if it's group quota is set.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a old-fashioned subvolume -- in a custom group
        createpath1 = os.path.join(".", "volumes", group, subvolume)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath1], omit_sudo=False)

        # this would auto-upgrade on access without anyone noticing
        subvolpath1 = self._fs_cmd("subvolume", "getpath", self.volname, subvolume, "--group-name", group)
        self.assertNotEqual(subvolpath1, None)
        subvolpath1 = subvolpath1.rstrip() # remove "/" prefix and any trailing newline

        # and... the subvolume path returned should be what we created behind the scene
        self.assertEqual(createpath1[1:], subvolpath1)

        # Set subvolumegroup quota on idempotent subvolumegroup creation
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_quota_v1_subvolume_removal(self):
        """
        Tests v1 subvolume removal if it's group quota is set.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # emulate a v1 subvolume -- in a custom group
        self._create_v1_subvolume(subvolume, subvol_group=group, has_snapshot=False)

        # Set subvolumegroup quota on idempotent subvolumegroup creation
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "1000000000")

        # remove subvolume
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)
        except CommandFailedError:
            self.fail("expected the 'fs subvolume rm' command to succeed if group quota is set")

        # remove subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_fail_invalid_size(self):
        """
        That a subvolume group cannot be resized to an invalid size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group with 1MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # try to resize the subvolume with an invalid size -10
        nsize = -10
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on resize of subvolume group with invalid size")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_fail_zero_size(self):
        """
        That a subvolume group cannot be resized to a zero size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group with 1MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group, "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # try to resize the subvolume group with size 0
        nsize = 0
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on resize of subvolume group with invalid size")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_quota_lt_used_size(self):
        """
        That a subvolume group can be resized to a size smaller than the current used size
        and the resulting quota matches the expected size.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create group with 20MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
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

        # shrink the subvolume group
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
        except CommandFailedError:
            self.fail("expected the 'fs subvolumegroup resize' command to succeed")

        # verify the quota
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, nsize)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_fail_quota_lt_used_size_no_shrink(self):
        """
        That a subvolume group cannot be resized to a size smaller than the current used size
        when --no_shrink is given and the quota did not change.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        # create group with 20MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create one file of 10MB
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+2)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        usedsize = int(self.mount_a.getfattr(grouppath, "ceph.dir.rbytes"))

        # shrink the subvolume group
        nsize = usedsize // 2
        try:
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize), "--no_shrink")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on resize of subvolumegroup with quota less than used")
        else:
            self.fail("expected the 'fs subvolumegroup resize' command to fail")

        # verify the quota did not change
        size = int(self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes"))
        self.assertEqual(size, osize)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_expand_on_full_subvolume(self):
        """
        That the subvolume group can be expanded after it is full and future write succeed
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*100
        # create group with 100MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 99 files of 1MB
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=99)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 500 files of 1MB
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
        except CommandFailedError:
            # Not able to write. So expand the subvolumegroup more and try writing the files again
            nsize = osize*7
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, str(nsize))
            try:
                self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                          "to succeed".format(subvolname))
        else:
            self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                      "to fail".format(subvolname))

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_resize_infinite_size(self):
        """
        That a subvolume group can be resized to an infinite size by unsetting its quota.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create group
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize))

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # resize inf
        self._fs_cmd("subvolumegroup", "resize", self.volname, group, "inf")

        # verify that the quota is None
        size = self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_group_resize_infinite_size_future_writes(self):
        """
        That a subvolume group can be resized to an infinite size and the future writes succeed.
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024*5
        # create group with 5MB quota
        group = self._generate_random_group_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group,
                     "--size", str(osize), "--mode=777")

        # make sure it exists
        grouppath = self._get_subvolume_group_path(self.volname, group)
        self.assertNotEqual(grouppath, None)

        # create subvolume under the group
        subvolname = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname,
                     "--group_name", group, "--mode=777")

        # make sure it exists
        subvolpath = self._get_subvolume_path(self.volname, subvolname, group_name=group)
        self.assertNotEqual(subvolpath, None)

        # create 4 files of 1MB
        self._do_subvolume_io(subvolname, subvolume_group=group, number_of_files=4)

        try:
            # write two files of 1MB file to exceed the quota
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=2)
            # For quota to be enforced
            time.sleep(20)
            # create 500 files of 1MB
            self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
        except CommandFailedError:
            # Not able to write. So resize subvolumegroup to 'inf' and try writing the files again
            # resize inf
            self._fs_cmd("subvolumegroup", "resize", self.volname, group, "inf")
            try:
                self._do_subvolume_io(subvolname, subvolume_group=group, create_dir='dir1', number_of_files=500)
            except CommandFailedError:
                self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                          "to succeed".format(subvolname))
        else:
            self.fail("expected filling subvolume {0} with 500 files of size 1MB "
                      "to fail".format(subvolname))


        # verify that the quota is None
        size = self.mount_a.getfattr(grouppath, "ceph.quota.max_bytes")
        self.assertEqual(size, None)

        # remove subvolume and group
        self._fs_cmd("subvolume", "rm", self.volname, subvolname, "--group_name", group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_ls(self):
        # tests the 'fs subvolumegroup ls' command

        subvolumegroups = []

        #create subvolumegroups
        subvolumegroups = self._generate_random_group_name(3)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        if len(subvolumegroupls) == 0:
            raise RuntimeError("Expected the 'fs subvolumegroup ls' command to list the created subvolume groups")
        else:
            subvolgroupnames = [subvolumegroup['name'] for subvolumegroup in subvolumegroupls]
            if collections.Counter(subvolgroupnames) != collections.Counter(subvolumegroups):
                raise RuntimeError("Error creating or listing subvolume groups")

    def test_subvolume_group_ls_filter(self):
        # tests the 'fs subvolumegroup ls' command filters '_deleting' directory

        subvolumegroups = []

        #create subvolumegroup
        subvolumegroups = self._generate_random_group_name(3)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        # create subvolume and remove. This creates '_deleting' directory.
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        subvolgroupnames = [subvolumegroup['name'] for subvolumegroup in subvolumegroupls]
        if "_deleting" in subvolgroupnames:
            self.fail("Listing subvolume groups listed '_deleting' directory")

    def test_subvolume_group_ls_filter_internal_directories(self):
        # tests the 'fs subvolumegroup ls' command filters internal directories
        # eg: '_deleting', '_nogroup', '_index', "_legacy"

        subvolumegroups = self._generate_random_group_name(3)
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        #create subvolumegroups
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "create", self.volname, groupname)

        # create subvolume which will create '_nogroup' directory
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # create snapshot
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone snapshot which will create '_index' directory
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # wait for clone to complete
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume which will create '_deleting' directory
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # list subvolumegroups
        ret = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        self.assertEqual(len(ret), len(subvolumegroups))

        ret_list = [subvolumegroup['name'] for subvolumegroup in ret]
        self.assertEqual(len(ret_list), len(subvolumegroups))

        self.assertEqual(all(elem in subvolumegroups for elem in ret_list), True)

        # cleanup
        self._fs_cmd("subvolume", "rm", self.volname, clone)
        for groupname in subvolumegroups:
            self._fs_cmd("subvolumegroup", "rm", self.volname, groupname)

    def test_subvolume_group_ls_for_nonexistent_volume(self):
        # tests the 'fs subvolumegroup ls' command when /volume doesn't exist
        # prerequisite: we expect that the test volume is created and a subvolumegroup is NOT created

        # list subvolume groups
        subvolumegroupls = json.loads(self._fs_cmd('subvolumegroup', 'ls', self.volname))
        if len(subvolumegroupls) > 0:
            raise RuntimeError("Expected the 'fs subvolumegroup ls' command to output an empty list")

    def test_subvolumegroup_pin_distributed(self):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.config_set('mds', 'mds_export_ephemeral_distributed', True)

        group = "pinme"
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolumegroup", "pin", self.volname, group, "distributed", "True")
        subvolumes = self._generate_random_subvolume_name(50)
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        self._wait_distributed_subtrees(2 * 2, status=status, rank="all")

        # remove subvolumes
        for subvolume in subvolumes:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_group_rm_force(self):
        # test removing non-existing subvolume group with --force
        group = self._generate_random_group_name()
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolumegroup rm --force' command to succeed")

    def test_subvolume_group_exists_with_subvolumegroup_and_no_subvolume(self):
        """Test the presence of any subvolumegroup when only subvolumegroup is present"""

        group = self._generate_random_group_name()
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_with_no_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolumegroup when no subvolumegroup is present"""

        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_with_subvolumegroup_and_subvolume(self):
        """Test the presence of any subvolume when subvolumegroup
            and subvolume both are present"""

        group = self._generate_random_group_name()
        subvolume = self._generate_random_subvolume_name(2)
        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume[0], "--group_name", group)
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume[1])
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolume in group
        self._fs_cmd("subvolume", "rm", self.volname, subvolume[0], "--group_name", group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume[1])
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "subvolumegroup exists")
        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_exists_without_subvolumegroup_and_with_subvolume(self):
        """Test the presence of any subvolume when subvolume is present
            but no subvolumegroup is present"""

        subvolume = self._generate_random_subvolume_name()
        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        ret = self._fs_cmd("subvolumegroup", "exist", self.volname)
        self.assertEqual(ret.strip('\n'), "no subvolumegroup exists")

    def test_subvolume_group_rm_when_its_not_empty(self):
        group = self._generate_random_group_name()
        subvolume = self._generate_random_subvolume_name()

        # create subvolumegroup
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)
        # try, remove subvolume group
        try:
            self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on deleting "
                             "subvolumegroup when it is not empty")
        else:
            self.fail("expected the 'fs subvolumegroup rm' command to fail")
        
        # delete subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)

        # delete subvolumegroup
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()
