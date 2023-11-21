class TestSnapshots(TestVolumesHelper):
    """
    Tests for FS subvolume snapshot operations.
    """

    def test_nonexistent_subvolume_snapshot_rm(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove snapshot again
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise
        else:
            raise RuntimeError("expected the 'fs subvolume snapshot rm' command to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


    def test_nonexistent_snapshot_rm_force(self):
        """
        Test removing non existing subvolume snapshot with --force
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # remove snapshot
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, "--force")
        except CommandFailedError:
            raise RuntimeError("expected the 'fs subvolume snapshot rm --force' command to succeed")

    def test_subvolume_rm_with_snapshots(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove subvolume -- should fail with ENOTEMPTY since it has snapshots
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOTEMPTY:
                raise RuntimeError("invalid error code returned when deleting subvolume with snapshots")
        else:
            raise RuntimeError("expected subvolume deletion to fail")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestCreate:

    def test_subvolume_snapshot_create_and_rm(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_create_idempotence(self):
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # try creating w/ same subvolume snapshot name -- should be idempotent
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info(self):

        """
        tests the 'fs subvolume snapshot info' command
        """

        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot, snap_missing = self._generate_random_snapshot_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=1)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snapshot info for non-existent snapshot
        try:
            self._get_subvolume_snapshot_info(self.volname, subvolume, snap_missing)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on snapshot info of non-existent snapshot")
        else:
            self.fail("expected snapshot info of non-existent snapshot to fail")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_create_snapshot_in_subvolme_group(self):
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # snapshot subvolume in group
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


    def test_create_subvol_subvolgrp_snapshot_name_conflict(self):
        """
        tests the scenario where creation of subvolume snapshot name
        with same name as it's subvolumegroup snapshot name. This should
        fail.
        """
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        group_snapshot = self._generate_random_snapshot_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create subvolumegroup snapshot
        group_snapshot_path = os.path.join(".", "volumes", group, ".snap", group_snapshot)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', group_snapshot_path], omit_sudo=False)

        # Validate existence of subvolumegroup snapshot
        self.mount_a.run_shell(['ls', group_snapshot_path])

        # Creation of subvolume snapshot with it's subvolumegroup snapshot name should fail
        try:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, group_snapshot, "--group_name", group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, msg="invalid error code when creating subvolume snapshot with same name as subvolume group snapshot")
        else:
            self.fail("expected subvolume snapshot creation with same name as subvolumegroup snapshot to fail")

        # remove subvolumegroup snapshot
        self.mount_a.run_shell(['sudo', 'rmdir', group_snapshot_path], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


class TestInherited:

    def test_subvolume_inherited_snapshot_ls(self):
        # tests the scenario where 'fs subvolume snapshot ls' command
        # should not list inherited snapshots created as part of snapshot
        # at ancestral level

        snapshots = []
        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snap_count = 3

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # create subvolume snapshots
        snapshots = self._generate_random_snapshot_name(snap_count)
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # Create snapshot at ancestral level
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", "ancestral_snap_1")
        ancestral_snappath2 = os.path.join(".", "volumes", group, ".snap", "ancestral_snap_2")
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1, ancestral_snappath2], omit_sudo=False)

        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume, group))
        self.assertEqual(len(subvolsnapshotls), snap_count)

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1, ancestral_snappath2], omit_sudo=False)

        # remove snapshot
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_inherited_snapshot_info(self):
        """
        tests the scenario where 'fs subvolume snapshot info' command
        should fail for inherited snapshots created as part of snapshot
        at ancestral level
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create snapshot at ancestral level
        ancestral_snap_name = "ancestral_snap_1"
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", ancestral_snap_name)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1], omit_sudo=False)

        # Validate existence of inherited snapshot
        group_path = os.path.join(".", "volumes", group)
        inode_number_group_dir = int(self.mount_a.run_shell(['stat', '-c' '%i', group_path]).stdout.getvalue().strip())
        inherited_snap = "_{0}_{1}".format(ancestral_snap_name, inode_number_group_dir)
        inherited_snappath = os.path.join(".", "volumes", group, subvolume,".snap", inherited_snap)
        self.mount_a.run_shell(['ls', inherited_snappath])

        # snapshot info on inherited snapshot
        try:
            self._get_subvolume_snapshot_info(self.volname, subvolume, inherited_snap, group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on snapshot info of inherited snapshot")
        else:
            self.fail("expected snapshot info of inherited snapshot to fail")

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--group_name", group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

    def test_subvolume_inherited_snapshot_rm(self):
        """
        tests the scenario where 'fs subvolume snapshot rm' command
        should fail for inherited snapshots created as part of snapshot
        at ancestral level
        """

        subvolume = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume in group
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--group_name", group)

        # Create snapshot at ancestral level
        ancestral_snap_name = "ancestral_snap_1"
        ancestral_snappath1 = os.path.join(".", "volumes", group, ".snap", ancestral_snap_name)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', ancestral_snappath1], omit_sudo=False)

        # Validate existence of inherited snap
        group_path = os.path.join(".", "volumes", group)
        inode_number_group_dir = int(self.mount_a.run_shell(['stat', '-c' '%i', group_path]).stdout.getvalue().strip())
        inherited_snap = "_{0}_{1}".format(ancestral_snap_name, inode_number_group_dir)
        inherited_snappath = os.path.join(".", "volumes", group, subvolume,".snap", inherited_snap)
        self.mount_a.run_shell(['ls', inherited_snappath])

        # inherited snapshot should not be deletable
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, inherited_snap, "--group_name", group)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, msg="invalid error code when removing inherited snapshot")
        else:
            self.fail("expected removing inheirted snapshot to fail")

        # remove ancestral snapshots
        self.mount_a.run_shell(['sudo', 'rmdir', ancestral_snappath1], omit_sudo=False)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)


class TestRetain:

    def test_subvolume_retain_snapshot_invalid_recreate(self):
        """
        ensure retained subvolume recreate does not leave any incarnations in
        the subvolume and trash
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate subvolume with an invalid pool
        data_pool = "invalid_pool"
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume, "--pool_layout", data_pool)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL, "invalid error code on recreate of subvolume with invalid poolname")
        else:
            self.fail("expected recreate of subvolume with invalid poolname to fail")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # getpath
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of subvolume with retained snapshots")
        else:
            self.fail("expected getpath of subvolume with retained snapshots to fail")

        # remove snapshot (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_recreate_subvolume(self):
        """
        ensure a retained subvolume can be recreated and further snapshotted
        """
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot1, snapshot2 = self._generate_random_snapshot_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # recreate retained subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "complete",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        # snapshot info (older snapshot)
        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot1))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # snap-create (new snapshot)
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot2)

        # remove with retain snapshots
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # list snapshots
        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        self.assertEqual(len(subvolsnapshotls), 2, "Expected the 'fs subvolume snapshot ls' command to list the"
                         " created subvolume snapshots")
        snapshotnames = [snapshot['name'] for snapshot in subvolsnapshotls]
        for snap in [snapshot1, snapshot2]:
            self.assertIn(snap, snapshotnames, "Missing snapshot '{0}' in snapshot list".format(snap))

        # remove snapshots (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot2)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_with_snapshots(self):
        """
        ensure retain snapshots based delete of a subvolume with snapshots retains the subvolume
        also test allowed and dis-allowed operations on a retained subvolume
        """
        snap_md = ["created_at", "data_pool", "has_pending_clones"]

        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove subvolume -- should fail with ENOTEMPTY since it has snapshots
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of retained subvolume with snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fetch info
        subvol_info = json.loads(self._fs_cmd("subvolume", "info", self.volname, subvolume))
        self.assertEqual(subvol_info["state"], "snapshot-retained",
                         msg="expected state to be 'snapshot-retained', found '{0}".format(subvol_info["state"]))

        ## test allowed ops in retained state
        # ls
        subvolumes = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumes), 1, "subvolume ls count mismatch, expected '1', found {0}".format(len(subvolumes)))
        self.assertEqual(subvolumes[0]['name'], subvolume,
                         "subvolume name mismatch in ls output, expected '{0}', found '{1}'".format(subvolume, subvolumes[0]['name']))

        # snapshot info
        snap_info = json.loads(self._get_subvolume_snapshot_info(self.volname, subvolume, snapshot))
        for md in snap_md:
            self.assertIn(md, snap_info, "'{0}' key not present in metadata of snapshot".format(md))
        self.assertEqual(snap_info["has_pending_clones"], "no")

        # rm --force (allowed but should fail)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--force")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of subvolume with retained snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        # rm (allowed but should fail)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOTEMPTY, "invalid error code on rm of subvolume with retained snapshots")
        else:
            self.fail("expected rm of subvolume with retained snapshots to fail")

        ## test disallowed ops
        # getpath
        try:
            self._fs_cmd("subvolume", "getpath", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on getpath of subvolume with retained snapshots")
        else:
            self.fail("expected getpath of subvolume with retained snapshots to fail")

        # resize
        nsize = self.DEFAULT_FILE_SIZE*1024*1024
        try:
            self._fs_cmd("subvolume", "resize", self.volname, subvolume, str(nsize))
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on resize of subvolume with retained snapshots")
        else:
            self.fail("expected resize of subvolume with retained snapshots to fail")

        # snap-create
        try:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, "fail")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on snapshot create of subvolume with retained snapshots")
        else:
            self.fail("expected snapshot create of subvolume with retained snapshots to fail")

        # remove snapshot (should remove volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_without_snapshots(self):
        """
        ensure retain snapshots based delete of a subvolume with no snapshots, deletes the subbvolume
        """
        subvolume = self._generate_random_subvolume_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove with snapshot retention (should remove volume, no snapshots to retain)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_trash_busy_recreate(self):
        """
        ensure retained subvolume recreate fails if its trash is not yet purged
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # fake a trash entry
        self._update_fake_trash(subvolume)

        # recreate subvolume
        try:
            self._fs_cmd("subvolume", "create", self.volname, subvolume)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, "invalid error code on recreate of subvolume with purge pending")
        else:
            self.fail("expected recreate of subvolume with purge pending to fail")

        # clear fake trash entry
        self._update_fake_trash(subvolume, create=False)

        # recreate subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestMetadata:

    def test_subvolume_snapshot_metadata_set(self):
        """
        Set custom metadata for subvolume snapshot.
        """
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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
        subvolname = self._generate_random_subvolume_name()
        group = self._generate_random_group_name()
        snapshot = self._generate_random_snapshot_name()

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


class TestMisc:

    def test_subvolume_snapshot_ls(self):
        # tests the 'fs subvolume snapshot ls' command

        snapshots = []

        # create subvolume
        subvolume = self._generate_random_subvolume_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # create subvolume snapshots
        snapshots = self._generate_random_snapshot_name(3)
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        subvolsnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, subvolume))
        if len(subvolsnapshotls) == 0:
            self.fail("Expected the 'fs subvolume snapshot ls' command to list the created subvolume snapshots")
        else:
            snapshotnames = [snapshot['name'] for snapshot in subvolsnapshotls]
            if collections.Counter(snapshotnames) != collections.Counter(snapshots):
                self.fail("Error creating or listing subvolume snapshots")

        # remove snapshot
        for snapshot in snapshots:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_protect_unprotect_sanity(self):
        """
        Snapshot protect/unprotect commands are deprecated. This test exists to ensure that
        invoking the command does not cause errors, till they are removed from a subsequent release.
        """
        subvolume = self._generate_random_subvolume_name()
        snapshot = self._generate_random_snapshot_name()
        clone = self._generate_random_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # now, protect snapshot
        self._fs_cmd("subvolume", "snapshot", "protect", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # now, unprotect snapshot
        self._fs_cmd("subvolume", "snapshot", "unprotect", self.volname, subvolume, snapshot)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()


