import os
import errno
import json
import time

from io import StringIO
from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestInfo(SubvolumeHelper):

    def test_clone_subvolume_info(self):
        # tests the 'fs subvolume info' command for a clone
        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid"]

        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=1)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        subvol_info = json.loads(self._get_subvolume_info(self.volname, clone))
        if len(subvol_info) == 0:
            raise RuntimeError("Expected the 'fs subvolume info' command to list metadata of subvolume")
        for md in subvol_md:
            if md not in subvol_info.keys():
                raise RuntimeError("%s not present in the metadata of subvolume" % md)
        if subvol_info["type"] != "clone":
            raise RuntimeError("type should be set to clone")

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_without_snapshot_clone(self):
        """
        Verify subvolume snapshot info output without cloning snapshot.
        If no clone is performed then path /volumes/_index/clone/{track_id}
        will not exist.
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        self.assertEqual(result['has_pending_clones'], "no")
        self.assertFalse('orphan_clones_count' in result)
        self.assertFalse('pending_clones' in result)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_no_clone_pending(self):
        """
        Verify subvolume snapshot info output if no clone is in pending state.
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clones status
        for clone in clone_list:
            self._wait_for_clone_to_complete(clone)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        self.assertEqual(result['has_pending_clones'], "no")
        self.assertFalse('orphan_clones_count' in result)
        self.assertFalse('pending_clones' in result)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in clone_list:
            self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_clone_pending_for_no_group(self):
        """
        Verify subvolume snapshot info output if clones are in pending state.
        Clones are not specified for particular target_group. Hence target_group
        should not be in the output as we don't show _nogroup (default group)
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        expected_clone_list = []
        for clone in clone_list:
            expected_clone_list.append({"name": clone})
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertFalse('orphan_clones_count' in result)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 3)

        # check clones status
        for clone in clone_list:
            self._wait_for_clone_to_complete(clone)

        # remove snapshot, subvolume, clone
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in clone_list:
            self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_clone_pending_for_target_group(self):
        """
        Verify subvolume snapshot info output if clones are in pending state.
        Clones are not specified for target_group.
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()
        group = self._gen_subvol_grp_name()
        target_group = self._gen_subvol_grp_name()

        # create groups
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolumegroup", "create", self.volname, target_group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, group, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone,
                     "--group_name", group, "--target_group_name", target_group)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot, "--group_name", group))

        # verify snapshot info
        expected_clone_list = [{"name": clone, "target_group": target_group}]
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertFalse('orphan_clones_count' in result)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 1)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=target_group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)
        self._fs_cmd("subvolume", "rm", self.volname, clone, target_group)

        # remove groups
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, target_group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_info_if_orphan_clone(self):
        """
        Verify subvolume snapshot info output if orphan clones exists.
        Orphan clones should not list under pending clones.
        orphan_clones_count should display correct count of orphan clones'
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone_list =  [f'clone_{i}' for i in range(3)]

        # create subvolume.
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 15)

        # schedule a clones
        for clone in clone_list:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # remove track file for third clone to make it orphan
        meta_path = os.path.join(".", "volumes", "_nogroup", subvolume, ".meta")
        pending_clones_result = self.mount_a.run_shell(['sudo', 'grep', 'clone snaps', '-A3', meta_path], omit_sudo=False, stdout=StringIO(), stderr=StringIO())
        third_clone_track_id = pending_clones_result.stdout.getvalue().splitlines()[3].split(" = ")[0]
        third_clone_track_path = os.path.join(".", "volumes", "_index", "clone", third_clone_track_id)
        self.mount_a.run_shell(f"sudo rm -f {third_clone_track_path}", omit_sudo=False)

        # list snapshot info
        result = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info
        expected_clone_list = []
        for i in range(len(clone_list)-1):
            expected_clone_list.append({"name": clone_list[i]})
        self.assertEqual(result['has_pending_clones'], "yes")
        self.assertEqual(result['orphan_clones_count'], 1)
        self.assertListEqual(result['pending_clones'], expected_clone_list)
        self.assertEqual(len(result['pending_clones']), 2)

        # check clones status
        for i in range(len(clone_list)-1):
            self._wait_for_clone_to_complete(clone_list[i])

        # list snapshot info after cloning completion
        res = json.loads(self._fs_cmd("subvolume", "snapshot", "info", self.volname, subvolume, snapshot))

        # verify snapshot info (has_pending_clones should be no)
        self.assertEqual(res['has_pending_clones'], "no")


class TestStatus(SubvolumeHelper):

    def test_non_clone_status(self):
        subvolume = self._gen_subvol_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        try:
            self._fs_cmd("clone", "status", self.volname, subvolume)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOTSUP:
                raise RuntimeError("invalid error code when fetching status of a non cloned subvolume")
        else:
            raise RuntimeError("expected fetching of clone status of a subvolume to fail")

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestInherit(SubvolumeHelper):

    def test_subvolume_clone_inherit_snapshot_namespace_and_size(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024*12

        # create subvolume, in an isolated namespace with a specified size
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--namespace-isolated", "--size", str(osize), "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # create a pool different from current subvolume pool
        subvol_path = self._get_subvolume_path(self.volname, subvolume)
        default_pool = self.mount_a.getfattr(subvol_path, "ceph.dir.layout.pool")
        new_pool = "new_pool"
        self.assertNotEqual(default_pool, new_pool)
        self.fs.add_data_pool(new_pool)

        # update source subvolume pool
        self._do_subvolume_pool_and_namespace_update(subvolume, pool=new_pool, pool_namespace="")

        # schedule a clone, with NO --pool specification
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_inherit_quota_attrs(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()
        osize = self.DEFAULT_FILE_SIZE*1024*1024*12

        # create subvolume with a specified size
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777", "--size", str(osize))

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=8)

        # get subvolume path
        subvolpath = self._get_subvolume_path(self.volname, subvolume)

        # set quota on number of files
        self.mount_a.setfattr(subvolpath, 'ceph.quota.max_files', "20", sudo=True)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # get subvolume path
        clonepath = self._get_subvolume_path(self.volname, clone)

        # verify quota max_files is inherited from source snapshot
        subvol_quota = self.mount_a.getfattr(subvolpath, "ceph.quota.max_files")
        clone_quota = self.mount_a.getfattr(clonepath, "ceph.quota.max_files")
        self.assertEqual(subvol_quota, clone_quota)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestCloneInProgress(SubvolumeHelper):

    def test_subvolume_clone_in_progress_getpath(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # clone should not be accessible right now
        try:
            self._get_subvolume_path(self.volname, clone)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when fetching path of an pending clone")
        else:
            raise RuntimeError("expected fetching path of an pending clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_in_progress_snapshot_rm(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # snapshot should not be deletable now
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, msg="invalid error code when removing source snapshot of a clone")
        else:
            self.fail("expected removing source snapshot of a clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_clone_in_progress_source(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # verify clone source
        result = json.loads(self._fs_cmd("clone", "status", self.volname, clone))
        source = result['status']['source']
        self.assertEqual(source['volume'], self.volname)
        self.assertEqual(source['subvolume'], subvolume)
        self.assertEqual(source.get('group', None), None)
        self.assertEqual(source['snapshot'], snapshot)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # clone should be accessible now
        subvolpath = self._get_subvolume_path(self.volname, clone)
        self.assertNotEqual(subvolpath, None)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestRetain(SubvolumeHelper):

    def test_subvolume_clone_retain_snapshot_with_snapshots(self):
        """
        retain snapshots of a cloned subvolume and check disallowed operations
        """
        subvolume = self._gen_subvol_name()
        snapshot1, snapshot2 = self._gen_subvol_snap_name(2)
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol1_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot1, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot1, clone, subvol_path=subvol1_path)

        # create a snapshot on the clone
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone, snapshot2)

        # retain a clone
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--retain-snapshots")

        # list snapshots
        clonesnapshotls = json.loads(self._fs_cmd('subvolume', 'snapshot', 'ls', self.volname, clone))
        self.assertEqual(len(clonesnapshotls), 1, "Expected the 'fs subvolume snapshot ls' command to list the"
                         " created subvolume snapshots")
        snapshotnames = [snapshot['name'] for snapshot in clonesnapshotls]
        for snap in [snapshot2]:
            self.assertIn(snap, snapshotnames, "Missing snapshot '{0}' in snapshot list".format(snap))

        ## check disallowed operations on retained clone
        # clone-status
        try:
            self._fs_cmd("clone", "status", self.volname, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on clone status of clone with retained snapshots")
        else:
            self.fail("expected clone status of clone with retained snapshots to fail")

        # clone-cancel
        try:
            self._fs_cmd("clone", "cancel", self.volname, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on clone cancel of clone with retained snapshots")
        else:
            self.fail("expected clone cancel of clone with retained snapshots to fail")

        # remove snapshots (removes subvolumes as all are in retained state)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone, snapshot2)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_clone(self):
        """
        clone a snapshot from a snapshot retained subvolume
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, subvol_path=subvol_path)

        # remove snapshots (removes retained volume)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_clone_from_newer_snapshot(self):
        """
        clone a subvolume from recreated subvolume's latest snapshot
        """
        subvolume = self._gen_subvol_name()
        snapshot1, snapshot2 = self._gen_subvol_snap_name(2)
        clone = self._gen_subvol_clone_name(1)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot1)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # get and store path for clone verification
        subvol2_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot newer subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot2)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # clone retained subvolume's newer snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot2, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot2, clone, subvol_path=subvol2_path)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot1)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot2)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_recreate(self):
        """
        recreate a subvolume from one of its retained snapshots
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # store path for clone verification
        subvol_path = self._get_subvolume_path(self.volname, subvolume)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=16)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # remove with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, "--retain-snapshots")

        # recreate retained subvolume using its own snapshot to clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, subvolume)

        # check clone status
        self._wait_for_clone_to_complete(subvolume)

        # verify clone
        self._verify_clone(subvolume, snapshot, subvolume, subvol_path=subvol_path)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify list subvolumes returns an empty list
        subvolumels = json.loads(self._fs_cmd('subvolume', 'ls', self.volname))
        self.assertEqual(len(subvolumels), 0)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_retain_snapshot_trash_busy_recreate_clone(self):
        """
        ensure retained clone recreate fails if its trash is not yet purged
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # clone subvolume snapshot
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # snapshot clone
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone, snapshot)

        # remove clone with snapshot retention
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--retain-snapshots")

        # fake a trash entry
        self._update_fake_trash(clone)

        # clone subvolume snapshot (recreate)
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, "invalid error code on recreate of clone with purge pending")
        else:
            self.fail("expected recreate of clone with purge pending to fail")

        # clear fake trash entry
        self._update_fake_trash(clone, create=False)

        # recreate subvolume
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone, snapshot)

        # remove subvolume
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestCloneFailure(SubvolumeHelper):

    def test_clone_failure_status_pending_in_progress_complete(self):
        """
        ensure failure status is not shown when clone is not in failed/cancelled state
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1 = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # pending clone shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for pending clone")

        # check clone1 to be in-progress
        self._wait_for_clone_to_be_in_progress(clone1)

        # in-progress clone1 shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for in-progress clone")

        # wait for clone1 to complete
        self._wait_for_clone_to_complete(clone1)

        # complete clone1 shouldn't show failure status
        clone1_result = self._get_clone_status(clone1)
        try:
            clone1_result["status"]["failure"]["errno"]
        except KeyError as e:
            self.assertEqual(str(e), "'failure'")
        else:
            self.fail("clone status shouldn't show failure for complete clone")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)

        # verify trash dir is clean
        self._wait_for_trash_empty()


    def test_clone_failure_status_failed(self):
        """
        ensure failure status is shown when clone is in failed state and validate the reason
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1 = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # remove snapshot from backend to force the clone failure.
        snappath = os.path.join(".", "volumes", "_nogroup", subvolume, ".snap", snapshot)
        self.mount_a.run_shell(['sudo', 'rmdir', snappath], omit_sudo=False)

        # wait for clone1 to fail.
        self._wait_for_clone_to_fail(clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "failed")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "2")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "snapshot '{0}' does not exist".format(snapshot))

        # clone removal should succeed after failure, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_pending_cancelled(self):
        """
        ensure failure status is shown when clone is cancelled during pending state and validate the reason
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1 = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # cancel pending clone1
        self._fs_cmd("clone", "cancel", self.volname, clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "canceled")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "4")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "user interrupted clone operation")

        # clone removal should succeed with force after cancelled, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_clone_failure_status_in_progress_cancelled(self):
        """
        ensure failure status is shown when clone is cancelled during in-progress state and validate the reason
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1 = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=200)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 5)

        # schedule a clone1
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # wait for clone1 to be in-progress
        self._wait_for_clone_to_be_in_progress(clone1)

        # cancel in-progess clone1
        self._fs_cmd("clone", "cancel", self.volname, clone1)

        # check clone1 status
        clone1_result = self._get_clone_status(clone1)
        self.assertEqual(clone1_result["status"]["state"], "canceled")
        self.assertEqual(clone1_result["status"]["failure"]["errno"], "4")
        self.assertEqual(clone1_result["status"]["failure"]["error_msg"], "user interrupted clone operation")

        # clone removal should succeed with force after cancelled, remove clone1
        self._fs_cmd("subvolume", "rm", self.volname, clone1, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestCloneCancel(SubvolumeHelper):

    def test_subvolume_snapshot_clone_cancel_in_progress(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=128)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # cancel on-going clone
        self._fs_cmd("clone", "cancel", self.volname, clone)

        # verify canceled state
        self._check_clone_canceled(clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_cancel_pending(self):
        """
        this test is a bit more involved compared to canceling an in-progress clone.
        we'd need to ensure that a to-be canceled clone has still not been picked up
        by cloner threads. exploit the fact that clones are picked up in an FCFS
        fashion and there are four (4) cloner threads by default. When the number of
        cloner threads increase, this test _may_ start tripping -- so, the number of
        clone operations would need to be jacked up.
        """
        # default number of clone threads
        NR_THREADS = 4
        # good enough for 4 threads
        NR_CLONES = 5
        # yeh, 1gig -- we need the clone to run for sometime
        FILE_SIZE_MB = 1024

        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clones = self._gen_subvol_clone_name(NR_CLONES)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=4, file_size=FILE_SIZE_MB)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule clones
        for clone in clones:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        to_wait = clones[0:NR_THREADS]
        to_cancel = clones[NR_THREADS:]

        # cancel pending clones and verify
        for clone in to_cancel:
            status = json.loads(self._fs_cmd("clone", "status", self.volname, clone))
            self.assertEqual(status["status"]["state"], "pending")
            self._fs_cmd("clone", "cancel", self.volname, clone)
            self._check_clone_canceled(clone)

        # let's cancel on-going clones. handle the case where some of the clones
        # _just_ complete
        for clone in list(to_wait):
            try:
                self._fs_cmd("clone", "cancel", self.volname, clone)
                to_cancel.append(clone)
                to_wait.remove(clone)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EINVAL:
                    raise RuntimeError("invalid error code when cancelling on-going clone")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        for clone in to_wait:
            self._fs_cmd("subvolume", "rm", self.volname, clone)
        for clone in to_cancel:
            self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

class TestMisc(SubvolumeHelper):

    def test_subvolume_snapshot_attr_clone(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io_mixed(subvolume)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()


class TestMisc2(SubvolumeHelper):

    def test_subvolume_snapshot_clone(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_quota_exceeded(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume with 20MB quota
        osize = self.DEFAULT_FILE_SIZE*1024*1024*20
        self._fs_cmd("subvolume", "create", self.volname, subvolume,"--mode=777", "--size", str(osize))

        # do IO, write 50 files of 1MB each to exceed quota. This mostly succeeds as quota enforcement takes time.
        try:
            self._do_subvolume_io(subvolume, number_of_files=50)
        except CommandFailedError:
            # ignore quota enforcement error.
            pass

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_in_complete_clone_rm(self):
        """
        Validates the removal of clone when it is not in 'complete|cancelled|failed' state.
        The forceful removl of subvolume clone succeeds only if it's in any of the
        'complete|cancelled|failed' states. It fails with EAGAIN in any other states.
        """

        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # Use --force since clone is not complete. Returns EAGAIN as clone is not either complete or cancelled.
        try:
            self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when trying to remove failed clone")
        else:
            raise RuntimeError("expected error when removing a failed clone")

        # cancel on-going clone
        self._fs_cmd("clone", "cancel", self.volname, clone)

        # verify canceled state
        self._check_clone_canceled(clone)

        # clone removal should succeed after cancel
        self._fs_cmd("subvolume", "rm", self.volname, clone, "--force")

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_retain_suid_guid(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # Create a file with suid, guid bits set along with executable bit.
        args = ["subvolume", "getpath", self.volname, subvolume]
        args = tuple(args)
        subvolpath = self._fs_cmd(*args)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath[1:].rstrip() # remove "/" prefix and any trailing newline

        file_path = subvolpath
        file_path = os.path.join(subvolpath, "test_suid_file")
        self.mount_a.run_shell(["touch", file_path])
        self.mount_a.run_shell(["chmod", "u+sx,g+sx", file_path])

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_and_reclone(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1, clone2 = self._gen_subvol_clone_name(2)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1)

        # check clone status
        self._wait_for_clone_to_complete(clone1)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # now the clone is just like a normal subvolume -- snapshot the clone and fork
        # another clone. before that do some IO so it's can be differentiated.
        self._do_subvolume_io(clone1, create_dir="data", number_of_files=32)

        # snapshot clone -- use same snap name
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, clone1, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, clone1, snapshot, clone2)

        # check clone status
        self._wait_for_clone_to_complete(clone2)

        # verify clone
        self._verify_clone(clone1, snapshot, clone2)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, clone1, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        self._fs_cmd("subvolume", "rm", self.volname, clone2)

        # verify trash dir is clean
        self._wait_for_trash_empty()


    def test_subvolume_snapshot_clone_different_groups(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()
        s_group, c_group = self._gen_subvol_grp_name(2)

        # create groups
        self._fs_cmd("subvolumegroup", "create", self.volname, s_group)
        self._fs_cmd("subvolumegroup", "create", self.volname, c_group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, s_group, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=s_group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, s_group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone,
                     '--group_name', s_group, '--target_group_name', c_group)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=c_group)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_group=s_group, clone_group=c_group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, s_group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, s_group)
        self._fs_cmd("subvolume", "rm", self.volname, clone, c_group)

        # remove groups
        self._fs_cmd("subvolumegroup", "rm", self.volname, s_group)
        self._fs_cmd("subvolumegroup", "rm", self.volname, c_group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_fail_with_remove(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone1, clone2 = self._gen_subvol_clone_name(2)

        pool_capacity = 32 * 1024 * 1024
        # number of files required to fill up 99% of the pool
        nr_files = int((pool_capacity * 0.99) / (SubvolumeHelper.DEFAULT_FILE_SIZE * 1024 * 1024))

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=nr_files)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # add data pool
        new_pool = "new_pool"
        self.fs.add_data_pool(new_pool)

        self.run_ceph_cmd("osd", "pool", "set-quota", new_pool,
                          "max_bytes", f"{pool_capacity // 4}")

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone1, "--pool_layout", new_pool)

        # check clone status -- this should dramatically overshoot the pool quota
        self._wait_for_clone_to_complete(clone1)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone1, clone_pool=new_pool)

        # wait a bit so that subsequent I/O will give pool full error
        time.sleep(120)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone2, "--pool_layout", new_pool)

        # check clone status
        self._wait_for_clone_to_fail(clone2)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone1)
        try:
            self._fs_cmd("subvolume", "rm", self.volname, clone2)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EAGAIN:
                raise RuntimeError("invalid error code when trying to remove failed clone")
        else:
            raise RuntimeError("expected error when removing a failed clone")

        #  ... and with force, failed clone can be removed
        self._fs_cmd("subvolume", "rm", self.volname, clone2, "--force")

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_on_existing_subvolumes(self):
        subvolume1, subvolume2 = self._gen_subvol_name(2)
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create subvolumes
        self._fs_cmd("subvolume", "create", self.volname, subvolume1, "--mode=777")
        self._fs_cmd("subvolume", "create", self.volname, subvolume2, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume1, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume1, snapshot)

        # schedule a clone with target as subvolume2
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, subvolume2)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError("invalid error code when cloning to existing subvolume")
        else:
            raise RuntimeError("expected cloning to fail if the target is an existing subvolume")

        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, clone)

        # schedule a clone with target as clone
        try:
            self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume1, snapshot, clone)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError("invalid error code when cloning to existing clone")
        else:
            raise RuntimeError("expected cloning to fail if the target is an existing clone")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume1, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume1, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume1)
        self._fs_cmd("subvolume", "rm", self.volname, subvolume2)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_pool_layout(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # add data pool
        new_pool = "new_pool"
        newid = self.fs.add_data_pool(new_pool)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, "--pool_layout", new_pool)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, clone_pool=new_pool)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        subvol_path = self._get_subvolume_path(self.volname, clone)
        desired_pool = self.mount_a.getfattr(subvol_path, "ceph.dir.layout.pool")
        try:
            self.assertEqual(desired_pool, new_pool)
        except AssertionError:
            self.assertEqual(int(desired_pool), newid) # old kernel returns id

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_under_group(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()
        group = self._gen_subvol_grp_name()

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, '--target_group_name', group)

        # check clone status
        self._wait_for_clone_to_complete(clone, clone_group=group)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, clone_group=group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone, group)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_attrs(self):
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        mode = "777"
        uid  = "1000"
        gid  = "1000"
        new_uid  = "1001"
        new_gid  = "1001"
        new_mode = "700"

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, "--mode", mode, "--uid", uid, "--gid", gid)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # change subvolume attrs (to ensure clone picks up snapshot attrs)
        self._do_subvolume_attr_update(subvolume, new_uid, new_gid, new_mode)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

    def test_subvolume_snapshot_clone_with_upgrade(self):
        """
        yet another poor man's upgrade test -- rather than going through a full
        upgrade cycle, emulate old types subvolumes by going through the wormhole
        and verify clone operation.
        further ensure that a legacy volume is not updated to v2, but clone is.
        """
        subvolume = self._gen_subvol_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # emulate a old-fashioned subvolume
        createpath = os.path.join(".", "volumes", "_nogroup", subvolume)
        self.mount_a.run_shell_payload(f"sudo mkdir -p -m 777 {createpath}", omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # do some IO
        self._do_subvolume_io(subvolume, number_of_files=64)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot)

        # ensure metadata file is in legacy location, with required version v1
        self._assert_meta_location_and_version(self.volname, subvolume, version=1, legacy=True)

        # Insert delay at the beginning of snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone)

        # snapshot should not be deletable now
        try:
            self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EAGAIN, msg="invalid error code when removing source snapshot of a clone")
        else:
            self.fail("expected removing source snapshot of a clone to fail")

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_version=1)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot)

        # ensure metadata file is in v2 location, with required version v2
        self._assert_meta_location_and_version(self.volname, clone)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # verify trash dir is clean
        self._wait_for_trash_empty()

class TestMisc3(SubvolumeHelper):

    def test_subvolume_snapshot_reconf_max_concurrent_clones(self):
        """
        Validate 'max_concurrent_clones' config option
        """

        # get the default number of cloner threads
        default_max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(default_max_concurrent_clones, 4)

        # Increase number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 6)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 6)

        # Decrease number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 2)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 2)

    def test_subvolume_snapshot_config_snapshot_clone_delay(self):
        """
        Validate 'snapshot_clone_delay' config option
        """

        # get the default delay before starting the clone
        default_timeout = int(self.config_get('mgr', 'mgr/volumes/snapshot_clone_delay'))
        self.assertEqual(default_timeout, 0)

        # Insert delay of 2 seconds at the beginning of the snapshot clone
        self.config_set('mgr', 'mgr/volumes/snapshot_clone_delay', 2)
        default_timeout = int(self.config_get('mgr', 'mgr/volumes/snapshot_clone_delay'))
        self.assertEqual(default_timeout, 2)

        # Decrease number of cloner threads
        self.config_set('mgr', 'mgr/volumes/max_concurrent_clones', 2)
        max_concurrent_clones = int(self.config_get('mgr', 'mgr/volumes/max_concurrent_clones'))
        self.assertEqual(max_concurrent_clones, 2)

    def test_periodic_async_work(self):
        """
        to validate that the async thread (purge thread in this case) will
        process a job that's manually created.
        """

        self.config_set('mgr', 'mgr/volumes/periodic_async_work', True)

        trashdir = os.path.join("./", "volumes", "_deleting")
        entry = os.path.join(trashdir, "subvol")
        # hand create the directory
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', entry], omit_sudo=False)

        # verify trash dir is processed
        self._wait_for_trash_empty()

        self.config_set('mgr', 'mgr/volumes/periodic_async_work', False)

        # wait a bit so that the default wakeup time (5s) is consumed
        # by the async threads (i.e., the default gets honoured before
        # the file system gets purged).
        time.sleep(10)

    def test_subvolume_under_group_snapshot_clone(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
        snapshot = self._gen_subvol_snap_name()
        clone = self._gen_subvol_clone_name()

        # create group
        self._fs_cmd("subvolumegroup", "create", self.volname, group)

        # create subvolume
        self._fs_cmd("subvolume", "create", self.volname, subvolume, group, "--mode=777")

        # do some IO
        self._do_subvolume_io(subvolume, subvolume_group=group, number_of_files=32)

        # snapshot subvolume
        self._fs_cmd("subvolume", "snapshot", "create", self.volname, subvolume, snapshot, group)

        # schedule a clone
        self._fs_cmd("subvolume", "snapshot", "clone", self.volname, subvolume, snapshot, clone, '--group_name', group)

        # check clone status
        self._wait_for_clone_to_complete(clone)

        # verify clone
        self._verify_clone(subvolume, snapshot, clone, source_group=group)

        # remove snapshot
        self._fs_cmd("subvolume", "snapshot", "rm", self.volname, subvolume, snapshot, group)

        # remove subvolumes
        self._fs_cmd("subvolume", "rm", self.volname, subvolume, group)
        self._fs_cmd("subvolume", "rm", self.volname, clone)

        # remove group
        self._fs_cmd("subvolumegroup", "rm", self.volname, group)

        # verify trash dir is clean
        self._wait_for_trash_empty()
