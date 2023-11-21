import os
import json

from logging import getLogger
from textwrap import dedent

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError

log = getLogger(__name__)


class TestMisc(SubvolumeHelper):

    def test_subvolume_info(self):
        # tests the 'fs subvolume info' command

        subvol_md = ["atime", "bytes_pcent", "bytes_quota", "bytes_used", "created_at", "ctime",
                     "data_pool", "gid", "mode", "mon_addrs", "mtime", "path", "pool_namespace",
                     "type", "uid", "features", "state"]

        # create subvolume
        subvolume = self._gen_subvol_name()
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
        subvolume = self._gen_subvol_name()

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

        subvolumes = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

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
