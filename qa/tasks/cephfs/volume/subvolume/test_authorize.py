import os
import errno
import json

from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestAuthorize(SubvolumeHelper):

    def test_authorize_deauthorize_legacy_subvolume(self):
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
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
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
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
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

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
        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()
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

        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

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

        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

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

        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

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

        subvolume = self._gen_subvol_name()
        group = self._gen_subvol_grp_name()

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

        subvolume1, subvolume2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

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

        subvolume1, subvolume2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

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

        subvolume1, subvolume2 = self._gen_subvol_name(2)
        group = self._gen_subvol_grp_name()

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



