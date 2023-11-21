class VolumeHelper(CephFSTestCase):
    """
    Helper class for testing FS volume, subvolume group and subvolume
    operations.
    """

    TEST_VOLUME_PREFIX = "volume"

    # for filling subvolume with data
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    # io defaults
    DEFAULT_FILE_SIZE = 1 # MB
    DEFAULT_NUMBER_OF_FILES = 1024

    def _fs_cmd(self, *args):
        return self.get_ceph_cmd_stdout("fs", *args)

    def _raw_cmd(self, *args):
        return self.get_ceph_cmd_stdout(args)

    def _generate_random_volume_name(self, count=1):
        n = self.volume_start
        volumes = [f"{TestVolumes.TEST_VOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.volume_start += count
        return volumes[0] if count == 1 else volumes

    def _enable_multi_fs(self):
        self._fs_cmd("flag", "set", "enable_multiple", "true", "--yes-i-really-mean-it")

    def _create_or_reuse_test_volume(self):
        result = json.loads(self._fs_cmd("volume", "ls"))
        if len(result) == 0:
            self.vol_created = True
            self.volname = self._generate_random_volume_name()
            self._fs_cmd("volume", "create", self.volname)
        else:
            self.volname = result[0]['name']

    def  _get_volume_info(self, vol_name, human_readable=False):
        if human_readable:
            args = ["volume", "info", vol_name, human_readable]
        else:
            args = ["volume", "info", vol_name]
        args = tuple(args)
        vol_md = self._fs_cmd(*args)
        return vol_md

    def _wait_for_trash_empty(self, timeout=60):
        # XXX: construct the trash dir path (note that there is no mgr
        # [sub]volume interface for this).
        trashdir = os.path.join("./", "volumes", "_deleting")
        self.mount_a.wait_for_dir_empty(trashdir, timeout=timeout)

    def _wait_for_subvol_trash_empty(self, subvol, group="_nogroup", timeout=30):
        trashdir = os.path.join("./", "volumes", group, subvol, ".trash")
        try:
            self.mount_a.wait_for_dir_empty(trashdir, timeout=timeout)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                pass
            else:
                raise

    def _assert_meta_location_and_version(self, vol_name, subvol_name, subvol_group=None, version=2, legacy=False):
        if legacy:
            subvol_path = self._get_subvolume_path(vol_name, subvol_name, group_name=subvol_group)
            m = md5()
            m.update(("/"+subvol_path).encode('utf-8'))
            meta_filename = "{0}.meta".format(m.digest().hex())
            metapath = os.path.join(".", "volumes", "_legacy", meta_filename)
        else:
            group = subvol_group if subvol_group is not None else '_nogroup'
            metapath = os.path.join(".", "volumes", group, subvol_name, ".meta")

        out = self.mount_a.run_shell(['sudo', 'cat', metapath], omit_sudo=False)
        lines = out.stdout.getvalue().strip().split('\n')
        sv_version = -1
        for line in lines:
            if line == "version = " + str(version):
                sv_version = version
                break
        self.assertEqual(sv_version, version, "version expected was '{0}' but got '{1}' from meta file at '{2}'".format(
                         version, sv_version, metapath))

    def _configure_guest_auth(self, guest_mount, authid, key):
        """
        Set up auth credentials for a guest client.
        """
        # Create keyring file for the guest client.
        keyring_txt = dedent("""
        [client.{authid}]
            key = {key}

        """.format(authid=authid,key=key))

        guest_mount.client_id = authid
        guest_mount.client_remote.write_file(guest_mount.get_keyring_path(),
                                             keyring_txt, sudo=True)
        # Add a guest client section to the ceph config file.
        self.config_set("client.{0}".format(authid), "debug client", 20)
        self.config_set("client.{0}".format(authid), "debug objecter", 20)
        self.set_conf("client.{0}".format(authid),
                      "keyring", guest_mount.get_keyring_path())

    def _auth_metadata_get(self, filedata):
        """
        Return a deserialized JSON object, or None
        """
        try:
            data = json.loads(filedata)
        except json.decoder.JSONDecodeError:
            data = None
        return data

    def setUp(self):
        super(TestVolumesHelper, self).setUp()
        self.volname = None
        self.vol_created = False
        self._enable_multi_fs()
        self._create_or_reuse_test_volume()
        self.config_set('mon', 'mon_allow_pool_delete', True)
        self.volume_start = random.randint(1, (1<<20))
        self.subvolume_start = random.randint(1, (1<<20))
        self.group_start = random.randint(1, (1<<20))
        self.snapshot_start = random.randint(1, (1<<20))
        self.clone_start = random.randint(1, (1<<20))

    def tearDown(self):
        if self.vol_created:
            self._delete_test_volume()
        super(TestVolumesHelper, self).tearDown()
