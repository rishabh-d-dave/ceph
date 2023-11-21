class SubvolumeHelper:
    '''
    Helper classess for subvolume tests.
    '''

    TEST_SUBVOLUME_PREFIX="subvolume"
    TEST_GROUP_PREFIX="group"
    TEST_SNAPSHOT_PREFIX="snapshot"
    TEST_CLONE_PREFIX="clone"
    TEST_FILE_NAME_PREFIX="subvolume_file"

    def __check_clone_state(self, state, clone, clone_group=None, timo=120):
        check = 0
        args = ["clone", "status", self.volname, clone]
        if clone_group:
            args.append(clone_group)
        args = tuple(args)
        while check < timo:
            result = json.loads(self._fs_cmd(*args))
            if result["status"]["state"] == state:
                break
            check += 1
            time.sleep(1)
        self.assertTrue(check < timo)

    def _get_clone_status(self, clone, clone_group=None):
        args = ["clone", "status", self.volname, clone]
        if clone_group:
            args.append(clone_group)
        args = tuple(args)
        result = json.loads(self._fs_cmd(*args))
        return result

    def _wait_for_clone_to_complete(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("complete", clone, clone_group, timo)

    def _wait_for_clone_to_fail(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("failed", clone, clone_group, timo)

    def _wait_for_clone_to_be_in_progress(self, clone, clone_group=None, timo=120):
        self.__check_clone_state("in-progress", clone, clone_group, timo)

    def _check_clone_canceled(self, clone, clone_group=None):
        self.__check_clone_state("canceled", clone, clone_group, timo=1)

    def _get_subvolume_snapshot_path(self, subvolume, snapshot, source_group, subvol_path, source_version):
        if source_version == 2:
            # v2
            if subvol_path is not None:
                (base_path, uuid_str) = os.path.split(subvol_path)
            else:
                (base_path, uuid_str) = os.path.split(self._get_subvolume_path(self.volname, subvolume, group_name=source_group))
            return os.path.join(base_path, ".snap", snapshot, uuid_str)

        # v1
        base_path = self._get_subvolume_path(self.volname, subvolume, group_name=source_group)
        return os.path.join(base_path, ".snap", snapshot)

    def _verify_clone_attrs(self, source_path, clone_path):
        path1 = source_path
        path2 = clone_path

        p = self.mount_a.run_shell(["find", path1])
        paths = p.stdout.getvalue().strip().split()

        # for each entry in source and clone (sink) verify certain inode attributes:
        # inode type, mode, ownership, [am]time.
        for source_path in paths:
            sink_entry = source_path[len(path1)+1:]
            sink_path = os.path.join(path2, sink_entry)

            # mode+type
            sval = int(self.mount_a.run_shell(['stat', '-c' '%f', source_path]).stdout.getvalue().strip(), 16)
            cval = int(self.mount_a.run_shell(['stat', '-c' '%f', sink_path]).stdout.getvalue().strip(), 16)
            self.assertEqual(sval, cval)

            # ownership
            sval = int(self.mount_a.run_shell(['stat', '-c' '%u', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%u', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

            sval = int(self.mount_a.run_shell(['stat', '-c' '%g', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%g', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

            # inode timestamps
            # do not check access as kclient will generally not update this like ceph-fuse will.
            sval = int(self.mount_a.run_shell(['stat', '-c' '%Y', source_path]).stdout.getvalue().strip())
            cval = int(self.mount_a.run_shell(['stat', '-c' '%Y', sink_path]).stdout.getvalue().strip())
            self.assertEqual(sval, cval)

    def _verify_clone_root(self, source_path, clone_path, clone, clone_group, clone_pool):
        # verifies following clone root attrs quota, data_pool and pool_namespace
        # remaining attributes of clone root are validated in _verify_clone_attrs

        clone_info = json.loads(self._get_subvolume_info(self.volname, clone, clone_group))

        # verify quota is inherited from source snapshot
        src_quota = self.mount_a.getfattr(source_path, "ceph.quota.max_bytes")
        # FIXME: kclient fails to get this quota value: https://tracker.ceph.com/issues/48075
        if isinstance(self.mount_a, FuseMount):
            self.assertEqual(clone_info["bytes_quota"], "infinite" if src_quota is None else int(src_quota))

        if clone_pool:
            # verify pool is set as per request
            self.assertEqual(clone_info["data_pool"], clone_pool)
        else:
            # verify pool and pool namespace are inherited from snapshot
            self.assertEqual(clone_info["data_pool"],
                             self.mount_a.getfattr(source_path, "ceph.dir.layout.pool"))
            self.assertEqual(clone_info["pool_namespace"],
                             self.mount_a.getfattr(source_path, "ceph.dir.layout.pool_namespace"))

    def _verify_clone(self, subvolume, snapshot, clone,
                      source_group=None, clone_group=None, clone_pool=None,
                      subvol_path=None, source_version=2, timo=120):
        # pass in subvol_path (subvolume path when snapshot was taken) when subvolume is removed
        # but snapshots are retained for clone verification
        path1 = self._get_subvolume_snapshot_path(subvolume, snapshot, source_group, subvol_path, source_version)
        path2 = self._get_subvolume_path(self.volname, clone, group_name=clone_group)

        check = 0
        # TODO: currently snapshot rentries are not stable if snapshot source entries
        #       are removed, https://tracker.ceph.com/issues/46747
        while check < timo and subvol_path is None:
            val1 = int(self.mount_a.getfattr(path1, "ceph.dir.rentries"))
            val2 = int(self.mount_a.getfattr(path2, "ceph.dir.rentries"))
            if val1 == val2:
                break
            check += 1
            time.sleep(1)
        self.assertTrue(check < timo)

        self._verify_clone_root(path1, path2, clone, clone_group, clone_pool)
        self._verify_clone_attrs(path1, path2)

    def _generate_random_subvolume_name(self, count=1):
        n = self.subvolume_start
        subvolumes = [f"{TestVolumes.TEST_SUBVOLUME_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.subvolume_start += count
        return subvolumes[0] if count == 1 else subvolumes

    def _generate_random_group_name(self, count=1):
        n = self.group_start
        groups = [f"{TestVolumes.TEST_GROUP_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.group_start += count
        return groups[0] if count == 1 else groups

    def _generate_random_snapshot_name(self, count=1):
        n = self.snapshot_start
        snaps = [f"{TestVolumes.TEST_SNAPSHOT_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.snapshot_start += count
        return snaps[0] if count == 1 else snaps

    def _generate_random_clone_name(self, count=1):
        n = self.clone_start
        clones = [f"{TestVolumes.TEST_CLONE_PREFIX}_{i:016}" for i in range(n, n+count)]
        self.clone_start += count
        return clones[0] if count == 1 else clones

    def  _get_subvolume_group_path(self, vol_name, group_name):
        args = ("subvolumegroup", "getpath", vol_name, group_name)
        path = self._fs_cmd(*args)
        # remove the leading '/', and trailing whitespaces
        return path[1:].rstrip()

    def  _get_subvolume_group_info(self, vol_name, group_name):
        args = ["subvolumegroup", "info", vol_name, group_name]
        args = tuple(args)
        group_md = self._fs_cmd(*args)
        return group_md

    def  _get_subvolume_path(self, vol_name, subvol_name, group_name=None):
        args = ["subvolume", "getpath", vol_name, subvol_name]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        path = self._fs_cmd(*args)
        # remove the leading '/', and trailing whitespaces
        return path[1:].rstrip()

    def  _get_subvolume_info(self, vol_name, subvol_name, group_name=None):
        args = ["subvolume", "info", vol_name, subvol_name]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        subvol_md = self._fs_cmd(*args)
        return subvol_md

    def _get_subvolume_snapshot_info(self, vol_name, subvol_name, snapname, group_name=None):
        args = ["subvolume", "snapshot", "info", vol_name, subvol_name, snapname]
        if group_name:
            args.append(group_name)
        args = tuple(args)
        snap_md = self._fs_cmd(*args)
        return snap_md

    def _delete_test_volume(self):
        self._fs_cmd("volume", "rm", self.volname, "--yes-i-really-mean-it")

    def _do_subvolume_pool_and_namespace_update(self, subvolume, pool=None, pool_namespace=None, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        if pool is not None:
            self.mount_a.setfattr(subvolpath, 'ceph.dir.layout.pool', pool, sudo=True)

        if pool_namespace is not None:
            self.mount_a.setfattr(subvolpath, 'ceph.dir.layout.pool_namespace', pool_namespace, sudo=True)

    def _do_subvolume_attr_update(self, subvolume, uid, gid, mode, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        # mode
        self.mount_a.run_shell(['sudo', 'chmod', mode, subvolpath], omit_sudo=False)

        # ownership
        self.mount_a.run_shell(['sudo', 'chown', uid, subvolpath], omit_sudo=False)
        self.mount_a.run_shell(['sudo', 'chgrp', gid, subvolpath], omit_sudo=False)

    def _do_subvolume_io(self, subvolume, subvolume_group=None, create_dir=None,
                         number_of_files=DEFAULT_NUMBER_OF_FILES, file_size=DEFAULT_FILE_SIZE):
        # get subvolume path for IO
        args = ["subvolume", "getpath", self.volname, subvolume]
        if subvolume_group:
            args.append(subvolume_group)
        args = tuple(args)
        subvolpath = self._fs_cmd(*args)
        self.assertNotEqual(subvolpath, None)
        subvolpath = subvolpath[1:].rstrip() # remove "/" prefix and any trailing newline

        io_path = subvolpath
        if create_dir:
            io_path = os.path.join(subvolpath, create_dir)
            self.mount_a.run_shell_payload(f"mkdir -p {io_path}")

        log.debug("filling subvolume {0} with {1} files each {2}MB size under directory {3}".format(subvolume, number_of_files, file_size, io_path))
        for i in range(number_of_files):
            filename = "{0}.{1}".format(TestVolumes.TEST_FILE_NAME_PREFIX, i)
            self.mount_a.write_n_mb(os.path.join(io_path, filename), file_size)

    def _do_subvolume_io_mixed(self, subvolume, subvolume_group=None):
        subvolpath = self._get_subvolume_path(self.volname, subvolume, group_name=subvolume_group)

        reg_file = "regfile.0"
        dir_path = os.path.join(subvolpath, "dir.0")
        sym_path1 = os.path.join(subvolpath, "sym.0")
        # this symlink's ownership would be changed
        sym_path2 = os.path.join(dir_path, "sym.0")

        self.mount_a.run_shell(["mkdir", dir_path])
        self.mount_a.run_shell(["ln", "-s", "./{}".format(reg_file), sym_path1])
        self.mount_a.run_shell(["ln", "-s", "./{}".format(reg_file), sym_path2])
        # flip ownership to nobody. assumption: nobody's id is 65534
        self.mount_a.run_shell(["sudo", "chown", "-h", "65534:65534", sym_path2], omit_sudo=False)

    def _create_v1_subvolume(self, subvol_name, subvol_group=None, has_snapshot=True, subvol_type='subvolume', state='complete'):
        group = subvol_group if subvol_group is not None else '_nogroup'
        basepath = os.path.join("volumes", group, subvol_name)
        uuid_str = str(uuid.uuid4())
        createpath = os.path.join(basepath, uuid_str)
        self.mount_a.run_shell(['sudo', 'mkdir', '-p', createpath], omit_sudo=False)

        # create a v1 snapshot, to prevent auto upgrades
        if has_snapshot:
            snappath = os.path.join(createpath, ".snap", "fake")
            self.mount_a.run_shell(['sudo', 'mkdir', '-p', snappath], omit_sudo=False)

        # add required xattrs to subvolume
        default_pool = self.mount_a.getfattr(".", "ceph.dir.layout.pool")
        self.mount_a.setfattr(createpath, 'ceph.dir.layout.pool', default_pool, sudo=True)

        # create a v1 .meta file
        meta_contents = "[GLOBAL]\nversion = 1\ntype = {0}\npath = {1}\nstate = {2}\n".format(subvol_type, "/" + createpath, state)
        if state == 'pending':
            # add a fake clone source
            meta_contents = meta_contents + '[source]\nvolume = fake\nsubvolume = fake\nsnapshot = fake\n'
        meta_filepath1 = os.path.join(self.mount_a.mountpoint, basepath, ".meta")
        self.mount_a.client_remote.write_file(meta_filepath1, meta_contents, sudo=True)
        return createpath

    def _update_fake_trash(self, subvol_name, subvol_group=None, trash_name='fake', create=True):
        group = subvol_group if subvol_group is not None else '_nogroup'
        trashpath = os.path.join("volumes", group, subvol_name, '.trash', trash_name)
        if create:
            self.mount_a.run_shell(['sudo', 'mkdir', '-p', trashpath], omit_sudo=False)
        else:
            self.mount_a.run_shell(['sudo', 'rmdir', trashpath], omit_sudo=False)
