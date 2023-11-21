import os
import errno

from logging import getLogger

from tasks.cephfs.volume.subvolume import SubvolumeHelper
from tasks.cephfs.fuse_mount import FuseMount

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestResize(SubvolumeHelper):

    def test_subvolume_resize_fail_invalid_size(self):
        """
        That a subvolume cannot be resized to an invalid size and the quota did not change
        """

        osize = self.DEFAULT_FILE_SIZE*1024*1024
        # create subvolume
        subvolname = self._gen_subvol_name()
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
        subvolname = self._gen_subvol_name()
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
        subvolname = self._gen_subvol_name()
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
        filename = "{0}.{1}".format(SubvolumeHelper.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+1)
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
        subvolname = self._gen_subvol_name()
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
        filename = "{0}.{1}".format(SubvolumeHelper.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+2)
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
        subvolname = self._gen_subvol_name()
        self._fs_cmd("subvolume", "create", self.volname, subvolname, "--size", str(osize), "--mode=777")
        subvolpath = self._get_subvolume_path(self.volname, subvolname)
        self.assertNotEqual(subvolpath, None)

        # create one file of size 10MB and write
        file_size=self.DEFAULT_FILE_SIZE*10
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(SubvolumeHelper.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+3)
        self.mount_a.write_n_mb(os.path.join(subvolpath, filename), file_size)

        # create a file of size 5MB and try write more
        file_size=file_size // 2
        number_of_files=1
        log.debug("filling subvolume {0} with {1} file of size {2}MB".format(subvolname,
                                                                             number_of_files,
                                                                             file_size))
        filename = "{0}.{1}".format(SubvolumeHelper.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+4)
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
        subvolname = self._gen_subvol_name()
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
        subvolname = self._gen_subvol_name()
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
        filename = "{0}.{1}".format(SubvolumeHelper.TEST_FILE_NAME_PREFIX, self.DEFAULT_NUMBER_OF_FILES+5)

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
        subvolname = self._gen_subvol_name()
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
        subvolname = self._gen_subvol_name()
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
