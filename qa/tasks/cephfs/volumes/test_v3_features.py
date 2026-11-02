'''
Test v3-specific features.
'''
from uuid import UUID
from errno import ENOENT
from os.path import join
from logging import getLogger

from tasks.cephfs.helpers.misc import (gen_name, gen_subvol_name,
                                       gen_subvol_grp_name,
                                       gen_subvol_snap_name)
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.helpers.subvolumes import SubvolV3Helper, gen_uuid, \
                                            verify_uuid

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


class TestLayout(CephFSTestCase):
    '''
    Test v3 subvols have their directory layout as expected.
    '''

    def wait_for_trash_dir_to_be_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)

    def test_basic_subvol(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()

        sv.verify_subvol_mnt_path()
        sv.verify_uuid()
        sv.verify_subvol_ls_cmd()
        sv.verify_layout()
        sv.verify_meta_file()

        # test writing files on this path.
        sv.write_files(num=5)
        subvol_mnt_path = sv.getpath()
        for i in range(5):
            file_path = join(subvol_mnt_path, f'file-{i}')
            self.mount_a.verify_path_is_reg(file_path)

        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_snap(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        subvol_mnt_path = sv.getpath()

        sv.verify_uuid()

        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)

        self.mount_a.run_shell(f'ls ./{snap_path}')
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        sv.snap_rm(snap_name)
        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_retained_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        sv.verify_uuid()
        subvol_mnt_path = sv.getpath()
        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)
        sv.rm(opts='--retain-snapshots')

        sv.verify_uuid()
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        sv.snap_rm(snap_name)
        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_2nd_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        for i in range(2):
            sv.create()
            sv.snap_create()
            if i != 1:
                sv.rm(opts='--retain-snapshots')

        snap_name = sv.all_incars[-1].snap_names[-1]
        sv.verify_uuid()
        # checks layout not just fot current incar but also previous incars
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        sv.rm(opts='--retain-snapshots')
        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def test_subvol_with_3rd_incar(self):
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        for i in range(3):
            sv.create()
            sv.snap_create()
            if i != 2:
                sv.rm(opts='--retain-snapshots')

        snap_name = sv.all_incars[-1].snap_names[-1]
        sv.verify_uuid()
        # checks layout not just fot current incar but also previous incars
        sv.verify_layout()
        sv.verify_meta_file()
        sv.verify_subvol_ls_cmd()
        sv.verify_subvol_getpath_cmd()
        sv.verify_snap_ls_cmd(snap_name)
        sv.verify_snap_getpath_cmd(snap_name)

        self.mount_a.negtestcmd(f'ls ./{sv.gen_base_path()}', retval=ENOENT)
        self.wait_for_trash_dir_to_be_empty()


class TestIncarnations(CephFSTestCase):
    '''
    Test only features specific to subvolumes v3 here, since usual operations like
    create, remove, ls, etc are tested for v3 too through usual tests.
    '''

    def wait_for_trash_dir_to_be_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)

    def _test_removing_retained_snap_removes_subvol(self):
        '''
        Test that an incarnation is retained if it the subvlume was removed but
        snapshots were retained.
        '''
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        sv.verify_uuid()

        subvol_mnt_path = sv.getpath()
        for i in range(5):
            file_path = f'{subvol_mnt_path}/file-{i}'
            self.mount_a.run_shell(f'echo abcd | tee {file_path}')
        snap_name = sv.create_snap()

        snap_path = sv.snap_getpath()
        sv.rm(opts='--retain-snapshots')
        self.mount_a.run_shell(f'ls ./{snap_path}')

        sv.snap_rm(snap_name)
        self.assertNotIn(sv.name, sv.ls())
        with self.assertRaise(CommandFailedError) as e:
            sv.getpath()
            self.assertEqual(e.exception.exitstatus, ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def rm_incar_and_check(self, sv, snap_paths, index):
        self.mount_a.negtest_cmd(f'ls ./{snap_paths[index]}', retval=ENOENT)
        snap_paths.pop(index)
        for snap_name in sv.snaps:
            sv.verify_snap_ls_cmd(snap_name)
            sv.verify_snap_getpath_cmd(snap_name)

    def _test_create_and_rm(self):

        '''
        * create a subvol with 7 incars
            * check that uuid is unique every time
            * check that snap path is unique every time

        * check snapshot ls cmd reports names of all snaps before amd after
          subvol removal with --retain-snapshots
        * check snapshot getpath cmd reports all snap paths before and after
          subvol removal with --retain-snapshots

        * check meta file contents

        * delete first, last and an incar in middle
        * delete rest of incars, last incar deletion deletes subvol
        '''
        sv = SubvolV3Helper(self.mon_manager, self.mount_a, self.fs.name)
        all_snap_paths = []
        all_uuids = []

        # create 7 incars
        for i in range(7):
            sv.create()

            sv.verify_uuid()
            sv.write_files(num=5, sfx=i)
            snap_name = sv.snap_create()
            snap_path = sv.snap_getpath()

            uuid = sv.get_uuid_from_path(snap_path)
            if uuid in all_uuids:
                raise RuntimeError('UUIDs must be unique!')
            all_uuids.append(uuid)

            sv.verify_meta_file()

            if snap_path in all_snap_paths:
                raise RuntimeError('subvol snap paths must be unique!')
            all_snap_paths.append(snap_path)

            sv.rm(opts='--retain-snapshots')
            for snap_name in sv.snaps:
                sv.verify_snap_ls_cmd(snap_name)
                sv.verify_snap_getpath_cmd(sv.gen_snap_path(snap_name))

        self.rm_incar_and_check(sv, all_snap_paths, 0)
        self.rm_incar_and_check(sv, all_snap_paths, -1)
        self.rm_incar_and_check(sv, all_snap_paths, 3)

        for i in sv.snaps:
            sv.snap_rm(i)
            self.mount_a.negtest_cmd(f'ls ./{all_snap_paths[i]}', retval=ENOENT)
            self.rm_incar_and_check(sv, all_snap_paths, 3)

        self.mount_a.negtest_cmd(f'ls ./{sv.subvol_mnt_path}', retval=ENOENT)
        self.assertNotIn(sv.name, str(sv.ls()))
        self.wait_for_trash_dir_to_be_empty()
