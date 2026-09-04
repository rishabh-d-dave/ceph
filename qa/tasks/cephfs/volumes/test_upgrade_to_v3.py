from uuid import uuid4
from json import loads as json_loads
from logging import getLogger
from textwrap import dedent
from time import sleep

from tasks.cephfs.test_volumes import VolumesHelper
from os.path import join, basename, dirname


log = getLogger(__name__)


class TestUpgradeFromV2(VolumesHelper):
    '''
    Tests related to upgrading subvol from v2 to v3.
    '''

    CLIENTS_REQUIRED = 2

    def create_v2_subvol(self, sv_name, svg_name=None, snap=None,
                         retained_snap=False):
        sv_uuid = str(uuid4())
        if not svg_name:
            svg_name = '_nogroup'
        sv_path = f'/volumes/{svg_name}/{sv_name}/{sv_uuid}'

        self.mount_a.run_shell(f'mkdir -p {sv_path[1:]}')

        sv_meta_filepath = f'volumes/{svg_name}/{sv_name}/.meta'
        self.mount_a.write_file(sv_meta_filepath, dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = {sv_path}
            state = complete'''), sudo=True)

        # so that files can accessed without unnecessary hassles
        self.mount_a.run_shell(f'chmod -R 755 {sv_path[1:]}')

        if snap:
            self.mount_a.run_shell(f'mkdir -p {sv_path}/.snap/fake')

        if retained_snap:
            pass

        return sv_path, sv_uuid

    def sanity_test_self_created_v2_subvol(self, sv_name, svg_name=None,
                                           snap_name=None, retained_snap=False):
        cmd_args = f'fs subvolume ls {self.volname}'
        if svg_name:
            cmd_args += f' --group-name {svg_name}'
        subvols = self.get_ceph_cmd_stdout(cmd_args)
        subvols = json_loads(subvols)
        assert {'name': sv_name} in subvols

    def test_regular_basic_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group.
        '''
        sv_name = self._gen_subvol_name()
        v2_sv_path, sv_uuid = self.create_v2_subvol(sv_name)

        self.sanity_test_self_created_v2_subvol(sv_name)
        
        # this will trigger subvol upgrade from v2 to v3
        v3_sv_path = self.get_ceph_cmd_stdout('fs subvolume getpath '
                                              f'{self.volname} {sv_name}')
        self.assertEqual(v3_sv_path,
                         f'/volumes/_nogroup/{sv_name}/roots/{sv_uuid}/mnt\n')
        v3_sv_path = v3_sv_path.strip()

        sv_meta_file_path = join(dirname(dirname(dirname(v3_sv_path))), '.meta')
        sv_meta_file_path = sv_meta_file_path[1:]
        meta_content = self.mount_a.get_shell_stdout(f'sudo cat {sv_meta_file_path}')
        meta_content = meta_content.split('\n')
        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = {v3_sv_path}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        self.run_ceph_cmd(f'fs subvolume rm {self.volname} {sv_name}')
        self._wait_for_trash_empty()

    def test_regular_basic_subvol_with_workload(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        sv_name = self._gen_subvol_name()
        v2_sv_path, sv_uuid = self.create_v2_subvol(sv_name)
        self.sanity_test_self_created_v2_subvol(sv_name)

        self.mount_b.remount(cephfs_mntpt=v2_sv_path)

        writer = self.mount_b.write_files_in_bg('/')
        log.info('giving 60 seconds for background threads for writing...')
        sleep(20)
        
        # this will trigger subvol upgrade from v2 to v3
        v3_sv_path = self.get_ceph_cmd_stdout('fs subvolume getpath '
                                              f'{self.volname} {sv_name}')
        self.assertEqual(writer.is_alive(), True)
        self.assertEqual(v3_sv_path,
                         f'/volumes/_nogroup/{sv_name}/roots/{sv_uuid}/mnt\n')
        v3_sv_path = v3_sv_path.strip()

        sv_meta_file_path = join(dirname(dirname(dirname(v3_sv_path))), '.meta')
        sv_meta_file_path = sv_meta_file_path[1:]
        meta_content = self.mount_a.get_shell_stdout(f'sudo cat {sv_meta_file_path}')
        meta_content = meta_content.split('\n')
        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = {v3_sv_path}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        writer.stop()
        file_count = self.mount_b.get_shell_stdout('find ./ -type f | wc -l')
        file_count = int(file_count.strip())
        self.assertEqual(file_count, int(writer.file_count))

        self.run_ceph_cmd(f'fs subvolume rm {self.volname} {sv_name}')
        self._wait_for_trash_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in a
        non-default subvol group.
        '''
        sv_name = self._gen_subvol_name()
        svg_name = self._gen_subvol_grp_name()
        v2_sv_path, sv_uuid = self.create_v2_subvol(sv_name, svg_name)
        self.sanity_test_self_created_v2_subvol(sv_name, svg_name)
        
        # this will trigger subvol upgrade from v2 to v3
        v3_sv_path = self.get_ceph_cmd_stdout('fs subvolume getpath '
                                              f'{self.volname} {sv_name} '
                                              f'--group-name {svg_name}')
        self.assertEqual(v3_sv_path,
                         f'/volumes/{svg_name}/{sv_name}/roots/{sv_uuid}/mnt')
        v3_sv_path = v3_sv_path.strip()

        sv_meta_file_path = join(dirname(dirname(dirname(v3_sv_path))), '.meta')
        sv_meta_file_path = sv_meta_file_path[1:]
        meta_content = self.mount_a.get_shell_stdout(f'sudo cat {sv_meta_file_path}')
        meta_content = meta_content.split('\n')
        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = {v3_sv_path}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        self.run_ceph_cmd(f'fs subvolume rm {self.volname} {sv_name} '
                          f'--group-name {svg_name}')
        self._wait_for_trash_empty()

    def test_subvol_with_snaps(self):
        pass

    def test_subvol_with_retained_snaps(self):
        pass
