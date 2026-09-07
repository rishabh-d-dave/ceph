from logging import getLogger
from os.path import join, basename, dirname
from uuid import uuid4
from time import sleep as time_sleep
from json import loads as json_loads
from textwrap import dedent

from tasks.cephfs.test_volumes import VolumesHelper


log = getLogger(__name__)


# NOTE: If generating IO load, 2 mounts are needed. One of operating on
# subvolume, other for generating load through it. self.mount_a for former
#and self.mount_b for latter. self.mount_b is also named as
# self.mount_for_io_load for convenience.
class SubvolHelper:
    '''
    Base class for v0, v1, v2, and v3 subvol helper classes.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, g_name=None,
                 writer=None, snap=False, retained_snap=False):
        # tco = test class object
        self.tco = tco
        self.volname = self.tco.volname

        # g_name = subvol group name
        if isinstance(g_name, str):
            self.g_name = None if g_name == '_nogroup' else g_name
        elif g_name is True:
            self.g_name = self.tco._gen_subvol_grp_name()
        elif g_name is None or g_name is False:
            self.g_name = None
        else:
            raise RuntimeErrror('self.g_name is neither True, None or '
                                f'str. self.g_name = {self.g_name}')

        self.name = name if name else self.tco._gen_subvol_name()
        self.uuid = uuid if uuid else str(uuid4())

        self._define_paths()

        self.tco.mount_for_io_load = self.tco.mount_b
        self.writer = writer

        self.snap = snap
        self.retained_snap = retained_snap

    def _define_paths(self):
        '''
        This methods only shows the attributes derived classes needs to set.
        '''
        raise NotImplementedError()

        # should be path to the dir where user's subvol data is
        self.data_path = None
        # should be path to the dir where subvol's metadata file is
        self.meta_path = None

    def remove(self, wait=False):
        sv_rm_cmd = f'fs subvolume rm {self.volname} {self.name}'
        if self.g_name:
            sv_rm_cmd += f' --group-name {self.g_name}'

        self.tco.run_ceph_cmd(sv_rm_cmd)

    def sanity_test(self):
        '''
        Checks if this create v2 subvol is being managed by volumes plugin.
        '''
        sv_ls_cmd = f'fs subvolume ls {self.volname}'
        if self.g_name:
            sv_ls_cmd += f' --group-name {self.g_name}'

        subvols = self.tco.get_ceph_cmd_stdout(sv_ls_cmd)
        subvols = json_loads(subvols)
        assert {'name': self.name} in subvols

    def create_client_and_remount(self):
        client_id = 'x1'

        keyring = self.tco.create_client(
                client_id, moncap='allow r',
                osdcap=f'allow rw pool={self.tco.fs.data_pool_name}',
                mdscap=f'allow rw path=/{self.data_path}')

        self.tco.mount_for_io_load.remount(client_id=client_id,
                                           client_keyring=keyring,
                                           cephfs_mntpt='/'+self.data_path)

    def gen_io_load_via_fs_client(self):
        self.writer = self.tco.mount_for_io_load.gen_io_load('/')

    def gen_io_load_via_subvol_client(self):
        self.create_client_and_remount()
        self.writer = self.tco.mount_for_io_load.gen_io_load('/')


class SubvolV3Helper(SubvolHelper):
    '''
    Helper class for subvol v3.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, g_name=None,
                 writer=None, snap=False, retained_snap=False):
        super(SubvolV3Helper, self).__init__(tco=tco, volname=volname,
                                             name=name, uuid=uuid,
                                             g_name=g_name, writer=writer,
                                             snap=snap,
                                             retained_snap=retained_snap)

    def _define_paths(self):
        g_name = '_nogroup' if self.g_name is None else self.g_name
        self.data_path = (f'volumes/{g_name}/{self.name}/roots/'
                          f'{self.uuid}/mnt')
        self.meta_path = f'volumes/{g_name}/{self.name}/.meta'

    def verify_meta_file(self):
        '''
        Verify that subvolume and its metadata file are how they should be for
        a v3 subvolume.
        '''
        meta_content = self.tco.mount_a.get_shell_stdout(f'sudo cat {self.meta_path}')

        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        self.tco.assertIn('version = 3', meta_content)
        self.tco.assertIn(f'path = /{self.data_path}', meta_content)
        self.tco.assertIn('state = complete', meta_content)
        self.tco.assertIn('type = subvolume', meta_content)


class SubvolV2Helper(SubvolHelper):
    '''
    Helper class for subvol v2.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, g_name=None,
                 writer=None, snap=False, retained_snap=False):
        super(SubvolV2Helper, self).__init__(tco=tco, volname=volname,
                                             name=name, uuid=uuid,
                                             g_name=g_name, writer=writer,
                                             snap=snap,
                                             retained_snap=retained_snap)

    def _define_paths(self):
        g_name = self.g_name if self.g_name else '_nogroup'
        self.data_path = f'volumes/{g_name}/{self.name}/{self.uuid}'
        self.meta_path = f'volumes/{g_name}/{self.name}/.meta'

    def create(self):
        '''
        Create mock v2 subvol for testing upgrade.
        '''
        self.tco.mount_a.run_shell(f'mkdir -p {self.data_path}')

        self.tco.mount_a.write_file(self.meta_path, dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = /{self.data_path}
            state = complete'''), sudo=True)

        # so that files can accessed without unnecessary hassles
        self.tco.mount_a.run_shell(f'chmod -R 755 {self.data_path}')

        if self.snap:
            self.tco.mount_a.run_shell(f'mkdir -p {self.data_path}/.snap/fake')
            raise NotImplementedError()

        if self.retained_snap:
            raise NotImplementedError()

    def upgrade_to_v3(self):
        getpath_cmd = f'fs subvolume getpath {self.volname} {self.name}'
        if self.g_name:
            getpath_cmd += f' --group-name {self.g_name}'

        # XXX: this will trigger subvol upgrade
        v3_sv_data_path = self.tco.get_ceph_cmd_stdout(getpath_cmd).\
                strip()

        v3 = SubvolV3Helper(tco=self.tco, volname=self.volname, name=self.name,
                            uuid=self.uuid, g_name=self.g_name,
                            writer=self.writer, snap=self.snap,
                            retained_snap=self.retained_snap)

        msg = (f'apparently subvol upgrade for {self.name} from v2 to v3 '
               'passed (because there was no crash) but output of getpath cmd '
               'is incorrect')
        self.tco.assertEqual(v3_sv_data_path, f'/{v3.data_path}', msg)

        return v3


class TestUpgradeFromV2(VolumesHelper):
    '''
    Test subvol upgrade from v2 to v3.
    '''

    CLIENTS_REQUIRED = 2

    def test_regular_basic_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.create()
        v2.sanity_test()
        
        v3 = v2.upgrade_to_v3()
        v3.verify_meta_file()
        v3.sanity_test()

        v3.remove()
        self._wait_for_trash_empty()

    def test_regular_basic_subvol_with_workload_via_fs_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.create()
        v2.sanity_test()

        v2.gen_io_load_via_subvol_client()
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(20)
        
        v3 = v2.upgrade_to_v3()
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        # XXX writer threads shouldn't die or be affected due to upgrade
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test()

        # upgrade was successful, stopping client workload
        v3.writer.stop()
        # avoids unnecessary failure in case writer threads takes some time to
        # stop
        time_sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()

    def test_regular_basic_subvol_with_workload_via_subvol_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.create()
        v2.sanity_test()

        v2.gen_io_load_via_fs_client()
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(20)
        
        v3 = v2.upgrade_to_v3()
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        # XXX writer threads shouldn't die or be affected due to upgrade
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test()

        # upgrade was successful, stopping client workload
        v3.writer.stop()
        # avoids unnecessary failure in case writer threads takes some time to
        # stop
        time_sleep(5)
        msg = ('upgrade was successful but writer thread didnt stop despite '
               'signaling stop')
        self.assertEqual(v3.writer.is_alive(), False, msg)

        # verifying if files were actually being written on the subvol
        v3.writer.verify_num_of_files_written()

        v3.remove()
        self._wait_for_trash_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in a
        non-default subvol group.
        '''
        v2 = SubvolV2Helper(tco=self, g_name=True)
        v2.create()
        v2.sanity_test()
        
        v3 = v2.upgrade_to_v3()
        v3.verify_meta_file()
        v3.sanity_test()

        v3.remove()
        self._wait_for_trash_empty()

    def test_subvol_with_snaps(self):
        pass

    def test_subvol_with_retained_snaps(self):
        pass
