from logging import getLogger
from os.path import join, basename, dirname
from uuid import uuid4, UUID
from time import sleep as time_sleep
from json import loads as json_loads
from textwrap import dedent

from tasks.cephfs.test_volumes import VolumesHelper


log = getLogger(__name__)

def ensure_uuid_is_valid(uuid):
    assert type(uuid) is str

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolHelper:
    '''
    Base class for v0, v1, v2, and v3 subvol helper classes.
    '''

    CLIENT_ID = 'x1'

    def __init__(self, tco, volname=None, name=None, uuid=None, grp_name=None,
                 snap_name=None, retained=False, writer=None):
        '''
        tco -> test class object
        grp_name -> group name/subvol group name
        snap_name -> snapshot name
        retailed -> has subvol been removed and snap has been retained
        '''
        self.tco = tco
        self._get_tco_stuff()

        self.volname = volname if volname else self.tco.volname

        self.grp_name = grp_name
        if self.grp_name is None:
            self.grp_name = self.grp_name
        elif self.grp_name is True:
            self.grp_name = self._gen_subvol_grp_name()
        elif type(self.grp_name) is str:
            pass
        else:
            raise RuntimeError('self.grp_name can be only str, True or None'
                               f'self.grp_name = {self.grp_name} '
                               f'type(self.grp_name) = {type(self.grp_name)}')

        self.name = name if name else self._gen_subvol_name()
        self.snap_name = snap_name if snap_name else self._gen_subvol_snap_name()
        self.retained = retained
        self._verify_attrs()

        self._define_paths()
        self.writer = writer

    def _get_tco_stuff(self):
        '''
        Get needed attrs/methods into class namespace so that using them is as
        straighforward as always.
        '''
        # stuff from CephFSTestCase
        self.fs = self.tco.fs
        self.mount_a = self.tco.mount_a
        self.mount_b = self.tco.mount_b
        self.create_client = self.tco.create_client

        # stuff from CephTestCase
        self.run_ceph_cmd = self.tco.run_ceph_cmd
        self.get_ceph_cmd_stdout = self.tco.get_ceph_cmd_stdout

        # stuff from VolumesHelper
        self.volname = self.tco.volname
        self._gen_name = self.tco._gen_name
        self._gen_subvol_name = self.tco._gen_subvol_name
        self._gen_subvol_snap_name = self.tco._gen_subvol_snap_name
        self._gen_subvol_grp_name = self.tco._gen_subvol_grp_name

        # stuff from unittest
        self.assertIn = self.tco.assertIn
        self.assertEqual = self.tco.assertEqual

    def _verify_attrs(self):
        msg = f'self.volname = {self.volname}'
        assert isinstance(self.volname, str), msg
        msg = f'self.grp_name = {self.grp_name}'
        assert self.grp_name is None or type(self.grp_name) is str, msg
        msg = f'self.name = {self.name}'
        assert isinstance(self.name, str), msg

        msg = f'self.snap_name = {self.snap_name}'
        assert self.snap_name is None or type(self.snap_name) is str, msg
        msg = f'self.retained = {self.retained}'
        assert self.retained in (True, False), msg

    def _define_paths(self):
        grp_name = self.grp_name if self.grp_name else '_nogroup'

        self.subvol_path = join('volumes', grp_name, self.name)
        self.meta_path = join(self.subvol_path, '.meta')

    def get_uuid(self):
        raise NotImplementedError()

    @property
    def data_path(self):
        '''
        path to the dir where user's subvol data is
        '''
        raise NotImplementedError()

    def remove(self, wait=False):
        sv_rm_cmd = f'fs subvolume rm {self.volname} {self.name}'
        if self.grp_name:
            sv_rm_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(sv_rm_cmd)

    def sanity_test_subvol(self):
        sv_ls_cmd = f'fs subvolume ls {self.volname}'
        if self.grp_name:
            sv_ls_cmd += f' --group-name {self.grp_name}'

        subvols = self.get_ceph_cmd_stdout(sv_ls_cmd)
        subvols = json_loads(subvols)
        assert {'name': self.name} in subvols

    def sanity_test_v2_snap(self):
        assert self.snap_name

        ss_ls_cmd = (f'fs subvolume snapshot ls {self.volname} {self.name}')
        if self.grp_name:
            ss_ls_cmd += f' --group-name {self.grp_name}'

        snap_names = self.get_ceph_cmd_stdout(ss_ls_cmd)
        snap_names = json_loads(snap_names)
        self.assertIn({'name': self.snap_name}, snap_names)

        v2_snap_path = f'volumes/{self.grp_name}/{self.name}/.snap'
        self.mount_a.run_shell(f'stat {v2_snap_path}')

    def sanity_test_retained_subvol(self):
        assert self.retained

        raise NotImplementedError()

    def sanity_test_retained_snap(self):
        assert self.retained

        raise NotImplementedError()

    def remove_snap(self):
        snap_rm_cmd = (f'fs subvolume snapshot rm {self.volname} {self.name} '
                       f'{self.snap_name}')
        if self.grp_name:
            snap_rm_cmd += f' --group-name {self.grp_name}'

        self.run_ceph_cmd(snap_rm_cmd)

    def create_subvol_client_and_remount(self):
        keyring = self.create_client(
                self.CLIENT_ID, moncap='allow r',
                osdcap=f'allow rw pool={self.fs.data_pool_name}',
                mdscap=f'allow rw path=/{self.data_path}')

        self.mount_b.remount(client_id=self.CLIENT_ID,
                                           client_keyring=keyring,
                                           cephfs_mntpt='/'+self.data_path)

    def gen_io_load_via_fs_client(self):
        self.writer = self.mount_b.gen_io_load('/')

    def gen_io_load_via_subvol_client(self):
        self.create_subvol_client_and_remount()
        self.writer = self.mount_b.gen_io_load('/')


class SubvolV2Helper(SubvolHelper):
    '''
    Helper class for v2 subvolumes.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, grp_name=None,
                 snap_name=None, retained=False, writer=None):
        super(SubvolV2Helper, self).__init__(tco=tco, volname=volname,
                                             name=name, uuid=uuid,
                                             grp_name=grp_name, writer=writer,
                                             snap_name=snap_name,
                                             retained=retained)

        self.uuid = uuid if uuid else str(uuid4())

    def _define_paths(self):
        super()._define_paths()

        self.uuid_path = join(self.subvol_path, self.uuid)
        self.snap_base_path = join(self.uuid_path, '.snap')
        if self.snap_name:
            self.snap_path = join(self.snap_base_path, self.snap_name)

    def _verify_attr(self):
        super()._verify_attrs()

        msg = f'self.uuid = {self.uuid}'
        assert isinstance(self.uuid, str), msg

        msg = f'self.snap_base_path = {self.snap_base_path}'
        assert type(self.snap_base_path) is str, msg

    @property
    def data_path(self):
        return self.uuid_path

    def custom_create(self):
        '''
        Create mock v2 subvol for testing upgrade.
        '''
        self.mount_a.run_shell(f'mkdir -p {self.data_path}')

        self.mount_a.write_file(self.meta_path, dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = /{self.data_path}
            state = complete'''), sudo=True)

        # so that files can accessed without unnecessary hassles
        self.mount_a.run_shell(f'chmod -R 755 {self.data_path}')

        if self.snap_name:
            self.mount_a.run_libcephfs_pybind_code(dedent(f"""
            from time import sleep
            sleep(2)
            cephfs.mksnap('{self.data_path}', '{self.snap_name}', 0o755)
            """))

            # verify that snap was created
            self.mount_a.run_shell(f'stat {self.snap_path}')

        if self.retained:
            raise NotImplementedError()

    def upgrade_to_v3(self, has_v2_snaps=False):
        '''
        :param has_v2_snaps: indicate whether v2 subvol had snaps and therefore
                             whether v3 subvol will have to deal with it
        '''
        getpath_cmd = f'fs subvolume getpath {self.volname} {self.name}'
        if self.grp_name:
            getpath_cmd += f' --group-name {self.grp_name}'

        # XXX: this will trigger subvol upgrade
        getpath_cmd_output = self.get_ceph_cmd_stdout(getpath_cmd).\
                strip()

        uuid = None if has_v2_snaps else self.uuid
        v3 = SubvolV3Helper(tco=self.tco, volname=self.volname, name=self.name,
                            uuid=uuid, grp_name=self.grp_name,
                            writer=self.writer, snap_name=self.snap_name,
                            retained=self.retained,
                            has_v2_snaps=has_v2_snaps)

        msg = (f'apparently subvol upgrade for {self.name} from v2 to v3 '
               'passed (because there was no crash) but output of getpath cmd '
               'is incorrect')
        self.assertEqual(getpath_cmd_output, f'/{v3.mnt_path}', msg)

        return v3


class SubvolV3Helper(SubvolHelper):
    '''
    Helper class for v3 subvolumes.
    '''

    def __init__(self, tco, volname=None, name=None, uuid=None, grp_name=None,
                 snap_name=None, retained=False, writer=None,
                 has_v2_snaps=False):
        super(SubvolV3Helper, self).__init__(tco=tco, volname=volname,
                                             name=name, uuid=uuid,
                                             grp_name=grp_name, writer=writer,
                                             snap_name=snap_name,
                                             retained=retained)

        self.uuid = uuid if uuid else self.get_uuid()
        self.has_v2_snaps = has_v2_snaps

    def _verify_attrs(self):
        super(SubvolV3Helper, self)._verify_attrs()

        msg = f'self.uuid = {self.uuid}'
        assert isinstance(self.uuid, str), msg
        msg = f'self.snap_base_path = {self.snap_base_path}'
        assert type(self.snap_base_path) is str, msg

    def _define_paths(self):
        super()._define_paths()

        self.roots_path = join(self.subvol_path, 'roots')
        self.uuid_path = join(self.roots_path, self.uuid)
        self.mnt_path = join(self.uuid_path, 'mnt')

        self.snap_base_path = join(self.uuid_path, '.snap')
        if self.snap_name:
            self.snap_path = join(self.snap_base_path, self.snap_name)

    def get_uuid(self):
        # self.roots_path might not be defined during times like __init__()
        roots_path = join('volumes', self.grp_name, self.name, 'roots')
        uuid = self.mount_a.get_shell_stdout(f'ls {roots_path}')
        ensure_uuid_is_valid(uuid)
        return uuid

    @property
    def data_path(self):
        return self.mnt_path

    def verify_meta_file(self):
        '''
        Verify that subvolume and its metadata file are how they should be for
        a v3 subvolume.
        '''
        meta_content = self.mount_a.get_shell_stdout(f'sudo cat {self.meta_path}')

        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = /{self.mnt_path}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        if self.has_v2_snaps:
            self.assertIn('has_v2_snaps = True', meta_content)


class TestWithoutIoLoad(VolumesHelper):
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
        v2.custom_create()
        v2.sanity_test_subvol()

        v3 = v2.upgrade_to_v3()

        v3.verify_meta_file()
        v3.sanity_test_subvol()

        v3.remove()
        self._wait_for_trash_empty()

    def test_custom_group_subvol(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in a
        non-default subvol group.
        '''
        v2 = SubvolV2Helper(tco=self, grp_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        v3 = v2.upgrade_to_v3()
        v3.verify_meta_file()
        v3.sanity_test_subvol()

        v3.remove()
        self._wait_for_trash_empty()

    def test_subvol_with_snap(self):
        pass

    def test_subvol_with_retained_snap(self):
        pass

    def test_subvol_with_v2_snap(self):
        '''
        Test subvol upgrade from v2 to v3 when it has a snap.
        '''
        v2 = SubvolV2Helper(tco=self, grp_name=True, snap_name=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        v3 = v2.upgrade_to_v3(True)
        v3.verify_meta_file()
        v3.sanity_test_subvol()
        # rishabh, start here: because v2 snap is not listed by "snap ls" cmd
        v3.sanity_test_v2_snap()

        v3.remove_snap()
        v3.remove()
        self._wait_for_trash_empty()

    def _test_subvol_with_retained_v2_snap(self):
        v2 = SubvolV2Helper(tco=self, grp_name=True, snap_name=True,
                            retained=True)
        v2.custom_create()
        v2.sanity_test_subvol()

        v3 = v2.upgrade_to_v3(True)
        v3.verify_meta_file()
        v3.sanity_test_subvol()
        v3.sanity_test_retained_subvol()
        v3.sanity_test_retained_snap()

        v3.remove_snap()
        v3.remove()
        self._wait_for_trash_empty()


class TestUnderIoLoad(VolumesHelper):
    '''
    Test subvol upgrade from v2 to v3 while IO is being performed on the subvol.
    '''

    def test_regular_basic_subvol_with_workload_via_fs_client(self):
        '''
        Test subvol upgrade from v2 to v3 when subvol is located in the
        default subvol group and the subvol is under IO load.
        '''
        v2 = SubvolV2Helper(tco=self)
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load_via_fs_client()
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(20)

        v3 = v2.upgrade_to_v3()
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        # XXX writer threads shouldn't die or be affected due to upgrade
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()

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
        v2.custom_create()
        v2.sanity_test_subvol()

        v2.gen_io_load_via_fs_client()
        log.info('giving 60 seconds for background threads for writing...')
        time_sleep(20)

        v3 = v2.upgrade_to_v3()
        msg = ('thread writing on subvol via client crashed while subvol '
               f'{v3.name} was being upgraded from v2 to v3')
        # XXX writer threads shouldn't die or be affected due to upgrade
        self.assertEqual(v3.writer.is_alive(), True, msg)

        v3.verify_meta_file()
        v3.sanity_test_subvol()

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
