'''
Helper code for operating on subvolume v3, v2 and v1.
'''
from logging import getLogger
from textwrap import dedent
from errno import ENOENT
from os.path import join, basename, dirname
from uuid import uuid4, UUID
from json import loads

from tasks.cephfs.helpers.misc import (gen_subvol_name, gen_subvol_grp_name,
                                       gen_subvol_snap_name)
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.ceph_manager import RunCephCmd


log = getLogger(__name__)


VOL_BASE_PATH = '/volumes'
DEFAULT_GRP_NAME = '_nogroup'
META_FILE_NAME = '.meta'
SNAP_BASE_DIR = '.snap'
# v3 incarnation layout (see SubvolV3Helper / V3IncarHelper)
INCAR_BASE_DIR = 'roots'
MNT_DIR = 'mnt'
RETAINED_DIR = '.retained'
# as mgr/volumes VolSpec.DEFAULT_POOL_NS_PFX; SubvolumeBase.namespace is
# f'{pfx}_{group}_{name}' (so '_nogroup' yields 'fsvolumens__nogroup_...').
POOL_NS_PFX = 'fsvolumens_'
EARMARK_XATTR = 'user.ceph.subvolume.earmark'


def gen_uuid():
    return str(uuid4())


def verify_uuid(uuid):
    assert type(uuid) is str, \
        f'uuid = {uuid} type(uuid) = {type(uuid)}'

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


# ----- pure path / layout helpers (no Mount / CephFSTestCase) -----
#
# Shared by unittest helpers and the teuthology volumes_upgrade task.
# base_path may be absolute (/volumes/...) or relative (volumes/...);
# join() preserves whichever form the caller passes.


def subvol_base_path(name, grp_name=None):
    grp = grp_name if grp_name else DEFAULT_GRP_NAME
    return join(VOL_BASE_PATH, grp, name)


def subvol_pool_namespace(name, grp_name=None):
    """
    RADOS namespace as mgr/volumes names it (SubvolumeBase.namespace).
    """
    grp = grp_name if grp_name else DEFAULT_GRP_NAME
    return f'{POOL_NS_PFX}_{grp}_{name}'


def as_relpath(path):
    """Strip a leading slash for paths relative to an fs-root mount cwd."""
    return path.lstrip('/')


def subvol_uuid_path(base_path, uuid):
    return join(base_path, uuid)


def subvol_meta_path(base_path, meta_name=META_FILE_NAME):
    return join(base_path, meta_name)


def subvol_v3_meta_file_name(uuid):
    return f'.meta.{uuid}'


def subvol_v3_mnt_path(base_path, uuid):
    return join(base_path, INCAR_BASE_DIR, uuid, MNT_DIR)


def subvol_v3_retained_path(base_path, uuid):
    return join(base_path, INCAR_BASE_DIR, uuid, RETAINED_DIR)


def subvol_v1_snap_path(base_path, uuid, snap_name, upgraded=False):
    """
    Path of a v1 snapshot. Pre-upgrade it lives under the data (uuid) dir;
    after auto-upgrade it moves with the data dir under roots/<uuid>/mnt.
    """
    if upgraded:
        return join(base_path, INCAR_BASE_DIR, uuid, MNT_DIR,
                    SNAP_BASE_DIR, snap_name)
    return join(base_path, uuid, SNAP_BASE_DIR, snap_name)


def subvol_v2_snap_data_path(base_path, uuid, snap_name):
    """
    Path of a v2 snapshot's data. Taken a level above the data dir and stays
    there across auto-upgrade.
    """
    return join(base_path, SNAP_BASE_DIR, snap_name, uuid)


def subvol_pre_v3_snap_path(version, base_path, uuid, snap_name,
                            upgraded=False):
    if version == 1:
        return subvol_v1_snap_path(base_path, uuid, snap_name, upgraded)
    if version == 2:
        return subvol_v2_snap_data_path(base_path, uuid, snap_name)
    raise ValueError(f'unsupported pre-v3 version {version}')


def gen_pre_v3_meta(version, mnt_path, state='complete', metadata=None):
    """
    Contents of a pre-v3 .meta file. mnt_path should be the absolute path
    stored in GLOBAL.path (leading slash).
    """
    if not mnt_path.startswith('/'):
        mnt_path = '/' + mnt_path
    text = (f'[GLOBAL]\nversion = {version}\ntype = subvolume\n'
            f'path = {mnt_path}\nstate = {state}\n')
    if metadata:
        text += '\n[USER_METADATA]\n'
        text += ''.join(f'{k} = {v}\n' for k, v in metadata.items())
    return text


def fabricate_v1_subvol_script(data_path, pool, *, namespace=None, size=None,
                               earmark=None, uid=0, gid=0, mode=0o755,
                               metadata=None):
    """
    Bash script that fabricates a v1 subvolume the way ceph-mgr used to
    (SubvolumeV1.create): data dir at data_path, .meta sibling under the
    subvolume base. data_path is relative to the fs root (or absolute).
    """
    data_path = as_relpath(data_path)
    base_path = dirname(data_path)
    meta = gen_pre_v3_meta(1, data_path, metadata=metadata)
    # escape for embedding in a double-quoted printf
    meta_escaped = meta.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
    cmds = [
        'set -e',
        f'mkdir -p -m 755 {data_path}',
        f'setfattr -n ceph.dir.layout.pool -v {pool} {data_path}',
        f'setfattr -n ceph.dir.subvolume -v 1 {data_path}',
    ]
    if namespace:
        cmds.append('setfattr -n ceph.dir.layout.pool_namespace '
                    f'-v {namespace} {data_path}')
    if size is not None:
        cmds.append(f'setfattr -n ceph.quota.max_bytes -v {size} {data_path}')
    if earmark:
        cmds.append(f'setfattr -n {EARMARK_XATTR} -v {earmark} {data_path}')
    cmds += [
        f'chown {uid}:{gid} {data_path}',
        f'chmod {mode:o} {data_path}',
        f'printf "{meta_escaped}" > {base_path}/{META_FILE_NAME}',
    ]
    return '\n'.join(cmds)


class SubvolAutoUpgrade(CephFSTestCase):
    '''
    Helpers for testing subvol auto-upgrades.
    '''

    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()

        # mount through which tests will create, auto-upgrade and run sanity
        # tests for subvol.
        self.admin_mount = self.mount_a
        # mount through which tests will mimic subvol users by constantly
        # writing files to the subvol
        self.user_mount = self.mount_b

        # removing these default names to avoid confusion reader and potential
        # mixup which can be fatal for test code.
        del self.mount_a, self.mount_b

    def tearDown(self):
        self.show_cephfs_dir_tree()
        if hasattr(self, 'writer'):
            self.writer.stop()

        super().tearDown()

    def show_cephfs_dir_tree(self):
        '''
        Incase of test failure, log dir tree for mount since it will help
        understand failure.
        '''
        if hasattr(self, '_outcome') and hasattr(self._outcome, 'result'):
            result = self._outcome.result
            for test, _ in result.errors + result.failures:
                if test == self:
                    failed = True
                    break
            else:
                failed = False

        if failed:
            self.user_mount.run_shell('tree -d ./')
            self.user_mount.run_shell('tree -L 2 ./')

    def raise_method(self, msg):
        '''
        Pass this method to GenIOLoad instance to cause tests too to fail when
        writer threads fail.
        '''
        log.error(msg)
        raise RuntimeError(msg)

    def wait_till_trash_is_empty(self, timeout=60):
        self.admin_mount.wait_for_dir_empty('volumes/_deleting', timeout)


class SubvolOpsHelper(RunCephCmd):
    '''
    Helper methods for running 'ceph fs subvolume' commands.
    '''

    def __init__(self, mon_manager):
        super().__init__(mon_manager)


    # ----- helpers for subvol ops -----


    def create(self, volname, subvol_name, grp_name=None, opts=''):
        cmd = f'fs subvolume create {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if opts:
            cmd += f' {opts}'

        return self.run_ceph_cmd(cmd)

    def rm(self, volname, subvol_name, grp_name=None, opts=''):
        cmd = f'fs subvolume rm {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if opts:
            cmd += f' {opts}'

        return self.run_ceph_cmd(cmd)

    def getpath(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume getpath {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def try_getpath(self, volname, subvol_name, grp_name=None):
        """
        Like getpath, but returns (exitstatus, path_or_empty) without raising.
        """
        from io import StringIO
        cmd = f'fs subvolume getpath {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        p = self.run_ceph_cmd(args=cmd, check_status=False, stdout=StringIO())
        return p.exitstatus, p.stdout.getvalue().strip()

    def info(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume info {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def exists(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume info {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return self.get_ceph_cmd_result(args=cmd, check_status=False) == 0

    def ls(self, volname, grp_name=None):
        cmd = f'fs subvolume ls {volname}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))

    def metadata_ls(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume metadata ls {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def metadata_set(self, volname, subvol_name, key, value, grp_name=None):
        cmd = (f'fs subvolume metadata set {volname} {subvol_name} '
               f'{key} {value}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return self.run_ceph_cmd(cmd)

    def auth(self, volname, subvol_name, client_id, grp_name=None,
             access_level=None):
        cmd = f'fs subvolume authorize {volname} {subvol_name} {client_id}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if access_level:
            cmd += f' --access_level {access_level}'

        self.run_ceph_cmd(cmd)

    def authorized_list(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume authorized_list {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def deauthorize(self, volname, subvol_name, client_id, grp_name=None):
        cmd = (f'fs subvolume deauthorize {volname} {subvol_name} '
               f'{client_id}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return self.run_ceph_cmd(cmd)

    def evict(self, volname, subvol_name, client_id, grp_name=None):
        cmd = f'fs subvolume evict {volname} {subvol_name} {client_id}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return self.run_ceph_cmd(cmd)

    def auth_get(self, client_id):
        if not client_id.startswith('client.'):
            client_id = f'client.{client_id}'
        return self.get_ceph_cmd_stdout(f'auth get {client_id}')

    def auth_rotate_aes(self, client_id):
        """
        Kernel clients on older releases may not support newer key types.
        """
        if not client_id.startswith('client.'):
            client_id = f'client.{client_id}'
        try:
            self.run_ceph_cmd(f'auth rotate {client_id} --key-type=aes')
        except Exception:
            # rotate is best-effort (same as the kclient / volumes_upgrade tasks)
            pass


    # ----- helpers for subvol snap ops -----


    def snap_create(self, volname, subvol_name, snap_name=None, grp_name=None):
        snap_name = snap_name if snap_name else gen_subvol_snap_name()
        cmd = (f'fs subvolume snapshot create {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        self.run_ceph_cmd(cmd)
        return snap_name

    def snap_rm(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = f'fs subvolume snapshot rm {volname} {subvol_name} {snap_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.run_ceph_cmd(cmd)

    def snap_getpath(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot getpath {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def snap_ls(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume snapshot ls {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))

    def snap_info(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot info {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def snap_metadata_ls(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot metadata ls {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def snap_metadata_set(self, volname, subvol_name, snap_name, key, value,
                          grp_name=None):
        cmd = (f'fs subvolume snapshot metadata set {volname} {subvol_name} '
               f'{snap_name} {key} {value}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return self.run_ceph_cmd(cmd)

    def snap_clone(self, volname, subvol_name, snap_name, target_name,
                   grp_name=None, target_grp_name=None):
        cmd = (f'fs subvolume snapshot clone {volname} {subvol_name} '
               f'{snap_name} {target_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if target_grp_name:
            cmd += f' --target_group_name {target_grp_name}'
        return self.run_ceph_cmd(cmd)

    def clone_status(self, volname, clone_name, grp_name=None):
        cmd = f'fs clone status {volname} {clone_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        return loads(self.get_ceph_cmd_stdout(cmd))

    def group_create(self, volname, group, opts=''):
        cmd = f'fs subvolumegroup create {volname} {group}'
        if opts:
            cmd += f' {opts}'
        return self.run_ceph_cmd(cmd)

    def group_info(self, volname, group):
        return loads(self.get_ceph_cmd_stdout(
            f'fs subvolumegroup info {volname} {group}'))


class SubvolBase(SubvolOpsHelper):
    '''
    Base class for v3, v2 and v1 subvol helper classes.
    '''


    # ----- init and related methods -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snap_names=[], retained=False):
        '''
        :param grp_name: should be None or True.
                            * True implies set self.grp_name to generated name
                            * None/False implies set self.grp_name to no/default group
        :param uuid: should be str, None, or True.
                          * str implies set self.uuid to passed str,
                          * True implies set self.uuid to generated uuid
                          * None/False implies set self.uuid to None
        '''
        self.mon_manager = mon_manager
        self.mount = mount
        super().__init__(self.mon_manager)

        self.volname = volname
        self.grp_name = gen_subvol_grp_name() if grp_name is True else grp_name
        self.name = name if name else gen_subvol_name()

        self.uuid = uuid
        if self.uuid is True:
            self.uuid = gen_uuid()
        elif self.uuid in (None, False):
            self.uuid = None
        elif type(self.uuid) is str:
            self.uuid = uuid
        else:
            assert False

        self.snap_names = snap_names
        self.retained = retained

        # NOTE subclasses should call these methods at end of their __init__()
        # self._reset_paths()
        # self._verify_attrs()
        return

    def _verify_attrs(self):
        assert type(self.volname) is str, \
            f'self.volname = {self.volname}'
        assert type(self.grp_name) in (type(None), str), \
            f'self.grp_name = {self.grp_name}'
        assert type(self.name) is str, \
            f'self.name = {self.name}'

    def _reset_paths(self):
        self._base_path = None
        self._meta_path = None


    # ----- subvol basics -----


    @property
    def version(self):
        raise NotImplementedError('should\'ve been overridden')

    def gen_base_path(self):
        return subvol_base_path(self.name, self.grp_name)


    # ----- helpers for subvol ops -----


    def create(self, opts='', default_mode=True):
        # unittest helpers historically default to mode 777; callers that
        # want mgr's default (0755) can pass default_mode=False or an
        # explicit --mode in opts.
        if default_mode and 'mode' not in opts:
            opts = opts + ' --mode=777'

        return super().create(self.volname, self.name, self.grp_name, opts)

    def rm(self, opts=''):
        return super().rm(self.volname, self.name, self.grp_name, opts)

    def getpath(self):
        return super().getpath(self.volname, self.name, self.grp_name).\
                strip()

    def try_getpath(self):
        return super().try_getpath(self.volname, self.name, self.grp_name)

    def info(self):
        return super().info(self.volname, self.name, self.grp_name)

    def exists(self):
        return super().exists(self.volname, self.name, self.grp_name)

    def ls(self):
        return super().ls(self.volname, self.grp_name)

    def metadata_ls(self):
        return super().metadata_ls(self.volname, self.name, self.grp_name)

    def metadata_set(self, key, value):
        return super().metadata_set(self.volname, self.name, key, value,
                                    self.grp_name)

    def auth(self, client_id, access_level=None):
        return super().auth(self.volname, self.name, client_id, self.grp_name,
                            access_level=access_level)

    def authorized_list(self):
        return super().authorized_list(self.volname, self.name, self.grp_name)

    def deauthorize(self, client_id):
        return super().deauthorize(self.volname, self.name, client_id,
                                   self.grp_name)

    def evict(self, client_id):
        return super().evict(self.volname, self.name, client_id, self.grp_name)

    def authorize(self, client_id, access_level='rw', rotate_aes=True):
        """
        Authorize client_id for this subvolume and return its keyring.
        """
        assert 'client.' not in client_id
        self.auth(client_id, access_level=access_level)
        if rotate_aes:
            self.auth_rotate_aes(client_id)
        return self.auth_get(client_id)

    def set_uuid_from_path(self, path=None):
        """
        Update self.uuid from a getpath-style path (.../<uuid> or
        .../roots/<uuid>/mnt).
        """
        path = path if path is not None else self.getpath()
        path = path.rstrip('/')
        if path.endswith('/' + MNT_DIR):
            # v3: /volumes/.../roots/<uuid>/mnt
            self.uuid = basename(dirname(path))
        else:
            self.uuid = basename(path)
        verify_uuid(self.uuid)
        self._reset_paths()
        return self.uuid

    def set_uuid_from_data_path(self, data_path):
        """Set uuid from a relative/absolute pre-v3 data path (.../<uuid>)."""
        self.uuid = data_path.rstrip('/').rsplit('/', 1)[1]
        verify_uuid(self.uuid)
        self._reset_paths()
        return self.uuid


    # ----- helpers for subvol snap ops -----


    def snap_create(self, snap_name=None):
        snap_name = super().snap_create(self.volname, self.name, snap_name,
                                        self.grp_name)
        return snap_name

    def snap_rm(self, snap_name):
        super().snap_rm(self.volname, self.name, snap_name, self.grp_name)

    def snap_getpath(self, snap_name):
        return super().snap_getpath(self.volname, self.name, snap_name,
                                    self.grp_name).strip()

    def snap_ls(self):
        return super().snap_ls(self.volname, self.name, self.grp_name)

    def snap_info(self, snap_name):
        return super().snap_info(self.volname, self.name, snap_name,
                                 self.grp_name)

    def snap_metadata_ls(self, snap_name):
        return super().snap_metadata_ls(self.volname, self.name, snap_name,
                                        self.grp_name)

    def snap_metadata_set(self, snap_name, key, value):
        return super().snap_metadata_set(self.volname, self.name, snap_name,
                                         key, value, self.grp_name)

    def snap_clone(self, snap_name, target_name, target_grp_name=None):
        return super().snap_clone(self.volname, self.name, snap_name,
                                  target_name, self.grp_name, target_grp_name)

    def clone_status(self):
        return super().clone_status(self.volname, self.name, self.grp_name)

    def group_create(self, group, opts=''):
        return super().group_create(self.volname, group, opts)

    def group_info(self, group):
        return super().group_info(self.volname, group)


    # ----- helpers for operating on subvol/subvol mnt path -----


    def write_files(self, path=None, data='00000', num=1, ifx=''):
        if path:
            if self.gen_mnt_path() not in path:
                path = path[1:] if path[0] == '/' else path
                path = join('/', self.gen_mnt_path(), path)
        else:
            path = './' + self.gen_mnt_path()

        for i in range(num):
            file_name = f'file-{ifx}-{i}' if ifx else f'file-{i}'
            file_path = join(path, file_name)
            self.mount.write_file(file_path, data, sudo=True)


    # ----- helpers for custom create -----


    def apply_create_options(self, options=''):
        if not options:
            return

        assert type(options) is str
        options = options.split()
        xattrs = {}

        if '--size' in options:
            size = options[options.index('--size') + 1]
            # testing that value passed to "--size" is indeed a number
            size = int(size)
            xattrs['ceph.quota.max_bytes'] = str(size)

        if '--namespace-isolated' in options:
            xattrs['ceph.dir.layout.pool_namespace'] = \
                subvol_pool_namespace(self.name, self.grp_name)

        for k, v in xattrs.items():
            self.mount.run_shell(
                f'setfattr -n {k} -v {v} ./{self.gen_mnt_path()}')


    # ----- helpers for sanity testing ------


    # XXX: do _NOT_ open subvol via volumes plugin commands.
    #
    # this is duplication to prevent accidental subvol opening as it will
    # trigger auto-upgrade for the subvol before it's desired.
    def verify_custom_made_subvol(self):
        ls_stdout = self.ls()
        assert self.name in str(ls_stdout), \
            f'snap_name = {self.name} ls_stdout = {ls_stdout}'

    # XXX: do _NOT_ open subvol via volumes plugin commands.
    #
    # this is duplication to prevent accidental subvol opening as it will
    # trigger auto-upgrade for the subvol before it's desired.
    def verify_custom_made_snap(self, snap_name):
        snap_path = self.gen_snap_path(snap_name)
        self.mount.run_shell(f'stat {snap_path[1:]}')

    def verify_subvol_ls_cmd(self):
        ls_stdout = self.ls()
        assert self.name in str(ls_stdout), \
            f'snap_name = {self.name} ls_stdout = {ls_stdout}'

    def verify_subvol_getpath_cmd(self):
        if self.retained:
            self.negtest_ceph_cmd(f'fs subvolume getpath {self.volname} '
                                  f'{self.name}', retval=ENOENT)
            return

        sv_path = self.getpath()
        assert sv_path == self.gen_mnt_path(), \
            f'sv_path = {sv_path} self.gen_mnt_path() = {self.gen_mnt_path()}'

    def verify_snap_ls_cmd(self, snap_name):
        ls_stdout = self.snap_ls()
        assert snap_name in str(ls_stdout), \
            f'snap_name = {snap_name} ls_stdout = {ls_stdout}'

    def verify_snap_getpath_cmd(self, snap_name):
        exp_snap_path = self.gen_snap_path(snap_name)
        reported_snap_path = self.snap_getpath(snap_name)

        assert reported_snap_path == exp_snap_path, \
            (f'reported_snap_path = {reported_snap_path} '
             f'exp_snap_path = {exp_snap_path}')


class V1V2Base(SubvolBase):
    '''
    Base class for SubvolV1 and SubvolV2.
    '''

    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, snap_names=[], retained=False):
        super().__init__(mon_manager=mon_manager, mount=mount, volname=volname,
                         name=name, uuid=uuid, grp_name=grp_name,
                         snap_names=snap_names, retained=retained)

        # SubvolBase already resolves uuid=True -> str; only invent one when
        # still unset (do not overwrite the resolved value with the raw arg).
        if not self.uuid:
            self.uuid = gen_uuid()

        self._reset_paths()
        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._uuid_path = None
        self._snap_base_path = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) in (str, bool, type(None)), \
            f'self.uuid = {self.uuid}'


    # ----- subvol basics -----


    @property
    def version(self):
        raise NotImplementedError('derived class should override this method')


    # ----- v1/v2 helpers -----


    def gen_uuid_path(self):
        if not self._uuid_path:
            self._uuid_path = subvol_uuid_path(self.gen_base_path(), self.uuid)
        return self._uuid_path

    def gen_meta_path(self):
        if not self._meta_path:
            self._meta_path = subvol_meta_path(self.gen_base_path())
        return self._meta_path

    def gen_snap_base_path(self):
        raise NotImplementedError('derived class should override this method')

    def gen_snap_path(self, snap_name):
        return join(self.gen_snap_base_path(), snap_name)


    # ----- create stuff without create command ------


    def custom_create(self):
        raise NotImplementedError('derived class should override this method')

    def custom_create_snap(self):
        raise NotImplementedError('derived class should override this method')

    def custom_subvol_auth(self, client_id, data_pool_name, namespace=None,
                           rotate_aes=True):
        """
        Caps matching `fs subvolume authorize` on a pre-v3 subvolume.
        """
        assert 'client.' not in client_id

        subvol_mnt_path = self.gen_mnt_path()
        if subvol_mnt_path[0] != '/':
            subvol_mnt_path = '/' + subvol_mnt_path

        ns = f' namespace={namespace}' if namespace else ''
        client_name = f'client.{client_id}'
        self.run_ceph_cmd(
            f'auth get-or-create {client_name} mon "allow r" '
            f'mds "allow rw path={subvol_mnt_path}" '
            f'osd "allow rw pool={data_pool_name}{ns}"')
        if rotate_aes:
            self.auth_rotate_aes(client_id)
        return self.auth_get(client_id)


class SubvolV1Helper(V1V2Base):
    '''
    Helper class for v1 subvolumes.
    '''


    # ----- subvol basics -----


    @property
    def version(self):
        return 1

    def gen_mnt_path(self):
        return subvol_uuid_path(self.gen_base_path(), self.uuid)

    def gen_snap_base_path(self):
        if not self._snap_base_path:
            self._snap_base_path = join(self.gen_uuid_path(), SNAP_BASE_DIR)
        return self._snap_base_path

    def gen_snap_path(self, snap_name):
        return subvol_v1_snap_path(self.gen_base_path(), self.uuid, snap_name)


    # ----- override subvol op methods ------


    def create(self, opts='', default_mode=True):
        super().create(opts, default_mode=default_mode)

    def rm(self, opts=''):
        super().rm(opts)

    def snap_create(self, snap_name=None):
        super().snap_create(snap_name)
        self.snap_names.append(snap_name)
        return snap_name

    def snap_rm(self, snap_name):
        super().snap_rm(snap_name)
        self.snap_names.remove(snap_name)


    # ----- create stuff without create command ------


    def custom_create(self, create_options='', *, pool=None, uid=0, gid=0,
                      mode=0o777, earmark=None, metadata=None,
                      populate_files=True):
        '''
        For testing subvol auto-upgrade, create v1 subvol without using
        'subvolume create' command.

        When pool is given, fabricate via fabricate_v1_subvol_script (full
        fidelity to SubvolumeV1.create, including mark_subvolume). Otherwise
        keep the lightweight unittest path (mkdir + optional create_options).
        '''
        self.uuid = gen_uuid()
        self._reset_paths()

        namespace = None
        size = None
        if create_options:
            opts = create_options.split()
            if '--size' in opts:
                size = int(opts[opts.index('--size') + 1])
            if '--namespace-isolated' in opts:
                namespace = subvol_pool_namespace(self.name, self.grp_name)

        if pool is not None:
            script = fabricate_v1_subvol_script(
                as_relpath(self.gen_mnt_path()), pool,
                namespace=namespace, size=size, earmark=earmark,
                uid=uid, gid=gid, mode=mode, metadata=metadata)
            self.mount.run_shell_payload(script, sudo=True)
        else:
            self.mount.run_shell(f'mkdir -p ./{self.gen_mnt_path()}')
            # as SubvolumeV1.mark_subvolume()
            self.mount.run_shell(
                f'setfattr -n ceph.dir.subvolume -v 1 ./{self.gen_mnt_path()}')
            self.apply_create_options(create_options)
            self.mount.run_shell(f'chmod {mode:o} ./{self.gen_mnt_path()}')
            self.mount.write_file(as_relpath(self.gen_meta_path()),
                                  gen_pre_v3_meta(1, self.gen_mnt_path()),
                                  sudo=True)

        if populate_files:
            self.write_files(num=5, ifx='at-creation')

    def custom_create_snap(self, snap_name=None):
        '''
        For testing subvol auto-upgrade, create v1 snap subvol without using
        'subvolume snapshot create' command.
        '''
        snap_name = snap_name if snap_name else gen_subvol_snap_name()

        self.mount.run_libcephfs_pybind_code(dedent(f"""
            cephfs.mksnap('{self.gen_snap_base_path()}', '{snap_name}', 0o755)
        """))

        snap_path = self.gen_snap_path(snap_name)
        # verify that snap was created
        self.mount.run_shell(f'stat ./{snap_path}')

        self.snap_names.append(snap_name)
        return snap_name


class SubvolV2Helper(V1V2Base):
    '''
    Helper class for v2 subvolumes.
    '''


    # ----- basic subvol helpers -----


    @property
    def version(self):
        return 2

    def gen_mnt_path(self):
        return subvol_uuid_path(self.gen_base_path(), self.uuid)

    def gen_snap_base_path(self):
        if not self._snap_base_path:
            self._snap_base_path = join(self.gen_base_path(), SNAP_BASE_DIR)
        return self._snap_base_path

    # ----- subvol op methods -----


    def create(self, opts='', default_mode=True):
        super().create(opts, default_mode=default_mode)

        if self.retained:
            self.retained = False

    def rm(self, opts=''):
        super().rm(opts)

        if '--retain-snapshots' in opts:
            if not self.retained:
                self.retained = True

    def snap_create(self, snap_name=None):
        super().snap_create(snap_name)
        self.snap_names.append(snap_name)
        return snap_name

    def snap_rm(self, snap_name):
        super().snap_rm(snap_name)

        self.snap_names.remove(snap_name)
        if len(self.snap_names) == 0:
            if self.retained:
                # subvol has been deleted
                self.retained = False


    # ----- create stuff without create command ------


    def custom_create(self, create_options=''):
        '''
        For testing subvol auto-upgrade, Create v2 subvol without using
        'subvolume create' command.
        '''
        self.uuid = gen_uuid()

        self.mount.run_shell(f'mkdir -p ./{self.gen_mnt_path()}')
        # as SubvolumeV2.mark_subvolume() — on the base path for v2
        self.mount.run_shell(
            f'setfattr -n ceph.dir.subvolume -v 1 ./{self.gen_base_path()}')
        self.apply_create_options(create_options)
        self.mount.run_shell(f'chmod -R 777 ./{self.gen_mnt_path()}')

        self.mount.write_file(as_relpath(self.gen_meta_path()),
                              gen_pre_v3_meta(2, self.gen_mnt_path()),
                              sudo=True)

        self.write_files(num=5, ifx='at-creation')

    def custom_create_snap(self, snap_name=None):
        '''
        For testing subvol auto-upgrade, create v1 snap subvol without using
        'subvolume snapshot create' command.
        '''
        snap_name = snap_name if snap_name else gen_subvol_snap_name()

        self.mount.run_libcephfs_pybind_code(dedent(f"""
        cephfs.mksnap('{self.gen_snap_base_path()}', '{snap_name}', 0o755)
        """))

        # verify that snap was created
        self.mount.run_shell(f'stat ./{self.gen_snap_path(snap_name)}')

        self.snap_names.append(snap_name)
        return snap_name


class V3IncarHelper:
    '''
    Helps test/deal with v3 incarnations.
    '''

    INCAR_BASE_DIR = 'roots'
    MNT_DIR = 'mnt'
    RETAINED_DIR = '.retained'

    def __init__(self, subvol_name, uuid, snap_names=None, retained=False,
                 grp_name=None):
        self.subvol_name = subvol_name
        self.uuid = uuid
        self.grp_name = grp_name
        self.retained = retained

        self._base_path = None
        self._meta_path = None
        self._incar_path = None

        self.snap_names = snap_names
        if type(self.snap_names) is str:
            self.snap_names = [self.snap_names, ]
        elif type(self.snap_names) is list:
            pass
        elif type(self.snap_names) is tuple:
            self.snap_names = list(self.snap_names)
        elif self.snap_names in (None, [], (), ''):
            self.snap_names = []
        else:
            assert False, f'self.snap_names = {self.snap_names}'


class SubvolV3Helper(SubvolBase):
    '''
    Helper class for v3 subvolumes.
    '''

    INCAR_BASE_DIR = 'roots'
    MNT_DIR = 'mnt'
    RETAINED_DIR = '.retained'


    # ----- init and its helpers -----


    def __init__(self, mon_manager=None, mount=None, volname=None, name=None,
                 uuid=None, grp_name=None, incars=[], retained=False,
                 subvol=None):
        self._reset_paths()
        self.incars = []

        if subvol:
            assert subvol and not (mon_manager or mount or volname or name or
                                   uuid or grp_name or incars or retained)
            assert isinstance(subvol, (SubvolV2Helper, SubvolV1Helper))

            super().__init__(mon_manager=subvol.mon_manager, mount=subvol.mount,
                             volname=subvol.volname, name=subvol.name,
                             uuid=subvol.uuid, grp_name=subvol.grp_name,
                             snap_names=[], retained=subvol.retained)
            if isinstance(subvol, SubvolV2Helper):
                self.has_v2_snaps = (subvol.snap_names != [])
                self.v2_uuid = subvol.uuid if self.has_v2_snaps else None
                self.v1_uuid = None
                self.has_v1_snaps = False
            elif isinstance(subvol, SubvolV1Helper):
                self.has_v1_snaps = (subvol.snap_names != [])
                self.v1_uuid = subvol.uuid if self.has_v1_snaps else None
                self.has_v2_snaps = False
                self.v2_uuid = None
            else:
                assert False, f'unexpected pre-v3 helper {type(subvol)}'

            self.add_incar(self.uuid, subvol.snap_names, self.retained)
        else:
            assert mount and volname and not subvol

            super().__init__(mon_manager, mount=mount, volname=volname,
                             name=name, uuid=uuid if uuid else False,
                             grp_name=grp_name, snap_names=[],
                             retained=retained)

            self.has_v2_snaps = False
            self.has_v1_snaps = False
            self.v2_uuid = None
            self.v1_uuid = None

        self.retained = None
        del self.snap_names

        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._incar_base_path = None
        self._meta_symlink_path = None
        self._meta_file_name = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) in (str, type(None)), \
            f'self.uuid = {self.uuid}'
        assert self.retained is None , \
            f'self.retained = {self.retained}'

        assert type(self.incars) is list, \
            f'self.incars = {self.incars}'

        assert type(self.has_v2_snaps) is bool, \
            f'self.has_v2_snaps = {self.has_v2_snaps}'
        assert type(self.v2_uuid) in (str, type(None)), \
            f'self.v2_uuid = {self.v2_uuid}'

        assert type(self.has_v1_snaps) is bool, \
            f'self.has_v1_snaps = {self.has_v1_snaps}'
        assert type(self.v1_uuid) in (str, type(None)), \
            f'self.v1_uuid = {self.v1_uuid}'


    # ----- subvol helper -----


    @property
    def version(self):
        return 3


    # ----- v3 helpers -----


    def get_uuid_from_path(self, path):
        if self.MNT_DIR == basename(path):
            return basename(dirname(self.gen_mnt_path()))
        elif SNAP_BASE_DIR == basename(path):
            return basename(dirname(self.gen_snap_base_path()))
        elif SNAP_BASE_DIR in path:
            return basename(dirname(dirname(self.gen_snap_base_path())))
        else:
            assert False, 'unknown type of path'

    def gen_meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = subvol_meta_path(self.gen_base_path())
        return self._meta_symlink_path

    def gen_meta_file_name(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return subvol_v3_meta_file_name(uuid)

    def gen_meta_path(self, uuid=None):
        return subvol_meta_path(self.gen_base_path(),
                                self.gen_meta_file_name(uuid))

    def gen_incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = join(self.gen_base_path(),
                                         self.INCAR_BASE_DIR)
        return self._incar_base_path

    def gen_incar_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return join(self.gen_incar_base_path(), uuid)

    def gen_mnt_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return subvol_v3_mnt_path(self.gen_base_path(), uuid)

    def gen_retained_path(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return subvol_v3_retained_path(self.gen_base_path(), uuid)

    def gen_snap_base_path(self, uuid=None):
        return join(self.gen_incar_path(uuid), SNAP_BASE_DIR)

    def gen_snap_path(self, snap_name, uuid=None):
        return join(self.gen_snap_base_path(uuid), snap_name)

    def gen_snap_data_path(self, snap_name, uuid=None):
        return join(self.gen_snap_base_path(uuid), snap_name, self.MNT_DIR)

    def gen_prev_path(self, uuid):
        return subvol_uuid_path(self.gen_base_path(), uuid)

    def gen_v2_snap_path(self, snap_name):
        assert self.v2_uuid
        return join(self.gen_base_path(), SNAP_BASE_DIR, snap_name)

    def gen_v2_snap_data_path(self, snap_name):
        return subvol_v2_snap_data_path(self.gen_base_path(), self.v2_uuid,
                                        snap_name)

    def gen_v1_snap_path(self, snap_name):
        assert self.v1_uuid
        return subvol_v1_snap_path(self.gen_base_path(), self.v1_uuid,
                                   snap_name, upgraded=True)

    def gen_v1_snap_data_path(self, snap_name):
            assert False, \
                'in v1, snap_data_path is same as snap path'

    def fetch_uuid(self):
        meta_file_name = self.mount.get_shell_stdout(
                f'readlink ./{self.gen_meta_symlink_path()}').strip()
        uuid = meta_file_name.replace('.meta.', '')
        verify_uuid(uuid)
        return uuid


    # ----- incar helper -----


    def add_incar(self, uuid, snap_names=None, retained=False):
        incar = self.get_incar(uuid)
        assert not incar, \
            'incar already present in self.incars'

        self.incars.append(V3IncarHelper(self.name, uuid, snap_names, retained))

    def get_incar(self, uuid=None, snap_name=None):
        assert not snap_name or not uuid

        if snap_name:
            for incar in self.incars:
                if snap_name in incar.snap_names:
                    return incar
        else:
            verify_uuid(uuid)
            uuid = uuid if uuid else self.uuid
            for incar in self.incars:
                if incar.uuid == uuid:
                    return incar

    def rm_incar(self, uuid=None, incar=None):
        if incar:
            self.incars.remove(incar)
        elif incar := self.get_incar(uuid):
            self.incars.remove(incar)
        else:
            assert False, 'attempted to remove an incar that does not exist'


    # ----- override subvol ops methods -----


    def create(self, opts='', default_mode=True):
        super().create(opts, default_mode=default_mode)

        self.uuid = self.fetch_uuid()
        if self.uuid in self.incars:
            raise RuntimeError(f'uuid is not unique, self.uuid = {self.uuid}, '
                               f'self.incars = {self.incars}')

        if incar := self.get_incar(self.uuid):
            incar.retained = False
        else:
            self.add_incar(self.uuid)

    def rm(self, opts=''):
        incar = self.get_incar(self.uuid)
        assert incar, \
            'incar is being removed but it is absent in self.incars!'

        super().rm(opts)

        if incar and '--retain-snapshots' in opts:
            assert incar.retained == False
            incar.retained = True

    def snap_create(self, snap_name=None):
        snap_name = super().snap_create(snap_name)

        incar = self.get_incar(self.uuid)
        incar.snap_names.append(snap_name)
        return snap_name

    def snap_rm(self, snap_name):
        incar = self.get_incar(snap_name=snap_name)

        super().snap_rm(snap_name)

        incar.snap_names.remove(snap_name)
        if incar.retained and len(incar.snap_names) == 0:
            self.rm_incar(incar=incar)


    # ----- testing subvol -----


    def verify_uuid(self, subvol_mnt_path=None):
        if not subvol_mnt_path:
            subvol_mnt_path = self.getpath()

        uuid = self.get_uuid_from_path(subvol_mnt_path)
        verify_uuid(uuid)
        self.uuid = self.fetch_uuid()

    def verify_layout(self):
        self.mount.verify_path_is_dir(self.gen_base_path())
        self.mount.verify_path_is_symlink(self.gen_meta_symlink_path())
        self.mount.verify_path_is_reg(self.gen_meta_path())
        self.mount.verify_path_is_dir(self.gen_incar_base_path())

        for incar in self.incars:
            self.mount.verify_path_is_reg(self.gen_meta_path(incar.uuid))
            self.mount.verify_path_is_dir(self.gen_incar_path(incar.uuid))
            if incar.retained:
                self.mount.verify_path_is_dir(self.gen_retained_path(incar.uuid))
            else:
                self.mount.verify_path_is_dir(self.gen_mnt_path(incar.uuid))

            if not incar.snap_names:
                continue
            for snap_name in incar.snap_names:
                self.verify_snap_layout(incar, snap_name)

    def verify_snap_layout(self, incar, snap_name):
        snap_data_paths = []
        snap_data_paths.append(self.gen_snap_data_path(snap_name, incar.uuid))
        if self.v2_uuid:
            snap_data_paths.append(self.gen_v2_snap_data_path(snap_name))
        if self.v1_uuid:
            snap_data_paths.append(self.gen_v1_snap_path(snap_name))

        for path in snap_data_paths:
            try:
                self.mount.verify_path_is_dir(path)
            except:
                continue
            else:
                return

        raise RuntimeError('subvol snap layout verification failed for snap '
                           f'"{snap_name}"')

    def verify_meta_file_for_v1(self, meta_content):
        v1_lines = ['[PRE_V3_INCAR]', 'pre_v3_version = v1',
                    f'pre_v3_uuid = {self.v1_uuid}',
                    f'pre_v3_path = {self.gen_prev_path(self.v1_uuid)}']

        for line in v1_lines:
            assert line in meta_content, \
                f'line = {line} meta_content = {meta_content}'

    def verify_meta_file_for_v2(self, meta_content):
        v2_lines = ['[PRE_V3_INCAR]', 'pre_v3_version = v2',
                    f'pre_v3_uuid = {self.v2_uuid}',
                    f'pre_v3_path = {self.gen_prev_path(self.v2_uuid)}']

        for line in v2_lines:
            assert line in meta_content, \
                f'line = {line} meta_content = {meta_content}'

    def verify_meta_file(self):
        meta_content = self.mount.get_shell_stdout('sudo cat '
                                                   f'./{self.gen_meta_path()}')
        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        assert 'version = 3' in meta_content
        assert 'type = subvolume' in meta_content

        if self.v2_uuid:
            self.verify_meta_file_for_v2(meta_content)
        if self.v1_uuid:
            self.verify_meta_file_for_v1(meta_content)

        incar = self.get_incar(self.uuid)
        if incar:
            if incar.retained:
                assert f'path = {self.gen_retained_path()}' in meta_content, \
                    f'self.gen_retained_path() = {self.gen_retained_path()}'
                assert 'state = snapshot-retained' in meta_content
            else:
                assert f'path = {self.gen_mnt_path()}' in meta_content, \
                    f'self.gen_mnt_path() = {self.gen_mnt_path()}'
                assert 'state = complete' in meta_content

    def verify_subvol_getpath_cmd(self):
        assert self.getpath() == self.gen_mnt_path()

    def verify_snap_getpath_cmd(self, snap_name, uuid=None, version=3):
        if version == 3:
            exp_snap_path = self.gen_snap_data_path(snap_name, uuid)
        elif version == 2 and self.has_v2_snaps:
            exp_snap_path = self.gen_v2_snap_data_path(snap_name)
        elif version == 1 and self.has_v1_snaps:
            exp_snap_path = self.gen_v1_snap_path(snap_name)
        else:
            assert False, \
                f'version = {version}'

        reported_snap_path = self.snap_getpath(snap_name)

        assert reported_snap_path == exp_snap_path, \
            (f'reported_snap_path = {reported_snap_path} '
             f'exp_snap_path = {exp_snap_path}')

    def verify_snap_ls_for_all_incars(self):
        all_snap_names = str(self.snap_ls())

        for incar in self.incars:
            for s in incar.snap_names:
                assert s in all_snap_names, \
                    f's = {s} curr_snap_names = {all_snap_names}'

    def verify_snap_getpath_for_all_incars(self):
        for incar in self.incars:
            for s in incar.snap_names:
                self.verify_snap_getpath_cmd(s, incar.uuid, 3)
                return
