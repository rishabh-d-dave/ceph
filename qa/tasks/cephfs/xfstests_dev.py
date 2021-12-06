from io import StringIO
from logging import getLogger

from os import getcwd as os_getcwd
from os.path import join
from textwrap import dedent

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.vstart_runner import LocalFuseMount
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.kernel_mount import KernelMount


logger = getLogger(__name__)


# TODO: add code to run non-ACL tests too.
# TODO: get tests running with SCRATCH_DEV and SCRATCH_DIR.
# TODO: make xfstests-dev tests running without running `make install`.
class XFSTestsDev(CephFSTestCase):

    def setUp(self):
        super(XFSTestsDev, self).setUp()
        self.prepare_xfstests_dev()

    def prepare_xfstests_dev(self):
        self.get_repo()
        self.get_test_and_scratch_dirs_ready()
        self.install_deps()
        self.create_reqd_users()
        self.write_local_config()

        # NOTE: On teuthology machines it's necessary to run "make" as
        # superuser since the repo is cloned somewhere in /tmp.
        self.mount_a.client_remote.run(args=['sudo', 'make'],
                                       cwd=self.repo_path, stdout=StringIO(),
                                       stderr=StringIO())
        self.mount_a.client_remote.run(args=['sudo', 'make', 'install'],
                                       cwd=self.repo_path, omit_sudo=False,
                                       stdout=StringIO(), stderr=StringIO())

    def get_repo(self):
        """
        Clone xfstests_dev repository. If already present, update it.
        """
        from teuthology.orchestra import run

        # TODO: make sure that repo is not cloned for every test. it should
        # happen only once.
        remoteurl = 'git://git.ceph.com/xfstests-dev.git'
        self.repo_path = self.mount_a.client_remote.mkdtemp(suffix=
                                                            'xfstests-dev')
        self.mount_a.run_shell(['git', 'archive', '--remote=' + remoteurl,
                                'HEAD', run.Raw('|'),
                                'tar', '-C', self.repo_path, '-x', '-f', '-'])

    def get_admin_key(self):
        import configparser

        cp = configparser.ConfigParser()
        cp.read_string(self.fs.mon_manager.raw_cluster_cmd(
            'auth', 'get-or-create', 'client.admin'))

        return cp['client.admin']['key']

    def get_test_and_scratch_dirs_ready(self):
        """ "test" and "scratch" directories are directories inside Ceph FS.
            And, test and scratch mounts are path on the local FS where "test"
            and "scratch" directories would be mounted. Look at xfstests-dev
            local.config's template inside this file to get some context.
        """
        self.test_dirname = 'test'
        self.mount_a.run_shell(['mkdir', self.test_dirname])
        # read var name as "test dir's mount path"
        self.test_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.test_dirname)
        self.mount_a.run_shell(['sudo','ln','-s',join(self.mount_a.mountpoint,
                                                      self.test_dirname),
                                self.test_dirs_mount_path])

        self.scratch_dirname = 'scratch'
        self.mount_a.run_shell(['mkdir', self.scratch_dirname])
        # read var name as "scratch dir's mount path"
        self.scratch_dirs_mount_path = self.mount_a.client_remote.mkdtemp(
            suffix=self.scratch_dirname)
        self.mount_a.run_shell(['sudo','ln','-s',join(self.mount_a.mountpoint,
                                                      self.scratch_dirname),
                                self.scratch_dirs_mount_path])

    def install_deps(self):
        from teuthology.misc import get_system_type

        distro, version = get_system_type(self.mount_a.client_remote,
                                          distro=True, version=True)
        distro = distro.lower()
        major_ver_num = int(version.split('.')[0]) # only keep major release
                                                   # number

        # we keep fedora here so that right deps are installed when this test
        # is run locally by a dev.
        if distro in ('redhatenterpriseserver', 'redhatenterprise', 'fedora',
                      'centos', 'centosstream'):
            deps = """acl attr automake bc dbench dump e2fsprogs fio \
            gawk gcc indent libtool lvm2 make psmisc quota sed \
            xfsdump xfsprogs \
            libacl-devel libattr-devel libaio-devel libuuid-devel \
            xfsprogs-devel btrfs-progs-devel python2 sqlite""".split()
            deps_old_distros = ['xfsprogs-qa-devel']

            if distro != 'fedora' and major_ver_num > 7:
                    deps.remove('btrfs-progs-devel')

            args = ['sudo', 'yum', 'install', '-y'] + deps + deps_old_distros
        elif distro == 'ubuntu':
            deps = """xfslibs-dev uuid-dev libtool-bin \
            e2fsprogs automake gcc libuuid1 quota attr libattr1-dev make \
            libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
            uuid-runtime python sqlite3""".split()

            if major_ver_num >= 19:
                deps[deps.index('python')] ='python2'
            args = ['sudo', 'apt-get', 'install', '-y'] + deps
        else:
            raise RuntimeError('expected a yum based or a apt based system')

        self.mount_a.client_remote.run(args=args, omit_sudo=False)

    def create_reqd_users(self):
        self.mount_a.client_remote.run(args=['sudo', 'useradd', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupadd', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'useradd',
                                             '123456-fsgqa'], omit_sudo=False,
                                       check_status=False)

    def write_local_config(self):
        if isinstance(self.mount_a, KernelMount):
            conf_contents = self._gen_conf_for_kernel_mnt()
        elif isinstance(self.mount_a, (FuseMount, LocalFuseMount)):
            conf_contents = self._gen_conf_for_fuse_mnt()

        self.mount_a.client_remote.write_file(join(self.repo_path,
                                                   'local.config'),
                                              conf_contents, sudo=True)

    # generate local.config for kernel cephfs mounts
    def _gen_conf_for_kernel_mnt(self):
        mon_sock = self.fs.mon_manager.get_msgrv1_mon_socks()[0]
        test_dev = mon_sock + ':/' + self.test_dirname
        scratch_dev = mon_sock + ':/' + self.scratch_dirname

        return dedent(f'''\
            export FSTYP=ceph
            export TEST_DEV={test_dev}
            export TEST_DIR={self.test_dirs_mount_path}
            export TEST_FS_MOUNT_OPTS="-o name=admin,secret={self.get_admin_key()}"
            #export SCRATCH_DEV={scratch_dev}
            #export SCRATCH_MNT={self.scratch_dirs_mount_path}''')

    # generate local.config for FUSE cephfs mounts
    def _gen_conf_for_fuse_mnt(self):
        mon_sock = self.fs.mon_manager.get_msgrv1_mon_socks()[0]
        test_dev = 'ceph-fuse'
        scratch_dev = ''
        # XXX: Please note that ceph_fuse_bin_path is not ideally required
        # because ceph-fuse binary ought to be present in one of the standard
        # locations during teuthology tests but then testing without
        # vstart_runner.py will be messy since ceph-fuse binary won't be
        # present in a standard locations during these sessions. Thus, this
        # workaround.
        ceph_fuse_bin_path = 'ceph-fuse' # bin expected to be in env
        if type(self.mount_a) == 'LocalFuseMount': # for vstart_runner.py runs
            ceph_fuse_bin_path = join(os_getcwd(), 'bin')
        keyring_path = self.mount_a.client_remote.mktemp(
            data=self.fs.mon_manager.get_keyring('client.admin'))

        return dedent(f'''\
            export FSTYP=ceph-fuse
            export CEPH_FUSE_BIN_PATH={ceph_fuse_bin_path}
            export TEST_DEV={test_dev}
            export TEST_DIR={self.test_dirs_mount_path}
            export TEST_FS_MOUNT_OPTS="-m {mon_sock} -k {keyring_path} --client_mountpoint {self.test_dirname}"
            #export SCRATCH_DEV={scratch_dev}
            #export SCRATCH_MNT={self.scratch_dirs_mount_path}''')

    def tearDown(self):
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', 'fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'userdel', '--force',
                                             '--remove', '123456-fsgqa'],
                                       omit_sudo=False, check_status=False)
        self.mount_a.client_remote.run(args=['sudo', 'groupdel', 'fsgqa'],
                                       omit_sudo=False, check_status=False)

        self.mount_a.client_remote.run(args=['sudo', 'rm', '-rf',
                                             self.repo_path],
                                       omit_sudo=False, check_status=False)

        super(XFSTestsDev, self).tearDown()
