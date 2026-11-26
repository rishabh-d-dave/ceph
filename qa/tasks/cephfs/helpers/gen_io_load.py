from logging import getLogger
from os.path import dirname
from textwrap import dedent
from time import sleep
import threading

from tasks.ceph_manager import RunCephCmd

from teuthology.contextutil import safe_while
from teuthology.misc import get_first_mon
from teuthology.orchestra import run


log = getLogger(__name__)


class GenIoLoad(RunCephCmd):
    '''
    Generate IO load by running a writer thread in background through a given
    mount, on a given path by writing zeros in 1 KB files until caller signals
    to stop.
    '''

    def __init__(self, mount, path, initial_wait=0, writer_sleep=0,
                 writer_timeout=60*60*15, raise_on_writer_crash=True,
                 raise_method=None):
        '''
        :param path: path to dir where IO load would be generated.
        :param mount: mount object to be used for generating IO load.
        :param client_id: client_id to be used for generating IO load.
        :param data_pool_name: CephFS data pool name where IO load will be
                               generated; used for creating caps
        :param initial_wait: period to wait for right after IO has begun,
                             useful for running IO without interruptions.
        :param writer_sleep: sleep duration for writer_thread between writing
                               files.
        :param writer_timeout: duration after which writer thread should be
                                 killed.
        '''
        self.mount = mount
        self.path = path
        self._init_mon_manager()
        RunCephCmd.__init__(self, self.mon_manager)

        self.initial_wait = initial_wait
        self.writer_sleep = writer_sleep
        self.writer_timeout = writer_timeout

        self.raise_on_writer_crash = raise_on_writer_crash
        self.raise_method = raise_method

        self.stop_writer = threading.Event()
        self.should_stop = lambda: self.stop_writer.is_set()

        self.file_count = 0
        self.writer = None
        self.writer_crashed = False

    def _init_mon_manager(self):
        ctx = self.mount.ctx

        # if vstart_runner.py has invoked this code
        if 'Local' in str(type(ctx)):
            self.mon_manager = ctx.managers['ceph']
        # else teuthology has invoked this code
        else:
            from tasks.ceph_manager import CephManager
            first_mon = get_first_mon(ctx, None)
            admin_remote = tuple(ctx.cluster.only(first_mon).remotes.keys())[0]
            self.mon_manager = CephManager(admin_remote, ctx=ctx,
                                           logger=log.getChild('ceph_manager'))

    def _handle_thread_crash(self, args):
        self.writer_crashed = True
        if self.raise_on_writer_crash:
           msg = (f'writer thread running crashed. mount = {self.mount} '
                  f'path = {self.path} args.exc_type = {args.exc_type} '
                  f'args.exc_value = {args.exc_value} '
                  f'args.thread = {args.thread}')

           if self.raise_method:
                self.raise_method(msg)
           log.error(msg)
           raise RuntimeError(msg)

    def create_client_and_remount(self, client_id, data_pool_name):
        '''
        Create CephX client for client_id with MDS caps for specific path
        on which IO is to be performed. And then remount with this client.
        '''
        assert 'client.' not in client_id

        path = self.path if self.path[0] == '/' else f'/{self.path}'

        client_name = f'client.{client_id}'
        self.run_ceph_cmd(f'auth add {client_name} '
                          'mon "allow r" '
                          f'osd "allow rw pool={data_pool_name}" '
                          f'mds "allow rw path={path}"')
        keyring = self.get_ceph_cmd_stdout(f'auth get {client_name}')

        self.mount.remount(client_id=self.client_id, client_keyring=keyring,
                           cephfs_mntpt=self.path)

    # TODO implement self.writer_timeout
    def write_files(self):
        self.file_count = 1

        while True:
            if self.should_stop():
                break

            file_path = f'./file-{self.file_count}'
            self.mount.run_shell(f'dd if=/dev/zero of={file_path} bs=1K count=1')
            sleep(self.writer_sleep)
            self.file_count += 1

    def get_initial_num_of_files(self):
        file_count = self.mount.get_shell_stdout('find ./ -type f | wc -l')
        self.initial_file_count = int(file_count.strip())

    def start(self):
        threading.excepthook = self._handle_thread_crash
        self.get_initial_num_of_files()
        self.writer = threading.Thread(target=self.write_files)
        self.writer.start()

        sleep(2)
        msg = 'writer thread died as soon as they had begun'
        log.info(msg)
        assert self.writer.is_alive(), msg

        log.info(f'waiting for {self.initial_wait} seconds for the writer '
                 'thread to run in background without interruptions')
        sleep(self.initial_wait)
        assert self.writer.is_alive(), \
                'writer thread crashed unexpectedly'

    def stop(self):
        if self.stop_writer.is_set():
            raise RuntimeError('stop_writer flag was already set')
        if not self.writer.is_alive():
            raise RuntimeError('writer thread is dead')

        self.stop_writer.set()
        # avoids unnecessary failure in case the writer threads takes some time
        # to stop.
        msg = 'the writer threads were singalled to stop but they haven\'t yet'
        with safe_while(tries=5, sleep=2, action=msg) as proceed:
            while proceed():
                if not self.is_alive():
                    break

        self.file_count -= 1

    def is_alive(self):
        return self.writer.is_alive()

    def verify_num_of_files_written(self):
        file_count = self.mount.get_shell_stdout('find ./ -type f | wc -l')
        file_count = int(file_count.strip())

        # diff of zero or one is okay
        diff = file_count - self.file_count - self.initial_file_count
        if diff not in (0, 1):
            raise AssertionError(f'file_count = {file_count} self.file_count = '
                                 f'{self.file_count}')


class RemoteIoLoad:
    '''
    Continuous IO via a long-lived bash writer on the client remote.

    Unlike GenIoLoad (Python thread issuing mount.run_shell per file), the
    script stays in the mount so ESTALE/EACCES after a path rename (e.g.
    subvolume auto-upgrade) fails the writer, and an md5 manifest is kept
    outside CephFS for remount verification.
    '''

    SCRIPT_BASENAME = 'gen_io_load.writer.sh'

    # Runs as root from within the mount. Writes until statedir/stop appears.
    # Manifest and progress live in statedir (not on CephFS).
    WRITER_SCRIPT = dedent('''\
        set -u
        mnt=$1
        state=$2
        manifest=$state/manifest
        fail() { echo "$(date -u +%FT%T) writer: $*" >&2; exit 1; }
        : > "$manifest" || fail "creating manifest"
        cd "$mnt" || fail "cd $mnt"
        mkdir -p io || fail "mkdir io"
        i=0
        while [ ! -e "$state/stop" ]; do
            i=$((i + 1))
            f=io/f.$i
            dd if=/dev/urandom of=$f.tmp bs=4k count=4 conv=fsync status=none || fail "write $f.tmp"
            mv $f.tmp $f || fail "rename $f.tmp"
            md5sum $f >> "$manifest" || fail "checksum $f"
            if [ $((i % 20)) -eq 0 ]; then
                tail -n 20 "$manifest" | md5sum -c --quiet || fail "verify at $i"
                mkdir io/d.$i && rmdir io/d.$i || fail "mkdir/rmdir io/d.$i"
                ls io > /dev/null || fail "readdir"
                stat . > /dev/null || fail "stat"
            fi
            echo $i > "$state/progress"
            sleep 0.5
        done
        md5sum -c --quiet "$manifest" || fail "final verify"
        n=$(find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l)
        [ "$n" -eq "$i" ] || fail "found $n files, wrote $i"
        echo "writer: wrote and verified $i files"
    ''')

    def __init__(self, mount, statedir, *, label=None):
        self.mount = mount
        self.statedir = statedir
        self.label = label if label is not None else statedir
        self.proc = None
        self._script_path = None

    @property
    def remote(self):
        return self.mount.client_remote

    @property
    def manifest_path(self):
        return f'{self.statedir}/manifest'

    @property
    def finished(self):
        return self.proc is None or self.proc.finished

    def start(self):
        remote = self.remote
        remote.run(args=['sudo', 'rm', '-rf', self.statedir])
        remote.run(args=['mkdir', '-p', self.statedir])
        # One script copy under the statedir parent (typically the testdir);
        # pkill -f matches SCRIPT_BASENAME.
        self._script_path = f'{dirname(self.statedir)}/{self.SCRIPT_BASENAME}'
        remote.write_file(self._script_path, self.WRITER_SCRIPT)
        self.proc = remote.run(
            args=['sudo', 'stdin-killer', '--timeout=60', '--',
                  'bash', self._script_path, self.mount.mountpoint,
                  self.statedir],
            wait=False, stdin=run.PIPE, label=f'writer {self.label}')
        return self

    def ensure_alive(self):
        if self.proc is None:
            raise RuntimeError(f'writer for {self.label} was never started')
        if self.proc.finished:
            self.proc.wait()
            raise RuntimeError(f'writer for {self.label} exited prematurely')

    @property
    def progress(self):
        self.ensure_alive()
        out = self.remote.sh(
            f'cat {self.statedir}/progress 2>/dev/null || echo 0')
        return int(out.strip())

    def wait_for_progress(self, nfiles, timeout=300, sleep=5):
        start = self.progress
        target = start + nfiles
        action = (f'writer for {self.label} to write {nfiles} files '
                  f'past {start}')
        with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                        action=action) as proceed:
            while proceed():
                if self.progress >= target:
                    return self.progress

    def stop(self, timeout=900, sleep=5):
        self.remote.run(args=['sudo', 'touch', f'{self.statedir}/stop'])
        action = f'writer for {self.label} to stop'
        with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                        action=action) as proceed:
            while proceed():
                if self.proc.finished:
                    break
        self.proc.stdin.close()
        self.proc.wait()
        return int(self.remote.sh(f'cat {self.statedir}/progress').strip())

    def kill(self):
        if self.proc is not None and not self.proc.finished:
            log.info(f'killing writer for {self.label}')
            self.remote.run(
                args=['sudo', 'pkill', '-KILL', '-f', self.SCRIPT_BASENAME],
                check_status=False)

    def verify_data(self):
        """
        After remount (or when the writer has stopped): checksums match the
        outside-CephFS manifest and the file count agrees.
        """
        manifest = self.manifest_path
        self.mount.run_shell(
            ['sudo', 'md5sum', '-c', '--quiet', manifest], timeout=1800)
        nlines = int(self.remote.sh(f'wc -l < {manifest}').strip())
        found = int(self.mount.get_shell_stdout(
            'sudo find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l'
            ).strip())
        assert found == nlines, \
            f'{self.label}: {found} files, expected {nlines} per manifest'
        return found
