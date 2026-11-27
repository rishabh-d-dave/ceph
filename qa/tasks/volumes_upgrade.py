"""
mgr/volumes upgrade sub-tasks: verify that auto-upgrade of v1 and v2
subvolumes to the v3 layout does not disrupt subvolume clients doing IO, that
async clones and purges started before an upgrade finish successfully after
it, that v1/v2 subvolumes keep working as such while only ceph-mgr is
upgraded, that subvolume (and snapshot) attributes set on each helper,
subvolume group attributes, auth IDs and the subvolume boundary survive
auto-upgrade, that clones get the attributes of their source, that sessions
mounted at the pre-v3 data dir can be evicted,
that snapshots taken before and after auto-upgrade are all listed, and that
subvolumes removed with retained snapshots (one of them with a pending
clone) can be upgraded.

Suite wiring (see qa/suites/fs/upgrade/volumes):

  - volumes_upgrade:        contextmanager — setup on the old release, then
                            after the cluster is upgraded (after yield) run
                            post-upgrade verification; teardown on exit
  - volumes_upgrade.verify_mgr_first:
                            after ceph-mgr is upgraded first (MDS still old),
                            before the rest of the cluster is upgraded

Subvolume create options and hand-set data-dir attrs are set explicitly on
each SubvolV*Helper (source of truth). Names are documentation only.
"""

import contextlib
import functools
import json
import logging
import time
from configparser import ConfigParser
from io import StringIO
from textwrap import dedent
from types import SimpleNamespace

from teuthology import misc
from teuthology.contextutil import safe_while

from tasks.cephfs.helpers.gen_io_load import RemoteIoLoad
from tasks.cephfs.helpers.subvolumes import (
    SubvolOpsHelper,
    SubvolV1Helper,
    SubvolV2Helper,
    SubvolV3Helper,
)

log = logging.getLogger(__name__)

VOLNAME = 'cephfs'
SNAPNAME = 'snap0'
SNAPNAME_2 = 'snap1'
# taken (on the clone source) by the new mgr while the MDS is still old
SNAP_MGR_ONLY = 'snap_mgr_only'
# taken on every subvolume after auto-upgrade
SNAP_V3 = 'snap_v3'
TRASH_DIR = 'volumes/_deleting'
# mgr/volumes/snapshot_clone_delay the old ceph-mgr picks up the clones with,
# so that they're still pending when it gets upgraded (pause_cloning cancels
# clones, see https://tracker.ceph.com/issues/81383).
CLONE_DELAY = 86400
# mgr/volumes/max_concurrent_clones for the old ceph-mgr: each delayed clone
# holds a cloner thread, and with mgr/volumes/snapshot_clone_no_wait (the
# default) ceph-mgr rejects a clone (EAGAIN) once as many clones as there are
# cloner threads are pending.
MAX_CONCURRENT_CLONES = 8
STATE_KEY = 'volumes-upgrade-state'

# Create-option / hand-set attr values. Set on each helper explicitly
# (source of truth); must survive auto-upgrade. User xattr and earmark are
# copied by auto-upgrade; ceph.* vxattrs (quota/layout) are carried over.
SV_SIZE = 10 * 1024 ** 3
SV_UID = SV_GID = 1000
SV_MODE = 0o750
SV_EARMARK = 'nfs'
SV_METADATA = {'vu_key_0': 'vu_val_0', 'vu_key_1': 'vu_val_1'}
SV_XATTR = ('user.volumes_upgrade', 'vu_xattr_val')
SV_MAX_FILES = 1000000
SV_ACL = 'u:1001:r-x,d:u:1001:r-x'
DATA_POOL_2 = f'cephfs.{VOLNAME}.data2'
DEFAULT_MODE = 0o755
GRP_SIZE = 20 * 1024 ** 3
GRP_UID = GRP_GID = 2000
GRP_MODE = 0o750
GRP_INFO_KEPT = ('uid', 'gid', 'mode', 'data_pool', 'bytes_quota', 'created_at')


def _configure_no_attrs(sv):
    sv.mode = DEFAULT_MODE


def _configure_all_attrs(sv):
    sv.size = SV_SIZE
    sv.uid, sv.gid = SV_UID, SV_GID
    sv.pass_uid_gid = True
    sv.mode = SV_MODE
    sv.namespace_isolated = True
    sv.namespace = sv.gen_pool_namespace()
    sv.earmark = SV_EARMARK
    sv.metadata = dict(SV_METADATA)
    sv.user_xattr = SV_XATTR
    sv.max_files = SV_MAX_FILES
    sv.acl = SV_ACL


def _configure_size(sv):
    sv.size = SV_SIZE
    sv.mode = DEFAULT_MODE


def _configure_pool_ns(sv):
    sv.mode = DEFAULT_MODE
    sv.namespace_isolated = True
    sv.namespace = sv.gen_pool_namespace()
    sv.pool = DATA_POOL_2
    sv.pool_layout = DATA_POOL_2


def _configure_group_inherit(sv):
    """Empty create options; group gets GRP_*; subvol inherits pool/uid/gid."""
    sv.mode = DEFAULT_MODE
    sv.uid, sv.gid = GRP_UID, GRP_GID
    sv.pool = DATA_POOL_2
    sv.group_attrs = True


# Specs: name is documentation; configure callable sets helper fields
# (source of truth). IO: (name, group, pre_version, nsnaps, configure)
IO_SUBVOLS = [
    ('subvol_v1_io_attr_size', None, 1, 2, _configure_size),
    ('subvol_v2_io_all_attrs', None, 2, 1, _configure_all_attrs),
    ('subvol_v2_io_attr_group', 'group_io', 2, 2, _configure_group_inherit),
]
# no-IO: (name, group, pre_version, configure)
NO_IO_SUBVOLS = [
    ('subvol_v1_no_io_no_attrs', None, 1, _configure_no_attrs),
    ('subvol_v1_no_io_attr_size', None, 1, _configure_size),
    ('subvol_v1_no_io_all_attrs', 'group_attr', 1, _configure_all_attrs),
    ('subvol_v2_no_io_no_attrs', None, 2, _configure_no_attrs),
    ('subvol_v2_no_io_attr_size', None, 2, _configure_size),
    ('subvol_v2_no_io_all_attrs', 'group_attr', 2, _configure_all_attrs),
]
CLONE_SRC = ('subvol_clone_src_attr_pool_ns', 'group_src', 2, _configure_pool_ns)
# Pending clones through upgrade (always v2 — old mgr creates clones as v2)
CLONES = [
    ('subvol_clone_pending_0', None),
    ('subvol_clone_pending_1', None),
    ('subvol_clone_pending_2', 'group_clone'),
]
PURGES = [
    ('subvol_purge_0', None),
    ('subvol_purge_1', None),
    ('subvol_purge_2', 'group_purge'),
]
# Clone of first snap of IO_SUBVOLS[i] (needs nsnaps >= 1)
IO_CLONE = (('subvol_clone_of_io', 'group_clone'), 1)
RETAINED_CLONE_SRC = ('subvol_retained_src', 'group_retained')
RETAINED_CLONE = ('subvol_clone_retained', 'group_clone')
RETAINED_PURGE = ('subvol_retained_purge', 'group_retained')
STATE_RETAINED = 'snapshot-retained'
# vxattrs of a data dir that must survive auto-upgrade (see _fs_attrs())
FS_VXATTRS = ('ceph.dir.layout.pool', 'ceph.dir.layout.pool_namespace',
              'ceph.quota.max_bytes', 'ceph.quota.max_files')
# a file in the data dir for probing the subvolume boundary
BOUNDARY_FILE = 'vu_boundary'
# user metadata of each pre-v3 snapshot (the value being the snapshot name)
SNAP_METADATA_KEY = 'vu_snap_key'
# `fs subvolume snapshot info` fields that must be as the old ceph-mgr
# reported them
SNAP_INFO_KEPT = ('created_at', 'data_pool')
# `fs subvolume info` fields that must be as the old ceph-mgr reported them
INFO_KEPT = ('type', 'uid', 'gid', 'mode', 'data_pool', 'pool_namespace',
             'bytes_quota', 'created_at', 'earmark')
# `fs subvolume info` fields a clone gets from (the snapshot of) its source
CLONE_INFO_KEPT = ('uid', 'gid', 'mode', 'data_pool', 'pool_namespace',
                   'bytes_quota', 'earmark')


def _copy_suite_attrs(src, dst):
    # create-option attrs are copied by SubvolV3Helper.copy_create_attrs
    for attr in ('data_path', 'recorded_info', 'fs_attrs', 'pre_version',
                 'retained_snaps', 'io_nsnaps'):
        if hasattr(src, attr):
            setattr(dst, attr, getattr(src, attr))


def _repoint_sv(st, old, new):
    """After promote: replace old helper with new across the state bag."""
    for name in ('io_svs', 'no_io_svs', 'clones', 'purges', 'retained'):
        lst = getattr(st, name)
        for i, x in enumerate(lst):
            if x is old:
                lst[i] = new
    if st.src is old:
        st.src = new
    st.clone_of = {
        k: (new if s is old else s, snap)
        for k, (s, snap) in st.clone_of.items()
    }
    st.late_clones = [
        (new if c is old else c, new if s is old else s, snap)
        for c, s, snap in st.late_clones
    ]
    for w in st.writers:
        if getattr(w, 'sv', None) is old:
            w.sv = new


def _new_sv(st, name, group, version):
    """
    SubvolV1/V2/V3Helper with suite-only fields. pre_version is kept across
    promote (helper.version -> 3). Create-option fields are set by the
    caller (or _configure_* helpers) — names are documentation only.
    """
    cls = {1: SubvolV1Helper, 2: SubvolV2Helper, 3: SubvolV3Helper}[version]
    sv = cls(st.manager, st.admin, VOLNAME, name=name, grp_name=group)
    sv.pre_version = version
    sv.data_path = None
    sv.recorded_info = None
    sv.fs_attrs = None
    sv.retained_snaps = False
    sv.io_nsnaps = 0
    return sv


def _promote_v3(st, sv, snap_names=None):
    """Like test_v2_to_v3: wrap pre-v3 helper as V3 after getpath auto-upgrade."""
    assert isinstance(sv, (SubvolV1Helper, SubvolV2Helper)), type(sv)
    if snap_names is not None:
        sv.snap_names = list(snap_names)
    elif not getattr(sv, 'snap_names', None):
        sv.snap_names = []
    if sv.retained_snaps:
        sv.retained = True
    new = SubvolV3Helper(subvol=sv)
    _copy_suite_attrs(sv, new)
    _repoint_sv(st, sv, new)
    return new


def _mark_retained(sv):
    sv.retained_snaps = True
    if not isinstance(sv, SubvolV3Helper):
        sv.retained = True


def _make_state(ctx, config):
    """
    Cross-phase bag kept on ctx[STATE_KEY] for task / verify_mgr_first / teardown.
    Both v1 and v2 profiled subvolumes live in every job.
    """
    st = SimpleNamespace()
    st.manager = ctx.managers['ceph']
    st.admin = _get_mount(ctx, config.get('admin_client', 'client.3'))
    st.io_mounts = [_get_mount(ctx, c) for c in
                    config.get('io_clients',
                               ['client.0', 'client.1', 'client.2'])]
    assert len(st.io_mounts) == len(IO_SUBVOLS)
    st.testdir = misc.get_testdir(ctx)

    st.io_svs = []
    for name, group, ver, nsnaps, configure in IO_SUBVOLS:
        sv = _new_sv(st, name, group, ver)
        sv.io_nsnaps = nsnaps
        configure(sv)
        st.io_svs.append(sv)

    name, group, ver, configure = CLONE_SRC
    st.src = _new_sv(st, name, group, ver)
    configure(st.src)

    st.no_io_svs = []
    for name, group, ver, configure in NO_IO_SUBVOLS:
        sv = _new_sv(st, name, group, ver)
        configure(sv)
        st.no_io_svs.append(sv)

    # group name -> `fs subvolumegroup info` by the old ceph-mgr, of each
    # group created with GRP_* attributes
    st.groups = {}
    st.clones = [_new_sv(st, name, group, 2) for name, group in CLONES]
    st.clone_of = {c.name: (st.src, SNAPNAME) for c in st.clones}
    st.purges = [_new_sv(st, name, group, 2) for name, group in PURGES]
    st.retained = [_new_sv(st, *RETAINED_CLONE_SRC, 2),
                   _new_sv(st, *RETAINED_PURGE, 2)]

    st.clone_src_files = config.get('clone_src_files', 5000)
    st.purge_files = config.get('purge_files', 5000)
    st.snaps = {}
    st.snap_manifests = {}
    st.late_clones = []
    st.writers = []
    st.snap_info = {}
    st.evict = {}
    return st


def _pre_v3_svs(st):
    """Profiled pre-v3 subvolumes (IO, clone source, no-IO attr cases)."""
    return st.io_svs + [st.src] + st.no_io_svs


def _get_mount(ctx, role):
    _, _, id_ = misc.split_role(role)
    return ctx.mounts[id_]


def _state(ctx):
    return ctx[STATE_KEY]


def _admin_sh(st, payload, timeout=900):
    """
    Run payload as root via bash, from the root of the admin mount. The
    payload must not contain single quotes.
    """
    p = st.admin.run_shell_payload(payload, sudo=True, timeout=timeout)
    return p.stdout.getvalue()


def _wait_for(desc, pred, timeout, sleep=5):
    with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                    action=desc) as proceed:
        while proceed():
            if pred():
                return


def _set_mgr_config(st, name, value):
    st.manager.run_ceph_cmd(f'config set mgr mgr/volumes/{name} {value}')
    # config_notify() is asynchronous
    time.sleep(5)


def _count_files(st, path):
    out = _admin_sh(st, f'test -d {path} && find {path} -type f | wc -l || echo 0')
    return int(out.strip())


def _read_meta(st, sv, meta='.meta'):
    out = _admin_sh(st, f'cat {(sv.gen_base_path()).lstrip('/')}/{meta}')
    cp = ConfigParser()
    cp.read_string(out)
    return cp


def _ops(st):
    """SubvolOpsHelper bound to the cluster manager (group/clone plumbing)."""
    return SubvolOpsHelper(st.manager)


def _diff(expected, actual):
    """
    Return {key: (expected, actual)} for each key whose values differ.
    """
    return {k: (expected.get(k), actual.get(k))
            for k in sorted(expected.keys() | actual.keys())
            if expected.get(k) != actual.get(k)}


def _relpath(st, sv):
    """Current getpath relative to fs root."""
    return sv.getpath().lstrip('/')


def _manifest(st, path):
    return _admin_sh(st, f'cd {path} && find . -type f -print0 | sort -z | '
                         'xargs -0 -r md5sum')


def _populate(st, path, nfiles, files_per_dir=100):
    ndirs = max(1, nfiles // files_per_dir)
    _admin_sh(st, dedent(f'''\
        set -e
        cd {path}
        for d in $(seq 1 {ndirs}); do
            mkdir -p d.$d
            for f in $(seq 1 {files_per_dir}); do
                head -c 4096 /dev/urandom > d.$d/f.$f
            done
        done
    '''), timeout=3600)


# ----- subvolume layout checks -----


def _assert_pre_v3(st, sv, strict=False):
    """
    The subvolume is in its pre-v3 (v1/v2) layout and untouched by any
    (failed) auto-upgrade attempt.

    ceph-mgr has long been upgrading (the metadata of) v1 subvolumes without
    snapshots to v2 on access, the layout being the same. Unless strict, such
    a v1 subvolume may be either.
    """
    ftype = _admin_sh(st, f'stat -c %F {(sv.gen_base_path()).lstrip('/')}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    meta = _read_meta(st, sv)
    version = meta.get('GLOBAL', 'version')
    expected = {str(sv.pre_version)}
    if sv.pre_version == 1 and not strict and not st.snaps.get(sv.name):
        expected.add('2')
    assert version in expected, f'{sv}: version = {version}, expected {expected}'
    path = meta.get('GLOBAL', 'path')
    assert path == f'/{sv.data_path}', f'{sv}: meta path = {path}'
    out = _admin_sh(st, f'test -d {sv.data_path} && echo yes; '
                        f'test -e {(sv.gen_base_path()).lstrip('/')}/roots && echo yes || echo no')
    assert out.split() == ['yes', 'no'], \
        f'{sv}: half-upgraded subvolume (v2 dir, roots dir present?: {out.split()})'


def _assert_v3(st, sv):
    """
    Auto-upgrade (if needed) and verify v3 layout — same workflow as
    test_v2_to_v3: getpath triggers upgrade, then SubvolV3Helper(subvol=...).
    """
    was_pre_v3 = sv.pre_version in (1, 2)

    if isinstance(sv, SubvolV3Helper):
        v3_path = sv.getpath()
        if sv.uuid is None:
            sv.set_uuid_from_path(v3_path)
        if not sv.get_incar(sv.uuid):
            sv.add_incar(sv.uuid)
        v3 = sv
    else:
        if sv.data_path:
            sv.set_uuid_from_data_path(sv.data_path)
        # XXX: triggering auto-upgrade
        v3_path = sv.getpath()
        sv.set_uuid_from_path(v3_path)
        # building v3 object (and repoint bag refs), nothing more.
        v3 = _promote_v3(st, sv, snap_names=st.snaps.get(sv.name, []))

    # verify the path returned by last getpath cmd
    assert v3_path == v3.gen_mnt_path(), \
        f'{v3}: getpath {v3_path} != {v3.gen_mnt_path()}'
    v3.verify_uuid()
    v3.verify_layout()
    v3.verify_meta_file()
    v3.verify_subvol_ls_cmd()
    v3.verify_subvol_getpath_cmd()

    if was_pre_v3 and v3.data_path:
        old_uuid = v3.data_path.rsplit('/', 1)[1]
        assert v3.uuid == old_uuid, \
            f'{v3}: uuid changed {old_uuid} -> {v3.uuid}'
        old_dir = _admin_sh(
            st, f'test -e {v3.data_path} && echo yes || echo no').strip()
        assert old_dir == 'no', \
            f'{v3}: v{v3.pre_version} data dir {v3.data_path} still exists'
    return v3_path.lstrip('/')


def _assert_usable(st, sv):
    """
    The subvolume is still v1/v2 and subvolume commands on it work as usual.
    """
    path = sv.getpath()
    assert path == f'/{sv.data_path}', f'{sv}: getpath returned {path}'
    info = sv.info()
    assert info['path'] == f'/{sv.data_path}', f'{sv}: info = {info}'
    _assert_snaps(st, sv)
    _assert_pre_v3(st, sv)
    _assert_attrs(st, sv, sv.data_path)


# ----- subvolume attributes -----


def _assert_attrs(st, sv, path):
    """
    The subvolume (whose data dir is at path) has the attributes it was
    created with: as the file system has them on its data dir (compared with
    those of its pre-v3 data dir at the end of setup), as reported by
    `fs subvolume info` (also compared with what the old ceph-mgr reported,
    if it could be queried), and its user metadata.
    """
    if sv.fs_attrs is not None:
        diff = _diff(sv.fs_attrs, _fs_attrs(st, path))
        assert not diff, \
            f'{sv}: data dir attributes changed on {path} (expected, actual): {diff}'

    info = sv.info()
    log.info(f'{sv}: info = {info}')
    expected = sv.expected_info()
    if sv.recorded_info:
        expected.update({k: sv.recorded_info.get(k) for k in INFO_KEPT})
        # v1 subvolumes gain features on getting upgraded
        if sv.pre_version == 2:
            expected['features'] = sv.recorded_info.get('features')
    actual = {k: info.get(k) for k in expected}
    for d in (expected, actual):
        d['mode'] &= 0o7777
    diff = _diff(expected, actual)
    assert not diff, f'{sv}: attributes changed (expected, actual): {diff}'

    metadata = sv.metadata_ls()
    expected = sv.metadata or {}
    assert metadata == expected, f'{sv}: metadata = {metadata}, expected {expected}'


def _fs_attrs(st, path):
    """
    Return the attributes of a data dir as the file system has them: owner
    and mode, the layout and quota vxattrs (empty if not set), and all the
    user xattrs and POSIX ACLs (hex encoded). Times are left out, as are
    security.* xattrs (e.g., SELinux labels), which users don't set.
    """
    vxattrs = ''.join(f'echo "{x}=$(getfattr --only-values -n {x} {path} 2>/dev/null)"\n'
                      for x in FS_VXATTRS)
    out = _admin_sh(st, f'stat -c "%u %g %a" {path}\n{vxattrs}'
                        f'getfattr -d -m - -e hex --absolute-names {path}\n')
    lines = out.splitlines()
    uid, gid, mode = lines[0].split()
    attrs = dict(uid=uid, gid=gid, mode=mode)
    for line in lines[1:]:
        name, sep, val = line.partition('=')
        if sep and (name in FS_VXATTRS or name.startswith(('user.', 'system.'))):
            attrs[name] = val
    return attrs


def _record_fs_attrs(st, sv):
    """
    Record the attributes of the pre-v3 data dir as the file system has
    them, which must match the helper's create-option / hand-set fields.
    """
    attrs = _fs_attrs(st, sv.data_path)
    log.info(f'{sv}: data dir attributes = {attrs}')
    expected = sv.expected_fs_attrs()
    diff = {k: (v, attrs.get(k)) for k, v in expected.items() if attrs.get(k) != v}
    assert not diff, f'{sv}: data dir attributes (expected, actual): {diff}'
    sv.fs_attrs = attrs


def _getfacl(st, path):
    return _admin_sh(st, f'getfacl -cpn {path}')


def _set_data_dir_attrs(st, sv):
    """
    Set hand-set data-dir attrs from the helper (user_xattr, max_files, acl).
    """
    cmds = ['set -e']
    if sv.user_xattr is not None:
        name, val = sv.user_xattr
        cmds.append(f'setfattr -n {name} -v {val} {sv.data_path}')
    if sv.max_files is not None:
        cmds.append(f'setfattr -n ceph.quota.max_files -v {sv.max_files} '
                     f'{sv.data_path}')
    if sv.acl is not None:
        cmds.append(f'setfacl -m {sv.acl} {sv.data_path}')
    cmds.append(f'touch {sv.data_path}/{BOUNDARY_FILE}')
    _admin_sh(st, '\n'.join(cmds))
    if sv.acl is not None:
        acl = _getfacl(st, sv.data_path)
        assert 'user:1001:r-x' in acl and 'default:user:1001:r-x' in acl, \
            f'{sv}: ACL not set:\n{acl}'


def _probe_boundary(st, sv, path):
    """
    Hard link a file in the data dir (at path) to the subvolume group dir,
    the subvolume dir, and (for v3) the incarnation dir, returning whether
    each is allowed ('ok') or crosses a subvolume boundary ('EXDEV'), so as
    to find where the subvolume is marked (ceph.dir.subvolume).
    """
    targets = {'group': (sv.gen_base_path()).lstrip('/').rsplit('/', 1)[0], 'subvolume': (sv.gen_base_path()).lstrip('/')}
    if path.endswith('/mnt'):
        targets['incarnation'] = path.rsplit('/', 1)[0]
    result = {}
    for what, d in targets.items():
        dst = f'{d}/{BOUNDARY_FILE}.{sv.name}.link'
        out = _admin_sh(st, f'ln {path}/{BOUNDARY_FILE} {dst} 2>&1 && '
                            f'rm -f {dst} && echo ok || true').strip()
        if out != 'ok':
            assert 'cross-device' in out, f'{sv}: ln to {d}: {out}'
            out = 'EXDEV'
        result[what] = out
    log.info(f'{sv}: subvolume boundary probe from {path}: {result}')
    return result


def _record_attrs(st, sv):
    """
    Record the attributes of the subvolume as reported by the (old)
    ceph-mgr.
    """
    sv.recorded_info = sv.info()


def _create_group(st, group, attrs=False):
    """
    Create a subvolume group once, with the GRP_* attributes and DATA_POOL_2
    if attrs.
    """
    created = st.__dict__.setdefault('_groups_created', set())
    if group in created:
        if attrs:
            assert group in st.groups, \
                f'group {group}: recreated without GRP_* attrs'
        return
    opts = ''
    if attrs:
        opts = (f'--size {GRP_SIZE} --pool_layout {DATA_POOL_2} '
                f'--uid {GRP_UID} --gid {GRP_GID} --mode {GRP_MODE:o}')
        st.groups[group] = None
    _ops(st).group_create(VOLNAME, group, opts)
    created.add(group)


def _assert_groups(st):
    """
    The subvolume groups created with GRP_* attributes have them, as the old
    ceph-mgr reported them (once recorded).
    """
    ops = _ops(st)
    for group, recorded in st.groups.items():
        info = ops.group_info(VOLNAME, group)
        log.info(f'group {group}: info = {info}')
        expected = dict(uid=GRP_UID, gid=GRP_GID, mode=GRP_MODE,
                        data_pool=DATA_POOL_2, bytes_quota=GRP_SIZE)
        if recorded:
            expected.update({k: recorded.get(k) for k in GRP_INFO_KEPT})
        actual = {k: info.get(k) for k in expected}
        for d in (expected, actual):
            d['mode'] &= 0o7777
        diff = _diff(expected, actual)
        assert not diff, \
            f'group {group}: attributes changed (expected, actual): {diff}'


def _record_groups(st):
    ops = _ops(st)
    for group in st.groups:
        st.groups[group] = ops.group_info(VOLNAME, group)


def _assert_clone_attrs(st, c, src):
    """
    The clone has the attributes of its source (those of the snapshot cloned
    being the same), data pool included, as the old ceph-mgr reported them.
    Not checked for a source without them recorded (e.g., one removed with
    retained snapshots, which `fs subvolume info` doesn't report them for).
    """
    if not src.recorded_info:
        return
    info = c.info()
    expected = {k: src.recorded_info.get(k) for k in CLONE_INFO_KEPT}
    actual = {k: info.get(k) for k in CLONE_INFO_KEPT}
    for d in (expected, actual):
        d['mode'] &= 0o7777
    diff = _diff(expected, actual)
    assert not diff, \
        f'clone {c}: attributes differ from source {src} (expected, actual): {diff}'


# ----- snapshots -----


def _pre_v3_snap_path(sv, snap, upgraded=False):
    """
    Path of a pre-v3 snapshot relative to the fs root.
    """
    if upgraded and isinstance(sv, SubvolV3Helper):
        if sv.pre_version == 1:
            return sv.gen_v1_snap_path(snap).lstrip('/')
        return sv.gen_v2_snap_data_path(snap).lstrip('/')
    # Still a V1/V2 helper (or uuid known from data_path)
    if not sv.uuid and sv.data_path:
        sv.uuid = sv.data_path.rsplit('/', 1)[1]
    return sv.gen_pre_v3_snap_path(snap, upgraded=upgraded).lstrip('/')


def _snap_create(st, sv, snap, pre_v3=False, direct=False):
    """
    Snapshot the subvolume through ceph-mgr, or directly (pre_v3 only), the
    way ceph-mgr would have. Direct v1 snaps use SubvolV1Helper.custom_create_snap
    (mksnap, not via mgr) so the old mgr does not upgrade v1 -> v2.
    """
    if direct:
        assert pre_v3 and isinstance(sv, SubvolV1Helper)
        sv.custom_create_snap(snap)
    else:
        # SubvolV1/V2/V3Helper.snap_create tracks snap_names / incar
        sv.snap_create(snap)
    st.snaps.setdefault(sv.name, set()).add(snap)
    if pre_v3:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: v{sv.pre_version} snapshot {snap} not at {path}'
        st.snap_manifests[(sv.name, snap)] = _manifest(st, path)


def _assert_snaps(st, sv, upgraded=False):
    """
    Snapshot listing has exactly the snapshots taken so far (v1/v2 as well
    as v3 ones), and each of them can be queried. upgraded is whether the
    subvolume has been auto-upgraded.
    """
    names = {s['name'] for s in sv.snap_ls()}
    expected = st.snaps.get(sv.name, set())
    assert names == expected, f'{sv}: snapshots = {names}, expected {expected}'
    for snap in names:
        info = sv.snap_info(snap)
        recorded = st.snap_info.get((sv.name, snap))
        if recorded:
            diff = {k: (recorded.get(k), info.get(k)) for k in SNAP_INFO_KEPT
                    if recorded.get(k) != info.get(k)}
            assert not diff, \
                f'{sv}: snapshot {snap} info changed (expected, actual): {diff}'
        if (sv.name, snap) in st.snap_manifests and not sv.retained_snaps:
            # a pre-v3 snapshot (snapshot metadata of a subvolume with
            # retained snapshots can't be queried)
            metadata = sv.snap_metadata_ls(snap)
            assert metadata == {SNAP_METADATA_KEY: snap}, \
                f'{sv}: snapshot {snap} metadata = {metadata}'
        if (sv.name, snap) in st.snap_manifests:
            _assert_snap_getpath(st, sv, snap, upgraded)


def _assert_snap_getpath(st, sv, snap, upgraded):
    """
    `fs subvolume snapshot getpath` of a pre-v3 snapshot returns where the
    snapshot is, before and after auto-upgrade, and the snapshot there has
    the data it was taken with. Where the snapshots of a subvolume removed
    with --retain-snapshots are after auto-upgrade is unspecified, so that
    path is only logged.
    """
    path = sv.snap_getpath(snap)
    if sv.retained_snaps and upgraded:
        log.info(f'{sv}: snapshot {snap} getpath returned {path}')
    else:
        expected = f'/{_pre_v3_snap_path(sv, snap, upgraded)}'
        assert path == expected, \
            f'{sv}: snapshot {snap} getpath returned {path}, expected {expected}'
        if upgraded and isinstance(sv, SubvolV3Helper):
            sv.verify_snap_getpath_cmd(snap, version=sv.pre_version)
    assert _manifest(st, path.lstrip('/')) == st.snap_manifests[(sv.name, snap)], \
        f'{sv}: data at {path} does not match snapshot {snap}'


def _record_snap_info(st, sv):
    for snap in st.snaps.get(sv.name, set()):
        st.snap_info[(sv.name, snap)] = sv.snap_info(snap)


def _assert_pending_clones(st, sv, snap, expected):
    info = sv.snap_info(snap)
    assert info.get('has_pending_clones') == expected, \
        f'{sv}: snapshot {snap} info = {info}, expected has_pending_clones = {expected}'


# ----- IO writers (RemoteIoLoad from helpers/gen_io_load.py) -----




def _remount(st, mount, sv, path, **kwargs):
    """
    Remount the client at the subvolume's data dir (path), and restore the
    data dir's mode, which the mount helpers set to 1777 (on the mount root)
    after mounting. The five digits clear the sticky bit too.
    """
    mount.remount(cephfs_mntpt=f'/{path}', **kwargs)
    _admin_sh(st, f'chmod {sv.mode:05o} {path}')


def _remount_and_verify(st, w, path, client_id, **creds):
    """
    Remount the client at the subvolume's (new) v3 path with the given
    credentials (client_keyring or client_keyring_path), and verify that all
    the data written by the writer is accessible and intact, and that the
    subvolume is writable.
    """
    mount, sv = w.mount, w.sv
    log.info(f'{sv}: remounting as client.{client_id} at /{path}')
    _remount(st, mount, sv, path, client_id=client_id, **creds)
    w.verify_data()
    mount.run_shell(['sudo', 'bash', '-c',
                     f'echo {client_id} > io/post-remount && '
                     f'grep -q {client_id} io/post-remount && '
                     'rm io/post-remount'])


def _kill_writers(st):
    for w in st.writers:
        w.kill()


def _kill_writers_on_failure(func):
    """
    Writers left running (in the subvolume mounts) make the client mounts
    fail to unmount on teardown, as do the extra (ceph-fuse) sessions'
    mounts the test dir removal.
    """
    @functools.wraps(func)
    def wrapper(ctx, config):
        try:
            return func(ctx, config)
        except Exception:
            try:
                _kill_writers(_state(ctx))
            except Exception:
                log.exception('failed to kill writers')
            try:
                _cleanup_evict_mounts(_state(ctx))
            except Exception:
                log.exception('failed to unmount extra sessions')
            raise
    return wrapper


# ----- auth IDs and evict -----


def _authorized_ids(st, sv):
    return {k for e in sv.authorized_list() for k in e}


def _mgr_auth_ids(st, i, post=False):
    """
    Auth IDs authorized for IO subvolume i through ceph-mgr (with
    `fs subvolume authorize`): those of the client (v2 only, v1 credentials
    being made by hand), of the extra session (if any), and the client's
    new one (post auto-upgrade).
    """
    sv = st.io_svs[i]
    ids = set()
    if sv.pre_version == 2:
        ids.add(f'svio{i}')
    if sv.name in st.evict:
        ids.add(st.evict[sv.name]['auth_id'])
    if post:
        ids.add(f'svio{i}new')
    return ids


def _assert_authorized(st, post=False):
    for i, sv in enumerate(st.io_svs):
        if sv.pre_version == 1 and not st.snaps.get(sv.name) and not post:
            # nothing authorized through ceph-mgr, see _setup_evict()
            continue
        ids = _authorized_ids(st, sv)
        expected = _mgr_auth_ids(st, i, post)
        assert ids == expected, f'{sv}: authorized_list = {ids}, expected {expected}'


def _sessions(st, auth_id):
    """
    Return the roots of the sessions of auth_id across all active ranks.
    """
    out = st.manager.get_ceph_cmd_stdout(f'fs get {VOLNAME} --format=json')
    max_mds = json.loads(out)['mdsmap']['max_mds']
    roots = []
    for rank in range(max_mds):
        out = st.manager.get_ceph_cmd_stdout(
            f'tell mds.{VOLNAME}:{rank} client ls --format=json')
        for session in json.loads(out):
            md = session.get('client_metadata', {})
            if md.get('entity_id') == auth_id:
                roots.append(md.get('root'))
    return roots


def _fuse_mount(st, auth_id, keyring_path, root, mnt, check_status=True):
    remote = st.admin.client_remote
    remote.run(args=['mkdir', '-p', mnt])
    p = remote.run(args=['sudo', 'timeout', '300', 'ceph-fuse',
                         '-c', '/etc/ceph/ceph.conf', '--id', auth_id,
                         '-k', keyring_path, '-r', f'/{root}', mnt],
                   check_status=check_status)
    return p.exitstatus


def _fuse_umount(st, mnt, auth_id=None):
    """
    Unmount an extra session's mount cleanly, so that ceph-fuse closes its
    session. A killed ceph-fuse leaves its session (and the caps it holds,
    e.g. on files the IO clients write) behind until the MDS autocloses it
    (after 300s). So, if it does not go away, evict its session (by
    auth_id).
    """
    remote = st.admin.client_remote
    # [c]: so as not to match the (remote) shell running pgrep/pkill
    pattern = f'[c]eph-fuse.*{mnt}'
    remote.run(args=['sudo', 'umount', mnt], check_status=False)

    def gone():
        return remote.run(args=['pgrep', '-f', pattern],
                          check_status=False).exitstatus != 0
    try:
        _wait_for(f'ceph-fuse at {mnt} to exit', gone, timeout=60)
    except Exception:
        # ceph-fuse mounted before the MDS upgrade may not exit after a
        # (successful) unmount: https://tracker.ceph.com/issues/64502
        log.warning(f'ceph-fuse at {mnt} did not exit after unmount, killing it')
        remote.run(args=['sudo', 'umount', '-l', mnt], check_status=False)
        remote.run(args=['sudo', 'pkill', '-KILL', '-f', pattern],
                   check_status=False)
        if auth_id:
            _evict_sessions(st, auth_id)
    remote.run(args=['rmdir', mnt], check_status=False)


def _evict_sessions(st, auth_id):
    p = st.manager.run_ceph_cmd(f'fs get {VOLNAME} --format=json',
                                check_status=False, stdout=StringIO())
    max_mds = (json.loads(p.stdout.getvalue())['mdsmap']['max_mds']
               if p.exitstatus == 0 else 1)
    for rank in range(max_mds):
        st.manager.run_ceph_cmd(
            f'tell mds.{VOLNAME}:{rank} client evict '
            f'client_metadata.entity_id={auth_id}', check_status=False)


def _cleanup_evict_mounts(st):
    for e in st.evict.values():
        _fuse_umount(st, e['mnt'], e['auth_id'])


def _setup_evict(st):
    """
    Authorize an auth ID for each IO subvolume through the old ceph-mgr, and
    mount the subvolume (pre-v3 data dir) with it using ceph-fuse on the
    admin client's node. A v1 subvolume without snapshots is skipped, as
    the old ceph-mgr would upgrade it to v2.
    """
    remote = st.admin.client_remote
    for i, sv in enumerate(st.io_svs):
        if sv.pre_version == 1 and not st.snaps.get(sv.name):
            log.info(f'{sv}: v1 without snapshots, not authorizing through ceph-mgr')
            continue
        auth_id = f'svev{i}'
        keyring = sv.authorize(auth_id, access_level='rw',
                                                rotate_aes=True)
        keyring_path = f'{st.testdir}/volumes_upgrade.{auth_id}.keyring'
        remote.write_file(keyring_path, keyring)
        mnt = f'{st.testdir}/volumes_upgrade.evict.{i}'
        st.evict[sv.name] = dict(auth_id=auth_id, keyring=keyring_path,
                                 mnt=mnt, root=sv.data_path)
        _fuse_mount(st, auth_id, keyring_path, sv.data_path, mnt)
        remote.run(args=['sudo', 'stat', f'{mnt}/{BOUNDARY_FILE}'])
        roots = set(_sessions(st, auth_id))
        assert roots == {f'/{sv.data_path}'}, f'{sv}: sessions of {auth_id}: {roots}'


def _verify_deauthorize_evict(st, i, path):
    """
    After auto-upgrade, deauthorizing the auth ID of the extra session must
    revoke its access to the subvolume (both the pre-v3 and v3 paths), and
    evicting it must evict the session, mounted at the pre-v3 data dir.
    """
    sv = st.io_svs[i]
    e = st.evict.get(sv.name)
    if not e:
        return
    auth_id, remote = e['auth_id'], st.admin.client_remote

    # the session survived the upgrade, and the subvolume is accessible
    roots = set(_sessions(st, auth_id))
    assert roots == {f'/{e["root"]}'}, f'{sv}: sessions of {auth_id}: {roots}'
    remote.run(args=['sudo', 'timeout', '300', 'stat', f'{e["mnt"]}/{BOUNDARY_FILE}'])

    sv.deauthorize(auth_id)
    ids = _authorized_ids(st, sv)
    assert auth_id not in ids, f'{sv}: {auth_id} still in authorized_list {ids}'
    p = st.manager.run_ceph_cmd(f'auth get client.{auth_id} --format=json',
                                check_status=False, stdout=StringIO())
    if p.exitstatus == 0:
        mds_caps = json.loads(p.stdout.getvalue())[0]['caps'].get('mds', '')
        for root in (e['root'], path):
            assert f'path=/{root}' not in mds_caps, \
                f'{sv}: deauthorized {auth_id} has caps for /{root}: {mds_caps}'

    sv.evict(auth_id)
    _wait_for(f'sessions of {auth_id} to be evicted',
              lambda: not _sessions(st, auth_id), timeout=120)
    _fuse_umount(st, e['mnt'], auth_id)

    # the old key must not get access to the subvolume any more
    rc = _fuse_mount(st, auth_id, e['keyring'], path, e['mnt'], check_status=False)
    if rc == 0:
        _fuse_umount(st, e['mnt'], auth_id)
    assert rc != 0, f'{sv}: deauthorized {auth_id} could mount /{path}'
    _fuse_umount(st, e['mnt'], auth_id)
    del st.evict[sv.name]


# ----- async jobs -----


def _clone_state(st, sv):
    """
    Read the clone state from the .meta file directly: "fs clone status"
    itself opens the subvolume, triggering auto-upgrade.
    """
    return _read_meta(st, sv).get('GLOBAL', 'state')


def _clone(st, src, snap, c):
    if c.grp_name:
        _ops(st).group_create(VOLNAME, c.grp_name)
    src.snap_clone(snap, c.name, target_grp_name=c.grp_name)


def _wait_for_clone(st, c):
    def done():
        status = c.clone_status()['status']
        assert status['state'] in ('pending', 'in-progress', 'complete'), \
            f'clone {c}: status = {status}'
        return status['state'] == 'complete'
    _wait_for(f'clone {c} to complete', done, timeout=1800, sleep=10)


def _assert_clone_data(st, c, src, snap, path=None):
    if path is None:
        path = _relpath(st, c)
    assert _manifest(st, path) == st.snap_manifests[(src.name, snap)], \
        f'clone {c}: data does not match snapshot {snap} of {src}'


def _trash_entries(st):
    out = _admin_sh(st, f'test -d {TRASH_DIR} && '
                        f'find {TRASH_DIR} -mindepth 1 -maxdepth 1 | wc -l || echo 0')
    return int(out.strip())


def _setup_clones(st):
    sv = st.src
    _create_pre_v3(st, sv)
    _populate(st, sv.data_path, st.clone_src_files)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    # a second snapshot, with different contents
    _admin_sh(st, f'mkdir {sv.data_path}/extra && touch {sv.data_path}/extra/file')
    _pre_v3_snap_create(st, sv, SNAPNAME_2)

    # the old ceph-mgr's cloner threads pick up the clones and sleep on the
    # delay (with the clones pending) until it gets upgraded. the delay is
    # reset at the end of setup (by then, the clones have long been picked
    # up), so that the new ceph-mgr starts them right away.
    _set_mgr_config(st, 'max_concurrent_clones', MAX_CONCURRENT_CLONES)
    _set_mgr_config(st, 'snapshot_clone_delay', CLONE_DELAY)
    clones, st.clones = st.clones, []
    for c in clones:
        _clone(st, sv, SNAPNAME, c)
        _register_clone(st, c, sv, SNAPNAME)

    # a clone of an IO subvolume, whose client keeps writing to it
    (name, group), i = IO_CLONE
    io_sv = st.io_svs[i]
    assert io_sv.io_nsnaps >= 1, f'{io_sv}: IO_CLONE needs a snap'
    io_snap = f'snap_v{io_sv.pre_version}_0'
    c = _new_sv(st, name, group, 2)
    _clone(st, io_sv, io_snap, c)
    _register_clone(st, c, io_sv, io_snap)

    # a clone of a subvolume that is then removed with --retain-snapshots
    # (in _setup_purges())
    rsv = st.retained[0]
    _create_retained(st, rsv, 500)
    c = _new_sv(st, *RETAINED_CLONE, 2)
    _clone(st, rsv, SNAPNAME, c)
    _register_clone(st, c, rsv, SNAPNAME)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    assert len(st.clones) <= MAX_CONCURRENT_CLONES


def _register_clone(st, c, src, snap):
    """
    Record a clone (issued with snapshot_clone_delay set) to be held pending
    through the upgrade. Clones are always v2 (as the old ceph-mgr creates them).
    """
    c.data_path = (_read_meta(st, c).get('GLOBAL', 'path')).lstrip('/')
    c.set_uuid_from_data_path(c.data_path)
    assert _clone_state(st, c) == 'pending'
    st.clones.append(c)
    st.clone_of[c.name] = (src, snap)


def _wait_for_clones(st, verify):
    """
    Wait for the clones held pending through the upgrade to finish, and
    verify their data (verify(c) returns the clone's data dir) and
    attributes.
    """
    for c in st.clones:
        _wait_for_clone(st, c)
        src, snap = st.clone_of[c.name]
        _assert_clone_data(st, c, src, snap, path=verify(c))
        _assert_clone_attrs(st, c, src)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'no')


def _assert_clones_pending(st):
    for c in st.clones:
        state = _clone_state(st, c)
        copied = _count_files(st, c.data_path)
        log.info(f'clone {c}: state = {state}, copied {copied}')
        assert state == 'pending' and copied == 0, \
            f'clone {c} was not held pending (state = {state}, copied {copied})'


def _setup_purges(st):
    for sv in st.purges:
        if sv.grp_name:
            _ops(st).group_create(VOLNAME, sv.grp_name)
        sv.create(default_mode=False)
        path = sv.getpath()
        sv.set_uuid_from_path(path)
        sv.data_path = path.lstrip('/')
        _populate(st, sv.data_path, st.purge_files)
    _create_retained(st, st.retained[1], st.purge_files)

    _set_mgr_config(st, 'pause_purging', 'true')
    for sv in st.purges:
        sv.rm()
    # moves their data dirs (but not their snapshots) to the trash; the
    # (first) one has a clone of its snapshot pending
    for sv in st.retained:
        sv.rm('--retain-snapshots')
        _mark_retained(sv)
        _assert_retained_pre_v3(st, sv)
    total = _count_files(st, TRASH_DIR)
    assert _trash_entries(st) == len(st.purges) + len(st.retained)

    # as with clones, catch the purges mid-way
    _set_mgr_config(st, 'pause_purging', 'false')
    _wait_for('purges to make progress',
              lambda: _count_files(st, TRASH_DIR) <= total * 0.9,
              timeout=600, sleep=1)
    _set_mgr_config(st, 'pause_purging', 'true')
    left = _count_files(st, TRASH_DIR)
    log.info(f'purges paused with {left}/{total} files and '
             f'{_trash_entries(st)} trash entries left')
    assert left > 0, 'purges were not caught mid-way, increase purge_files'


def _create_retained(st, sv, nfiles):
    """
    Create a (v2, regardless of the version of the other subvolumes) subvolume
    with a snapshot, to be removed with --retain-snapshots.
    """
    if sv.grp_name:
        _ops(st).group_create(VOLNAME, sv.grp_name)
    sv.create(default_mode=False)
    path = sv.getpath()
    sv.set_uuid_from_path(path)
    sv.data_path = path.lstrip('/')
    _populate(st, sv.data_path, nfiles)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    _record_snap_info(st, sv)


def _assert_retained(st, sv, upgraded=False):
    """
    The subvolume, removed with --retain-snapshots, is listed (with its
    snapshots) and reported as such.
    """
    info = sv.info()
    assert info.get('state') == STATE_RETAINED, f'{sv}: info = {info}'
    names = {s['name'] for s in sv.ls()}
    assert sv.name in names, f'{sv}: not in subvolume ls: {names}'
    _assert_snaps(st, sv, upgraded)


def _assert_retained_pre_v3(st, sv):
    """
    The subvolume, removed with --retain-snapshots, is still v2 (its data dir
    gone, its snapshots in place), and is reported as such.
    """
    ftype = _admin_sh(st, f'stat -c %F {(sv.gen_base_path()).lstrip('/')}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    glob = dict(_read_meta(st, sv).items('GLOBAL'))
    assert (glob.get('version'), glob.get('state'), glob.get('path')) == \
        ('2', STATE_RETAINED, ''), f'{sv}: meta = {glob}'
    out = _admin_sh(st, f'test -e {sv.data_path} && echo yes || echo no; '
                        f'test -e {(sv.gen_base_path()).lstrip('/')}/roots && echo yes || echo no')
    assert out.split() == ['no', 'no'], \
        f'{sv}: data dir, roots dir present?: {out.split()}'
    for snap in st.snaps[sv.name]:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: retained snapshot {snap} not at {path}'
    _assert_retained(st, sv)


def _verify_retained(st, sv, clone=None):
    """
    After auto-upgrade: the retained snapshot can still be cloned (into
    clone, if given), and removing it, the last one, removes the subvolume.
    """
    _assert_retained(st, sv, upgraded=True)
    log.info(f'{sv}: after auto-upgrade:\n' +
             _admin_sh(st, f'ls -la {(sv.gen_base_path()).lstrip('/')} {(sv.gen_base_path()).lstrip('/')}/roots 2>&1 || true; '
                           f'cat {(sv.gen_base_path()).lstrip('/')}/.meta'))
    if clone:
        _clone(st, sv, SNAPNAME, clone)
        _wait_for_clone(st, clone)
        _assert_clone_data(st, clone, sv, SNAPNAME, path=_assert_v3(st, clone))
        _assert_pending_clones(st, sv, SNAPNAME, 'no')
    sv.snap_rm(SNAPNAME)
    st.snaps[sv.name].discard(SNAPNAME)
    assert not sv.exists(), \
        f'{sv}: still exists after removing its last snapshot'
    names = {s['name'] for s in sv.ls()}
    assert sv.name not in names, f'{sv}: still listed: {names}'


def _setup_io(st):
    """
    Create IO subvolumes, remount clients on their data dirs with subvol
    caps, and start RemoteIoLoad writers — same shape as
    test_v2_to_v3.TestWithIoLoad (authorize → remount → GenIoLoad/RemoteIoLoad).
    """
    for i, (mount, sv) in enumerate(zip(st.io_mounts, st.io_svs)):
        _create_pre_v3(st, sv)
        client_id = f'svio{i}'
        keyring = _pre_v3_authorize(st, sv, client_id)
        _remount(st, mount, sv, sv.data_path, client_id=client_id,
                 client_keyring=keyring)
        statedir = f'{st.testdir}/volumes_upgrade.io.{sv.name}'
        writer = RemoteIoLoad(mount, statedir, label=str(sv))
        writer.sv = sv
        writer.start()
        st.writers.append(writer)

    for w in st.writers:
        w.wait_for_progress(20)
        for j in range(w.sv.io_nsnaps):
            _pre_v3_snap_create(st, w.sv, f'snap_v{w.sv.pre_version}_{j}')
            w.wait_for_progress(20)


# ----- pre-v3 subvolumes -----
#
# v1 subvolumes can't be created by any release that can be upgraded from,
# so they are fabricated in the layout ceph-mgr used to create them in. A
# v1 subvolume without snapshots must not be accessed through the old
# ceph-mgr before the upgrade, as it would upgrade it to v2 (the new
# ceph-mgr does the same when only it is upgraded, which is fine).


def _data_pool(st):
    return _admin_sh(st, 'getfattr --only-values -n ceph.dir.layout.pool .').strip()


def _resolve_pool(st, sv):
    """Fill sv.pool from pool_layout / group_attrs / volume default if needed."""
    if sv.pool_layout is not None:
        sv.pool = sv.pool_layout
    elif sv.group_attrs:
        sv.pool = DATA_POOL_2
        if not sv.pass_uid_gid:
            sv.uid, sv.gid = GRP_UID, GRP_GID
    elif sv.pool is None:
        sv.pool = _data_pool(st)
    if sv.mode is None:
        sv.mode = DEFAULT_MODE
    if sv.namespace_isolated and not sv.namespace:
        sv.namespace = sv.gen_pool_namespace()


def _create_pre_v3(st, sv):
    """
    Create a v1 or v2 subvolume from the helper's create-option fields
    (pre_version selects fabricate vs fs subvolume create).
    """
    _resolve_pool(st, sv)
    if sv.grp_name:
        _create_group(st, sv.grp_name, attrs=sv.group_attrs)
    log.info(f'{sv}: creating v{sv.pre_version} subvolume '
             f'(size={sv.size}, mode={sv.mode}, pool={sv.pool}, '
             f'ns={sv.namespace_isolated}, earmark={sv.earmark})')
    if sv.pre_version == 1:
        # Fabricate via SubvolV1Helper.custom_create (old SubvolumeV1.create
        # layout); cannot use mgr create on a release that still auto-upgrades
        # v1 -> v2 on open.
        sv.custom_create(populate_files=False)
        sv.data_path = (sv.gen_mnt_path()).lstrip('/')
        log.info(f'{sv}: created v1 subvolume at {sv.data_path}:\n'
                 f'{_admin_sh(st, f"cat {(sv.gen_base_path()).lstrip('/')}/.meta")}')
    else:
        assert sv.pre_version == 2, sv.pre_version
        # default_mode=False: pass --mode from sv.mode (mgr default is 0755,
        # not the helper unittest default of 0777); metadata applied by create()
        sv.create(default_mode=False)
        path = sv.getpath()
        sv.set_uuid_from_path(path)
        sv.data_path = path.lstrip('/')
        if sv.namespace_isolated:
            ns = _admin_sh(
                st, 'getfattr --only-values -n ceph.dir.layout.pool_namespace '
                    f'{sv.data_path}').strip()
            assert ns, f'{sv}: no pool namespace'
            assert ns == sv.namespace, \
                f'{sv}: namespace {ns} != expected {sv.namespace}'
    _set_data_dir_attrs(st, sv)
    # the data dir must be in a subvolume
    probe = _probe_boundary(st, sv, sv.data_path)
    assert probe['group'] == 'EXDEV', f'{sv}: not marked a subvolume: {probe}'


def _pre_v3_authorize(st, sv, auth_id):
    if sv.pre_version == 2:
        return sv.authorize(auth_id, access_level='rw', rotate_aes=True)
    return sv.custom_subvol_auth(
        auth_id, sv.pool, namespace=sv.namespace, rotate_aes=True)


def _pre_v3_snap_create(st, sv, snap):
    """
    Snapshot a pre-v3 subvolume through the old ceph-mgr, except for the
    first snapshot of a v1 subvolume (taken directly in its data dir, as v1
    does), which keeps the old ceph-mgr from upgrading it to v2.
    """
    direct = sv.pre_version == 1 and not st.snaps.get(sv.name)
    _snap_create(st, sv, snap, pre_v3=True, direct=direct)
    sv.snap_metadata_set(snap, SNAP_METADATA_KEY, snap)


def _teardown(st):
    """
    Stop whatever setup left running (writers, extra sessions) and remove the
    files it created in the test dir, so that the client mounts can be
    unmounted and the test dir removed, whether or not the job failed.
    """
    try:
        _kill_writers(st)
    except Exception:
        log.exception('failed to kill writers')
    try:
        _cleanup_evict_mounts(st)
    except Exception:
        log.exception('failed to unmount extra sessions')
    remotes = {m.client_remote for m in st.io_mounts + [st.admin]}
    for remote in remotes:
        # --one-file-system: don't descend into an extra session's mount
        # that failed to unmount
        remote.run(args=['sudo', 'bash', '-c',
                         f'rm -rf --one-file-system {st.testdir}/volumes_upgrade.* '
                         f'{st.testdir}/{RemoteIoLoad.SCRIPT_BASENAME}'],
                   check_status=False)


# ----- task entry points -----


def _check_io(st, nfiles=10):
    for w in st.writers:
        w.wait_for_progress(nfiles)


def _setup(st):
    """
    On the old release: create mixed v1 (fabricated) and v2 (fs subvolume
    create) subvolumes, start writers, hold clones pending, pause purges.
    """
    st.manager.run_ceph_cmd(f'osd pool create {DATA_POOL_2}')
    st.manager.run_ceph_cmd(f'fs add_data_pool {VOLNAME} {DATA_POOL_2}')

    _setup_io(st)
    _setup_evict(st)
    _setup_clones(st)
    for sv in st.no_io_svs:
        _create_pre_v3(st, sv)
    _setup_purges(st)
    _assert_clones_pending(st)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    _set_mgr_config(st, 'snapshot_clone_delay', 0)

    for sv in _pre_v3_svs(st):
        _record_fs_attrs(st, sv)
        if sv.pre_version == 1 and not st.snaps.get(sv.name):
            _assert_pre_v3(st, sv, strict=True)
        else:
            # the old ceph-mgr must handle a v1 subvolume with snapshots as v1
            # (checks the attributes too)
            _assert_usable(st, sv)
            _record_attrs(st, sv)
            _record_snap_info(st, sv)
    _assert_groups(st)
    _record_groups(st)
    _assert_authorized(st)
    for c in st.clones:
        _assert_pre_v3(st, c)
    _ops(st).ls(VOLNAME)


@contextlib.contextmanager
def task(ctx, config):
    """
    Main entry point. Sets up pre-v3 subvolumes on the old release, yields
    while the suite upgrades the cluster, then verifies auto-upgrade.

    Example::

        tasks:
        - volumes_upgrade:
            io_clients: [client.0, client.1, client.2]
            admin_client: client.3
        # ... cluster upgrade ...
        # post-upgrade verification runs when this context exits
    """
    config = dict(config or {})
    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('volumes_upgrade', {}))

    st = _make_state(ctx, config)
    ctx[STATE_KEY] = st
    try:
        _setup(st)
        yield
        try:
            _post_upgrade(st)
        except Exception:
            try:
                _kill_writers(st)
            except Exception:
                log.exception('failed to kill writers')
            try:
                _cleanup_evict_mounts(st)
            except Exception:
                log.exception('failed to unmount extra sessions')
            raise
    finally:
        _teardown(st)



@_kill_writers_on_failure
def verify_mgr_first(ctx, config):
    """
    After a mgr-first upgrade (MDS still old): auto-upgrade cannot set
    ceph.dir.subvolume.prevpath. Verify v1/v2 keep working — IO, commands,
    clones and purges — before the rest of the cluster is upgraded.
    """
    st = _state(ctx)
    _check_io(st)

    for sv in _pre_v3_svs(st):
        _assert_usable(st, sv)
    _assert_groups(st)
    _assert_authorized(st)

    # a v1/v2 snapshot taken by the new mgr
    _pre_v3_snap_create(st, st.src, SNAP_MGR_ONLY)
    _assert_usable(st, st.src)

    # the new mgr does not honor pause_purging on startup, so the purges may
    # already be running. resume them explicitly anyway.
    _set_mgr_config(st, 'pause_purging', 'false')

    _wait_for('purges to finish with old MDS',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)
    for sv in st.purges:
        assert not sv.exists(), \
            f'purged subvolume {sv} still exists'

    def verify(c):
        _assert_usable(st, c)
        return c.data_path
    _wait_for_clones(st, verify)
    for sv in st.retained:
        _assert_retained_pre_v3(st, sv)

    # a new clone of a v1/v2 snapshot, by the new mgr
    c = _new_sv(st, 'subvol_clone_mgr_only', 'group_clone', 2)
    _clone(st, st.src, SNAPNAME_2, c)
    _wait_for_clone(st, c)
    _assert_clone_data(st, c, st.src, SNAPNAME_2)
    _assert_clone_attrs(st, c, st.src)
    _assert_snaps(st, c)
    st.late_clones.append((c, st.src, SNAPNAME_2))
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')

    for sv in st.io_svs:
        _assert_usable(st, sv)
    _check_io(st)


def _post_upgrade(st):
    """
    Whole cluster upgraded: verify auto-upgrade while clients do IO, that
    clones and purges finish, and that pre- and post-upgrade snapshots list.
    """
    _check_io(st)
    _set_mgr_config(st, 'pause_purging', 'false')

    # first subvolume command on each subvolume triggers its auto-upgrade;
    # the writers must carry on regardless.
    for w in st.writers:
        sv = w.sv
        _assert_attrs(st, sv, _assert_v3(st, sv))
        w.wait_for_progress(20)
    _assert_authorized(st)

    _wait_for_clones(st, lambda c: _assert_v3(st, c))
    for c, src, snap in st.late_clones:
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))
        _assert_clone_attrs(st, c, src)

    for sv in [st.src] + st.no_io_svs:
        _assert_attrs(st, sv, _assert_v3(st, sv))
    _assert_groups(st)

    # where the upgraded subvolumes are marked as such must be the same as
    # for a subvolume created as v3
    fresh = _new_sv(st, 'subvol_fresh_v3', 'group_io', 3)
    fresh.create(default_mode=False)
    fresh_path = _assert_v3(st, fresh)
    _admin_sh(st, f'touch {fresh_path}/{BOUNDARY_FILE}')
    expected = _probe_boundary(st, fresh, fresh_path)
    for sv in _pre_v3_svs(st):
        probe = _probe_boundary(st, sv, _relpath(st, sv))
        assert probe == expected, \
            f'{sv}: subvolume boundary {probe}, expected {expected} (as for v3)'

    # v1/v2 snapshots must still be listed after auto-upgrade (with their
    # metadata and info intact, and getpath returning where they now are),
    # and listed along with v3 snapshots once those are taken.
    late = [c for c, _, _ in st.late_clones]
    for sv in _pre_v3_svs(st) + st.clones + late:
        _assert_snaps(st, sv, upgraded=True)
        _snap_create(st, sv, SNAP_V3)
        _assert_snaps(st, sv, upgraded=True)

    # clones of v1/v2 snapshots (taken pre-upgrade) must still work
    io_src = st.io_svs[-1]
    assert io_src.io_nsnaps >= 1, f'{io_src}: need a pre-v3 snap to clone'
    io_snap = f'snap_v{io_src.pre_version}_{io_src.io_nsnaps - 1}'
    for c, src, snap in [
            (_new_sv(st, 'subvol_clone_post_0', None, 3), st.src, SNAPNAME),
            (_new_sv(st, 'subvol_clone_post_1', 'group_clone', 3), st.src, SNAPNAME_2),
            (_new_sv(st, 'subvol_clone_post_2', 'group_clone', 3), io_src, io_snap)]:
        _clone(st, src, snap, c)
        _wait_for_clone(st, c)
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))
        _assert_clone_attrs(st, c, src)
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')
    _assert_pending_clones(st, io_src, io_snap, 'no')

    _wait_for('purges to finish', lambda: _trash_entries(st) == 0,
              timeout=1800, sleep=10)
    for sv in st.purges:
        assert not sv.exists(), \
            f'purged subvolume {sv} still exists'

    # the subvolumes removed with --retain-snapshots (whose data dirs have
    # been purged by now): clone the retained snapshot again, then remove it
    # (along with the subvolume)
    _verify_retained(st, st.retained[0],
                     _new_sv(st, 'subvol_clone_post_retained', 'group_clone', 3))
    _verify_retained(st, st.retained[1])
    _wait_for('subvolumes with retained snapshots to be purged',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)

    # stop writers and check nothing was lost (cf. GenIoLoad.verify_num_of_files_written)
    for w in st.writers:
        sv = w.sv
        written = w.stop()
        path = _relpath(st, sv)
        found = _count_files(st, f'{path}/io')
        log.info(f'{sv}: writer wrote {written} files, found {found} at {path}')
        assert found == written, f'{sv}: wrote {written} files, found {found}'

    # clients may choose to remount, in which case they mount the new v3
    # path, with either the credentials they had been using (authorized for
    # the v2 path) or with ones authorized after the upgrade.
    for i, w in enumerate(st.writers):
        sv, mount = w.sv, w.mount
        path = _relpath(st, sv)
        _remount_and_verify(st, w, path, mount.client_id,
                            client_keyring_path=mount.client_keyring_path)

        auth_id = f'svio{i}new'
        keyring = sv.authorize(auth_id, access_level='rw', rotate_aes=True)
        out = st.manager.get_ceph_cmd_stdout(
            f'auth get client.{auth_id} --format=json')
        mds_caps = json.loads(out)[0]['caps']['mds']
        assert f'path=/{path}' in mds_caps, \
            f'{sv}: caps of client.{auth_id} not for the v3 path: {mds_caps}'
        _remount_and_verify(st, w, path, auth_id, client_keyring=keyring)
    _assert_authorized(st, post=True)

    for i, sv in enumerate(st.io_svs):
        _verify_deauthorize_evict(st, i, _relpath(st, sv))

    _ops(st).ls(VOLNAME)
