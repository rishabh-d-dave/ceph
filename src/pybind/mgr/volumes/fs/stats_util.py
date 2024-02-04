'''
This module contains classes, methods & helpers that are used to get statistics
(specifically number of files and total size of data present under the source
and destination directory for the copy operation that is performed for snapshot
cloning) and pass, print, log and convert them to human readable format
conveniently.
'''
from os.path import join as os_path_join
from uuid import uuid4
from typing import Optional

from .operations.volume import open_volume_lockless, list_volumes
from .operations.subvolume import open_clone_sv_pair_in_vol
from .operations.clone_index import open_clone_index
from .operations.resolver import resolve_group_and_subvolume_name

from mgr_util import RTimer, format_bytes, format_dimless
from cephfs import ObjectNotFound


PATH_MAX = 4096


def get_size_ratio_str(size1, size2, human=True):
    if human:
        size1, size2 = format_bytes(size1, 4), format_bytes(size2, 4)

    size_string =  f'{size1}/{size2}'
    size_string = size_string.replace(' ', '')
    return size_string


def get_num_ratio_str(num1, num2, human=True):
    if human:
        num1, num2 = format_dimless(num1, 4), format_dimless(num2, 4)

    num_string = f'{num1}/{num2}'
    num_string = num_string.replace(' ', '')
    return num_string


def get_amount_copied(src_path, dst_path, fs_handle, human=True):
    rbytes = 'ceph.dir.rbytes'

    size_t = int(fs_handle.getxattr(src_path, rbytes))
    size_c = int(fs_handle.getxattr(dst_path, rbytes))

    percent: Optional[float]
    if size_t == 0 or size_c == 0:
        percent = 0
    else:
        percent = ((size_c/size_t) * 100)
        percent = round(percent, 3)

    return size_t, size_c, percent


def get_stats(src_path, dst_path, fs_handle, human=True):
    rentries = 'ceph.dir.rentries'
    rentries_t = int(fs_handle.getxattr(src_path, rentries))
    rentries_c = int(fs_handle.getxattr(dst_path, rentries))

    size_t, size_c, percent = get_amount_copied(src_path, dst_path, fs_handle,
                                                human)

    return {
        'percentage cloned': percent,
        'amount cloned': get_size_ratio_str(size_c, size_t, human),
        'files cloned': get_num_ratio_str(rentries_c, rentries_t, human),
    }


class CloneInfo:

    def __init__(self, volname):
        self.volname = volname

        self.src_grp_name = None
        self.src_sv_name = None
        self.src_path = None

        self.dst_grp_name = None
        self.dst_sv_name = None
        self.dst_path = None


class CloneProgressReporter:

    def __init__(self, volclient, vol_spec):
        self.vol_spec = vol_spec

        # instance of VolumeClient is needed here so that call to
        # LibCephFS.getxattr() can be made.
        self.volclient = volclient

        # Creating an RTimer instance in advance so that we can check if clone
        # reporting has already been initiated by calling RTimer.is_alive().
        self.update_task = RTimer(1, self._update_progress_bar)

    def initiate_reporting(self):
        if not self.update_task.is_alive():
            self.pev_id = str(uuid4())
            self.update_task.start()

    def _get_clone_dst_info(self, fs_handle, ci, clone_entry,
                            clone_index_path):
        ce_path = os_path_join(clone_index_path, clone_entry)
        # XXX: This may raise ObjectNotFound exception. As soon as cloning is
        # finished, clone entry is deleted by cloner thread. This exception is
        # handled in _get_info_for_all_clones().
        ci.dst_path = fs_handle.readlink(ce_path, PATH_MAX).decode('utf-8')

        ci.dst_grp_name, ci.dst_sv_name = \
            resolve_group_and_subvolume_name(self.vol_spec, ci.dst_path)

    def _get_clone_src_info(self, fs_handle, ci):
        with open_clone_sv_pair_in_vol(
                self.volclient, self.vol_spec, ci.volname, ci.dst_grp_name,
                ci.dst_sv_name) as (dst_sv, src_sv, snap_name):
            ci.src_grp_name = src_sv.group_name
            ci.src_sv_name = src_sv.subvolname
            ci.src_path = src_sv.snapshot_data_path(snap_name)

    def _get_info_for_all_clones(self):
        clones = []

        volnames = list_volumes(self.volclient.mgr)
        for volname in volnames:
            with open_volume_lockless(self.volclient, volname) as fs_handle:
                with open_clone_index(fs_handle, self.vol_spec) as clone_index:
                    clone_index_path = clone_index.path
                    clone_entries = clone_index.list_entries()

            for ce in clone_entries:
                ci = CloneInfo(volname)

                try:
                    self._get_clone_dst_info(fs_handle, ci, ce,
                                             clone_index_path)
                    self._get_clone_src_info(fs_handle, ci)
                    if ci.src_path is None or ci.dst_path is None:
                        continue
                # clone entry went missing, it was removed because cloning has
                # finished.
                except ObjectNotFound:
                    continue

                clones.append(ci)

        return clones

    def _update_progress_bar(self):
        assert self.pev_id is not None
        clone = self._get_info_for_all_clones()
        if not clone:
            self._finish()
            return

        with open_volume_lockless(self.volclient, clone.volname) as fs_handle:
            _, _, percent = get_amount_copied(clone.src_path, clone.dst_path,
                                          fs_handle)

        # progress module takes progress as a fraction between 0.0 to 1.0.
        progress_fraction = percent / 100
        msg = f'Subvolume "{self.dst_svname}" has been {percent}% cloned'

        self.volclient.mgr.remote('progress', 'update', ev_id=self.pev_id,
                           ev_msg=msg, ev_progress=progress_fraction,
                           refs=['mds', 'clone'], add_to_ceph_s=True)

        if progress_fraction == 1.0:
            self._finish()

    def _finish(self):
        assert self.pev_id is not None

        self.volclient.mgr.remote('progress', 'complete', self.pev_id)
        self.pev_id = None

        self.update_task.finished.set()
