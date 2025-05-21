from json import loads
from time import sleep as time_sleep
from logging import getLogger

from tasks.cephfs.test_volumes import TestVolumesHelper

from teuthology.contextutil import safe_while


log = getLogger(__name__)

class RsizeDoesntMatch(Exception):

    def __init__(self, msg):
        self.msg = msg


class ProgressBarHelper(TestVolumesHelper):

    # XXX: it is important to wait for rbytes value to catch up to actual size of
    # subvolume so that progress bar shows sensible amount of progress
    def wait_till_rbytes_is_right(self, v_name, sv_name, exp_size,
                                  grp_name=None, sleep=2, max_count=60):
        getpath_cmd = f'fs subvolume getpath {v_name} {sv_name}'
        if grp_name:
            getpath_cmd += f' {grp_name}'
        sv_path = self.get_ceph_cmd_stdout(getpath_cmd)
        sv_path = sv_path[1:]

        for i in range(max_count):
            r_size = self.mount_a.get_shell_stdout(
                f'getfattr -n ceph.dir.rbytes {sv_path}').split('rbytes=')[1]
            r_size = int(r_size.replace('"', '').replace('"', ''))
            log.info(f'r_size = {r_size} exp_size = {exp_size}')
            if exp_size == r_size:
                break

            time_sleep(sleep)
        else:
            msg = ('size reported by rstat is not the expected size.\n'
                   f'expected size = {exp_size}\n'
                   f'size reported by rstat = {r_size}')
            raise RsizeDoesntMatch(msg)

    def filter_in_only_certain_pevs(self, progress_events, pev_id_substr):
        '''
        Progress events dictionary in output of "ceph status --format json"
        has the progress bars and message associated with each progress bar.
        Sometimes during testing of clone progress bars, and sometimes
        otherwise too, an extra progress bar is seen with message "Global
        Recovery Event". This extra progress bar interferes with testing of
        progress bars for cloning.

        This helper methods goes through this dictionary and picks only
        (filters in) clone events.

        pev_id_substr should contain a string that'll be used to filter
        progress events obtained from output of "ceph status" command. Progress
        event ID will checked for the presence of string referenced by
        pev_id_substr. If found, it is filtered-in and returned, else
        filtered-out.
        '''
        if progress_events == {}:
            return {}

        if not isinstance(progress_events, dict):
            raise RuntimeError('variable "progress_events" should be '
                               'dictionary, regardless of whether with or '
                               'without any members')

        selected_pevs = {}
        for k, v in progress_events.items():
            if pev_id_substr in k:
                selected_pevs[k] = v

        return selected_pevs

    def get_certain_pevs_from_ceph_status(self, pev_id_substr):
        '''
        For understandig pev_id_substr, see docstring of
        filter_in_only_certain_pevs().
        '''
        o = self.get_ceph_cmd_stdout('status --format json-pretty')
        o = loads(o)
        try:
            pevs = o['progress_events'] # pevs = progress events
        except KeyError:
            return []

        pevs = self.filter_in_only_certain_pevs(pevs, pev_id_substr)
        return pevs

    def wait_for_both_progress_bars_to_appear(self, pev_id_substr, sleep=1,
                                              iters=20):
        pevs = []
        msg = (f'Waited for {iters*sleep} seconds but couldn\'t 2 progress '
                'bars in output of "ceph status" command.')
        with safe_while(tries=iters, sleep=sleep, action=msg) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout('status --format json-pretty')
                o = loads(o)
                pevs = o['progress_events']
                pevs = self.filter_in_only_certain_pevs(pevs, pev_id_substr)
                if len(pevs) == 2:
                    v = tuple(pevs.values())
                    if 'ongoing+pending' in v[1]['message']:
                        self.assertIn('ongoing', v[0]['message'])
                    else:
                        self.assertIn('ongoing', v[1]['message'])
                        self.assertIn('ongoing+pending', v[0]['message'])
                    break
