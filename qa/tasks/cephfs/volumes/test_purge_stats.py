from logging import getLogger

from teuthology.contextutil import safe_while
from tasks.cephfs.test_volumes import TestVolumesHelper
from tasks.cephfs.volumes.test_clone_stats import ProgressBarHelper


log = getLogger(__name__)


class PurgeProgressBarHelper(ProgressBarHelper):

    def get_purge_pevs_from_ceph_status(self):
        return self.get_certain_pevs_from_ceph_status('purge')

    def _wait_for_purge_progress_bars_to_be_removed(self):
        with safe_while(tries=10, sleep=0.5) as proceed:
            while proceed():
                pevs = self.get_purge_pevs_from_ceph_status()
                if not pevs:
                    break

class TestFsPurgeStatus(TestVolumesHelper):

    def test_with_regular_subvols(self):
        v = self.volname
        sv = self._gen_subvol_name()

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3000, 1)
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume rm {v} {sv}')
        with safe_while(tries=5, sleep=1) as proceed:
            while proceed():
                o = self.get_ceph_cmd_stdout(f'fs purge status {v}')

                try:
                    self.assertIn('ongoing', o['status']['state'])
                except AssertionError:
                    continue

                self.assertIn('subvols', o['status']['progress_report']['amount_purged'])
                self.assertIn('files', o['status']['progress_report']['amount_purged'])
                self.assertIn('size', o['status']['progress_report']['amount_purged'])

                self.assertIn('subvols', o['status']['progress_report']['percentage_purged'])
                self.assertIn('files', o['status']['progress_report']['percentage_purged'])
                self.assertIn('size', o['status']['progress_report']['percentage_purged'])

                self.assertIn('purge_rate', o['status']['progress_report'])

    def test_after_purging_is_complete(self):
        v = self.volname
        sv = self._gen_subvol_name()

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 1, 1)
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume rm {v} {sv}')
        # allow bit of time for purging to finish
        time_sleep(2)

        o = self.get_ceph_cmd_stdout(f'fs purge status {v}')
        self.assertIn('complete', o['status']['state'])


    def test_when_there_were_n_subvols_to_purge(self):
        pass

    def test_with_subvols_with_retained_snapshots(self):
        pass

class TestPurgeProgressBar(PurgeProgressBarHelper):

    # unique string that'll be present in purge progress bar's ID.
    PURGE_PBAR_ID = 'purge'

    def test_with_regular_subvolume(self):
        v = self.volname
        sv = self._gen_subvol_name()

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3000, 1)
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume rm {v} {sv}')
        with safe_while(tries=10, sleep=1) as proceed:
            while proceed():
                pev = self.get_purge_pevs_from_ceph_status()

                if len(pev) < 1:
                   continue
                elif len(pev) > 1:
                    raise RuntimeError(
                        'Output of "ceph status" command contains more than '
                        'one purge progress bar were but it should have only 1 '
                       f'progress bar.\npev -\n{pev}')

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('Purging 1 subvolumes/3001 files, average progress = ', pev_msg)
                break

        self._wait_for_purge_progress_bars_to_be_removed()

    def test_with_retained_snapshots(self):
        v = self.volname
        sv = self._gen_subvol_name()
        ss = self._gen_snap_name()

        self.run_ceph_cmd(f'fs subvolume create {v} {sv} --mode=777')
        size = self._do_subvolume_io(sv, None, None, 3000, 1)
        self.run_ceph_cmd(f'fs subvolume snapshot create {v} {sv} {ss}')
        self.wait_till_rbytes_is_right(v, sv, size)

        self.run_ceph_cmd(f'fs subvolume rm {v} {sv}')
        with safe_while(tries=10, sleep=1) as proceed:
            while proceed():
                pev = self.get_purge_pevs_from_ceph_status()

                if len(pev) < 1:
                   continue
                elif len(pev) > 1:
                    raise RuntimeError(
                        'Output of "ceph status" command contains more than '
                        'one purge progress bar were but it should have only 1 '
                       f'progress bar.\npev -\n{pev}')

                pev_msg = tuple(pev.values())[0]['message']
                self.assertIn('Purging 1 subvolumes/3001 files, average progress = ', pev_msg)
                break

        self._wait_for_purge_progress_bars_to_be_removed()


    def test_disable_purge_progress_bar(self):
        pass

    # create a new class that'll contain only v1 tests and move this test to
    # that class. this allow tracking and enabling and disabling of v1 tests
    # separately.
    def test_with_unupgraded_v1(self):
        pass
