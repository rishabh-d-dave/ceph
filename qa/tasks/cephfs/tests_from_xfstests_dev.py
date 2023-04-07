from logging import getLogger

from tasks.cephfs.xfstests_dev import XFSTestsDev


log = getLogger(__name__)


class TestXFSTestsDev(XFSTestsDev):

    def test_generic(self):
        self.run_generic_tests()

    def test_quick_auto_generic_ceph(self):
        self.run_test('-g quick,auto; ./check -x quick,auto generic/*; ./check -x quick,auto ceph/*')
