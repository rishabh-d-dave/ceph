#!/bin/sh -ex

sudo useradd -u 1001 newtmpuser1

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
sudo python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_rmtree_no_perm_on_nonroot_dir_supress_errors

sudo userdel newtmpuser1
exit 0
