#!/bin/sh -ex

sudo useradd -u 1001 newtmpuser1
type pwd
pwd
pwd
echo $0
sudo ls -la
echo  $(dirname $0)/../../../../
sudo chown -R newtmpuser1:newtmpuser1 $(dirname $0)/../../../../

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_rmtree_no_perm_on_nonroot_dir_supress_errors

sudo chown -R root:root $(dirname $0)/../../../../
sudo userdel newtmpuser1
exit 0
