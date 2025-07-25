#!/bin/sh -ex

echo $0
pwd
sudo ls -la ./
echo  $(dirname $0)/../../../../
id
username=$(id -un)
sudo chown -R $username:$username $(dirname $0)/../../../../
sudo chown -R $username:$username ./
ls -la /home/ubuntu/cephtest/mnt.0/client.0/tmp
ls -la /home/ubuntu/cephtest/mnt.0/client.0/
ls -la /home/ubuntu/cephtest/mnt.0/

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_rmtree_no_perm_on_nonroot_dir_supress_errors || echo abcd

pwd
ls -la ./
ls -la /home/ubuntu/cephtest/mnt.0/client.0/tmp/pytest.ini
ls -la /home/ubuntu/cephtest/mnt.0/client.0/tmp
ls -la /home/ubuntu/cephtest/mnt.0/client.0/
ls -la /home/ubuntu/cephtest/mnt.0/
# restoring original owner.
sudo chown -R root:root $(dirname $0)/../../../../
exit 0
