#!/bin/sh -ex

echo $0
pwd
sudo ls -la
echo  $(dirname $0)/../../../../
echo $UID
id -u
id
findmnt | grep ceph
cephfs_name=$(ceph fs ls | awk '{print $2}' | tr -d ,)
#sudo chown -R ubuntu:ubuntu $(dirname $0)/../../../../
sudo useradd -m tempuser1
uid_=$(id -u tempuser1)
gid_=$(id -g tempuser1)
ceph auth add client.x mon "allow rw fsname=$cephfs_name" osd "allow rw tag cephfs data=*" mds "allow rw fsname=$cephfs_name uid=$uid_ gids=$gid_"
sudo chown -R tempuser1:tempuser1 $(dirname $0)/../../../../
sudo chown -R tempuser1:tempuser1 ./
ls -la /home/ubuntu/cephtest/mnt.0/client.0/tmp
ls -la /home/ubuntu/cephtest/mnt.0/client.0/
ls -la /home/ubuntu/cephtest/mnt.0/

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
sudo -u tempuser1 python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_rmtree_no_perm_on_nonroot_dir_supress_errors

# restoring original owner.
sudo chown -R root:root $(dirname $0)/../../../../
sudo userdel -r tempuser1
exit 0
