#!/bin/sh -ex

username=$(id -un)
sudo chown -R $username:$username $(dirname $0)/../../../../
sudo chown -R $username:$username ./

python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py

# restoring original owner.
sudo chown -R root:root $(dirname $0)/../../../../
sudo chown -R root:root ./
exit 0
