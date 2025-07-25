#!/bin/sh -ex

username=$(id -un)
sudo chown -R $username:$username $(dirname $0)/../../../../

python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py

exit 0
