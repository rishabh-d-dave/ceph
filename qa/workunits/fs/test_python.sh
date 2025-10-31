#!/bin/sh -ex

old_interval=$(ceph config get mds mds_dirstat_min_interval)
ceph config set mds mds_dirstat_min_interval 1200

# Running as root because the filesystem root directory will be
# owned by uid 0, and that's where we're writing.
python3 -m pytest -v $(dirname $0)/../../../src/test/pybind/test_cephfs.py -k test_create_and_rm_2000_subdir_levels_close_v4

set +e
ceph tell mds.0 perf dump
ceph tell mds.0 perf dump
set -e

ceph config set mds mds_dirstat_min_interval $old_interval

exit 0
