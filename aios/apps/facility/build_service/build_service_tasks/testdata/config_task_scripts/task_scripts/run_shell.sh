#!/bin/sh

# test arguments
echo "test arguments: $*"
echo 'BS_TASK_PARTITION_ID = '$BS_TASK_PARTITION_ID
echo 'BS_TASK_INSTANCE_ID = '$BS_TASK_INSTANCE_ID
# test source file
. config/var.sh
if [ $a == "10" ] && [ $b == "20" ]; then
    echo "test source file: ok"
fi

# test run binary from _external/bin 
#bin=fs_util
#$bin ls /
#if [ $? -eq 0 ]; then
#    echo "test binary: fs_util ok"
#fi

# test run binary from script root
test_link_bin
if [ $? -eq 0 ]; then
    echo "test third-part binary: test_link ok"
fi


