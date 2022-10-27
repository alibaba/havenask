# -*- mode: python -*- #
####################################################################
# You set build options for "ascons" in this file.                 #
# Invoke "scons -h" to see all available options.                  #
#                                                                  #
# ascons' wiki:                                                    #
# http://grd.alibaba-inc.com/projects/heavenasks/wiki/ascons_usage #
####################################################################

install_root = '%{install_root}'
install_prefix = '/usr/local'
depend_prefix = [install_root + install_prefix, install_root + '/usr/',
                 '/ha3_depends/usr', '/ha3_depends/usr/local']

#paths of dependent xxx files which are not in %depend_prefix%/xxx
#can be given by the follow options. (xxx = include or lib)
#depend_includedir = []
depend_libdir = ['/opt/taobao/java/jre/lib/amd64/server/']

heapchecktype = 'none'
mode = 'release'
enable_strip = False
JAVA_SDK_PATH = '/opt/taobao/java/'
system_includedir = ['/ha3_depends/usr/local/include', '/ha3_depends/usr/include',
                     install_root + '/usr/local/include', install_root + '/usr/include']
valgrind_check = 'no'
indexlib_profile = 'none'
indexlib_perftest = 'false'
default_disable_package_file = 'false'
use_hdfs = 'no'
use_pangu = 'no'
gtest_filter = '-*PerfTest.*:*.*LongCaseTest*:ExpackPostingMergerTest*:IndexPartitionMergerInteTest*:MultiPartitionMergerTest*:NumberIndexReaderTest*:OnDiskIndexIteratorTest*:OperationReplayerTest*:PackPostingMergerTest*:TextIndexReaderTest*:'
gtest_color = 'yes'
build_signature = 'yes'
enable_internal_executor = 'no'
