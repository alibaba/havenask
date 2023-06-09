load('//bazel:defs.bzl', 'rpm_library')
hdrs = [
    'proto.h', 'recordio.h', 'zookeeper.h', 'zookeeper.jute.h',
    'zookeeper_log.h', 'zookeeper_version.h'
]
static_lib = 'libzookeeper_mt.a'
rpm_library(
    name='zookeeper',
    rpms=['@zookeeper-package//file:file'],
    hdrs=hdrs,
    static_lib=static_lib,
    include_path='usr/local/include/zookeeper',
    lib_path='usr/local/lib',
    include_prefix='zookeeper'
)
