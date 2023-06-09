load('//bazel:defs.bzl', 'rpm_library')
hdrs = ['lz4.h', 'lz4frame.h', 'lz4hc.h']
static_lib = 'liblz4.a'
static_lib = None
shared_lib = 'liblz4.so'
rpm_library(
    name='lz4',
    rpms=['@lz4//file:file'],
    hdrs=hdrs,
    static_lib=static_lib,
    shared_lib=shared_lib,
    include_path='usr/local/include',
    lib_path='usr/local/lib64'
)
