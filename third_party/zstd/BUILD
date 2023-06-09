load('//bazel:defs.bzl', 'rpm_library')
hdrs = ['zdict.h', 'zstd.h', 'zstd_errors.h']
static_lib = None
shared_lib = 'libzstd.so'
rpm_library(
    name='zstd',
    hdrs=hdrs,
    static_lib=static_lib,
    shared_lib=shared_lib,
    include_path='usr/local/include',
    lib_path='usr/local/lib64'
)
