load('//bazel:defs.bzl', 'rpm_library')
hdrs = [
    'adapters/ae.h', 'adapters/glib.h', 'adapters/ivykis.h', 'adapters/libev.h',
    'adapters/libevent.h', 'adapters/libuv.h', 'adapters/macosx.h',
    'adapters/qt.h', 'async.h', 'hiredis.h', 'read.h', 'sds.h'
]
static_lib = 'libhiredis.a'
shared_lib = 'libhiredis.so'
rpm_library(
    name='hiredis',
    hdrs=hdrs,
    static_lib=static_lib,
    shared_lib=shared_lib,
    include_path='usr/local/include/hiredis',
    lib_path='usr/local/lib',
    include_prefix='hiredis'
)
