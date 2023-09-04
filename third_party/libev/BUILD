load('//bazel:defs.bzl', 'rpm_library')
rpm_library(
    name='libev',
    rpms=['@libev//file:file', '@libev-devel//file:file'],
    lib_path='usr/lib64',
    include_path='usr/include',
    shared_lib='libev.so',
    hdrs=['ev.h', 'ev++.h'],
    alwayslink=1
)
