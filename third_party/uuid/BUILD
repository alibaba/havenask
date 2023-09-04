load('//bazel:defs.bzl', 'rpm_library')
rpm_library(
    name='uuid',
    rpms=['@uuid//file:file', '@uuid-devel//file:file'],
    hdrs=['uuid/uuid.h'],
    shared_lib='libuuid.so',
    include_path='usr/include',
    lib_path='usr/lib64'
)
