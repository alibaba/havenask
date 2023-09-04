load('//bazel:defs.bzl', 'rpm_library')
hdrs = ['KeyCenterClient.h']
shared_lib = 'libKeyCenterClient.so'
rpm_library(
    name='keycenter4c',
    hdrs=hdrs,
    shared_lib=shared_lib,
    include_path='usr/local/include',
    lib_path='usr/local/lib64'
)
