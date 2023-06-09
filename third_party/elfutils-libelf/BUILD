load('//bazel:defs.bzl', 'rpm_library')
rpm_library(
    name='elfutils-libelf',
    rpms=[
        '@elfutils-libelf-devel//file:file', '@elfutils-libelf-lib//file:file'
    ],
    hdrs=['gelf.h', 'libelf.h'],
    include_path='usr/include',
    lib_path='usr/lib64',
    shared_lib='libelf.so',
    include_prefix=''
)
