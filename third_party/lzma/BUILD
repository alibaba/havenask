load('//bazel:defs.bzl', 'rpm_library')
rpm_library(
    name='lzma',
    rpms=['@xz-devel//file:file', '@xz-libs//file:file'],
    hdrs=[
        'lzma.h', 'lzma/base.h', 'lzma/bcj.h', 'lzma/block.h', 'lzma/check.h',
        'lzma/container.h', 'lzma/delta.h', 'lzma/filter.h', 'lzma/hardware.h',
        'lzma/index.h', 'lzma/index_hash.h', 'lzma/lzma12.h',
        'lzma/stream_flags.h', 'lzma/version.h', 'lzma/vli.h'
    ],
    shared_lib='liblzma.so',
    include_path='usr/include',
    lib_path='usr/lib64'
)
