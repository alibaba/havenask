load('//bazel:defs.bzl', 'cc_proto')
load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='wal',
    srcs=[],
    hdrs=[],
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage/indexlib/file_system:__subpackages__'
    ],
    deps=[':Wal']
)
strict_cc_library(
    name='Wal',
    deps=[
        ':wal_proto_cc_proto', '//aios/autil:crc32c',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
cc_proto(
    name='wal_proto',
    srcs=['Wal.proto'],
    import_prefix='indexlib/file_system/wal',
    visibility=[':__subpackages__'],
    deps=[]
)
