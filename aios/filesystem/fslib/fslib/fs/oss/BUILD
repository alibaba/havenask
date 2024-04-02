package(default_visibility=['//visibility:public'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_oss',
    exports_filter=[],
    preloaded_deps=[
        '//aios/autil:lock', '//aios/filesystem/fslib:fslib-framework'
    ],
    roots=[':oss'],
    static_deps=[
        '@oss-sdk//:oss-c-sdk', '@oss-sdk//:apr-1', '@oss-sdk//:apr',
        '@oss-sdk//:mxml', '@oss-sdk//:oss-shared', '@curl//:curl',
        '@boringssl//:ssl', '@boringssl//:crypto', '@zlib_archive//:zlib'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='oss',
    srcs=(
        glob([
            'ConvertUTF.cpp', 'OssFile.cpp', 'OssFileSystem.cpp',
            'OssFileSystemCreator.cpp'
        ]) + ['havenask/OssConfig.cpp']
    ),
    hdrs=(
        glob([
            'ConvertUTF.h', 'OssFile.h', 'OssFileSystem.h',
            'OssFileSystemCreator.h'
        ]) + ['havenask/OssConfig.h']
    ),
    copts=['-Werror', '-fno-access-control'],
    include_prefix='fslib/fs/oss',
    linkstatic=1,
    deps=[
        '//aios/autil:lock', '//aios/filesystem/fslib:fslib-framework',
        '@boringssl//:crypto', '@boringssl//:ssl', '@curl', '@oss-sdk//:apr',
        '@oss-sdk//:apr-1', '@oss-sdk//:mxml', '@oss-sdk//:oss-c-sdk',
        '@zlib_archive//:zlib'
    ],
    alwayslink=1
)
