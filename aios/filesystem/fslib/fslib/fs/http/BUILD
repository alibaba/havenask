package(default_visibility=['//visibility:private'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_http',
    exports_filter=[],
    preloaded_deps=[
        '//aios/autil:lock', '//aios/filesystem/fslib:fslib-framework',
        '@zlib_archive//:zlib'
    ],
    roots=[':http'],
    static_deps=['@boringssl//:ssl', '@boringssl//:crypto', '@curl//:curl'],
    visibility=['//visibility:public']
)
cc_library(
    name='http',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='fslib/fs/http',
    deps=[
        '//aios/autil:lock', '//aios/filesystem/fslib:fslib-framework',
        '@boringssl//:crypto', '@boringssl//:ssl', '@curl',
        '@zlib_archive//:zlib'
    ],
    alwayslink=True
)
