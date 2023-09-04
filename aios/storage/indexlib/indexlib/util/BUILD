cc_library(
    name='util_all',
    srcs=glob([]),
    hdrs=glob([]),
    copts=['-Werror'],
    include_prefix='indexlib/util',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':base', '//aios/autil:murmur_hash', '//aios/storage/indexlib/util:all'
    ],
    alwayslink=True
)
cc_library(
    name='base',
    srcs=glob(['*.cpp'], exclude=[]),
    hdrs=glob(['*.h'], exclude=[]),
    copts=['-Werror'],
    include_prefix='indexlib/util',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:murmur_hash',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/util:all'
    ],
    alwayslink=True
)
