package(default_visibility=[':__subpackages__'])
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='shape',
    srcs=glob(['*.cpp', 'shape/*.cpp']),
    hdrs=glob(['*.h', 'shape/*.h']),
    copts=['-Werror', '-mavx'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':geo_hash', ':geo_hash_util', '//aios/autil:NoCopyable',
        '//aios/autil:log', '//aios/autil:span', '//aios/autil:string_helper',
        '//aios/storage/indexlib/index/common/field_format/attribute:type_info',
        '//aios/storage/indexlib/util:Avl',
        '//aios/storage/indexlib/util:Singleton',
        '//aios/storage/indexlib/index/inverted_index/config:SpatialIndexConfig'
    ]
)
indexlib_cc_library(
    name='geo_hash',
    srcs=['geo_hash/geohash.cpp'],
    hdrs=['geo_hash/geohash.h'],
    copts=['-Werror', '-mavx'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    alwayslink=True
)
indexlib_cc_library(
    name='geo_hash_util',
    srcs=['geo_hash/GeoHashUtil.cpp'],
    hdrs=['geo_hash/GeoHashUtil.h'],
    copts=['-Werror', '-mavx'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':geo_hash', '//aios/autil:NoCopyable', '//aios/autil:log']
)
