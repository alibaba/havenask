package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_memcache_headers',
    hdrs=glob(['*.h']),
    deps=['//aios/autil:log'],
    include_prefix='ha3/util/memcache'
)
cc_library(
    name='ha3_memcache',
    srcs=glob(['*.c', '*.cpp']),
    deps=[
        ':ha3_memcache_headers', '//aios/autil:lock', '//aios/autil:thread',
        '//aios/autil:object_allocator'
    ]
)
