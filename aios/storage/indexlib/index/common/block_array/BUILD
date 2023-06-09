load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':BlockArrayReader']
)
indexlib_cc_library(
    name='writer',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':BlockArrayWriter']
)
indexlib_cc_library(
    name='BlockArrayCacheDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
indexlib_cc_library(
    name='BlockArrayCompressDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
indexlib_cc_library(
    name='BlockArrayDataAccessor',
    srcs=[],
    deps=[':KeyValueItem', '//aios/storage/indexlib/index/common:error_code']
)
indexlib_cc_library(name='BlockArrayIterator', srcs=[])
indexlib_cc_library(
    name='BlockArrayMemoryDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
indexlib_cc_library(
    name='BlockArrayReader',
    srcs=[],
    deps=[
        ':BlockArrayCacheDataAccessor', ':BlockArrayCompressDataAccessor',
        ':BlockArrayIterator', ':BlockArrayMemoryDataAccessor'
    ]
)
indexlib_cc_library(name='BlockArrayWriter', srcs=[], deps=[':KeyValueItem'])
indexlib_cc_library(
    name='KeyValueItem',
    srcs=[],
    deps=['//aios/autil:long_hash_value', '//aios/storage/indexlib/base:Types']
)
