load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':BlockArrayReader']
)
strict_cc_library(
    name='writer',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':BlockArrayWriter']
)
strict_cc_library(
    name='BlockArrayCacheDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
strict_cc_library(
    name='BlockArrayCompressDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
strict_cc_library(
    name='BlockArrayDataAccessor',
    srcs=[],
    deps=[':KeyValueItem', '//aios/storage/indexlib/index/common:error_code']
)
strict_cc_library(name='BlockArrayIterator', srcs=[])
strict_cc_library(
    name='BlockArrayMemoryDataAccessor',
    srcs=[],
    deps=[':BlockArrayDataAccessor']
)
strict_cc_library(
    name='BlockArrayReader',
    srcs=[],
    deps=[
        ':BlockArrayCacheDataAccessor', ':BlockArrayCompressDataAccessor',
        ':BlockArrayIterator', ':BlockArrayMemoryDataAccessor'
    ]
)
strict_cc_library(name='BlockArrayWriter', srcs=[], deps=[':KeyValueItem'])
strict_cc_library(
    name='KeyValueItem',
    srcs=[],
    deps=['//aios/autil:long_hash_value', '//aios/storage/indexlib/base:Types']
)
