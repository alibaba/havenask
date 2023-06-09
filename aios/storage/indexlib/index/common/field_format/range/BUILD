package(default_visibility=[':__subpackages__'])
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='RangeFieldEncoder',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:NumberTerm',
        '//aios/storage/indexlib/index/inverted_index:config'
    ]
)
