load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='DateFieldEncoder',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':DateTerm', '//aios/autil:string_helper',
        '//aios/storage/indexlib/index/inverted_index:config'
    ]
)
indexlib_cc_library(
    name='DateQueryParser',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DateTerm', '//aios/storage/indexlib/index/common:NumberTerm']
)
indexlib_cc_library(
    name='DateTerm',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/inverted_index/config:DateLevelFormat',
        '//aios/storage/indexlib/util:TimestampUtil'
    ]
)
