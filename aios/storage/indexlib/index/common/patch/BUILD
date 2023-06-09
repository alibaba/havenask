load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index:__subpackages__',
        '//aios/storage/indexlib/table:__subpackages__'
    ]
)
indexlib_cc_library(
    name='PatchFileFinder',
    deps=[
        ':PatchFileInfos', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
indexlib_cc_library(
    name='PatchFileInfo',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='PatchFileInfos',
    deps=[
        ':PatchFileInfo', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='PatchFileMerger',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchMerger'
    ]
)
indexlib_cc_library(
    name='PatchMerger', deps=[':PatchFileInfos', '//aios/autil:NoCopyable']
)
indexlib_cc_library(
    name='PatchWriter',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/common:DictKeyInfo'
    ]
)
indexlib_cc_library(
    name='SrcDestPatchFileFinder',
    deps=[
        ':PatchFileFinder', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system'
    ]
)
