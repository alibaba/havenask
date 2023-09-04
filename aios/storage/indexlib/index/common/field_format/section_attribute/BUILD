load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='SectionMeta',
    srcs=[],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(
    name='MultiSectionMeta',
    srcs=[],
    deps=[
        ':SectionAttributeFormatter', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='InDocSectionMeta',
    srcs=[],
    deps=[
        ':SectionAttributeFormatter', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(
    name='SectionAttributeEncoder',
    deps=[
        ':SectionMeta', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/common/numeric_compress:S16Compressor'
    ]
)
strict_cc_library(
    name='SectionAttributeFormatter',
    deps=[
        ':SectionAttributeEncoder', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/inverted_index/config:SectionAttributeConfig',
        '//aios/storage/indexlib/util:numeric_util'
    ]
)
