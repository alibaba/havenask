load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='IDocumentRewriter', srcs=[], deps=['//aios/autil:NoCopyable']
)
strict_cc_library(
    name='DocumentRewriteChain',
    deps=[
        ':IDocumentRewriter', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(
    name='DocumentInfoToAttributeRewriter',
    deps=[
        ':IDocumentRewriter', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor'
    ]
)
strict_cc_library(
    name='TTLSetter',
    deps=[
        ':IDocumentRewriter', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor'
    ]
)
