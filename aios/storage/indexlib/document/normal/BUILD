load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='convertor',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':TokenizeDocumentConvertor']
)
indexlib_cc_library(
    name='extend_document',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':NormalExtendDocument']
)
indexlib_cc_library(
    name='NormalDocumentParser',
    deps=[
        ':NormalExtendDocument', ':SingleDocumentParser',
        ':TokenizeDocumentConvertor',
        '//aios/storage/indexlib/document:BuiltinParserInitParam',
        '//aios/storage/indexlib/document/document_rewriter:IDocumentRewriter'
    ]
)
indexlib_cc_library(
    name='TokenizeDocumentConvertor',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/analyzer',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document/normal/tokenize'
    ]
)
indexlib_cc_library(
    name='Field',
    deps=[
        '//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:Exception'
    ]
)
indexlib_cc_library(
    name='IndexDocument',
    deps=[
        ':IndexRawField', ':IndexTokenizeField', ':ModifiedTokens',
        ':NullField', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/common:constants',
        '//aios/storage/indexlib/index/inverted_index/config:PayloadConfig',
        '//aios/storage/indexlib/util:ErrorLogCollector',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='IndexRawField', deps=[':Field', '//aios/autil:string_type']
)
indexlib_cc_library(name='IndexTokenizeField', deps=[':Field', ':Section'])
indexlib_cc_library(
    name='ModifiedTokens',
    deps=[
        ':Token', '//aios/autil:data_buffer', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(name='NullField', deps=[':Field'])
indexlib_cc_library(
    name='Section',
    deps=[
        ':Token', '//aios/autil:dynamic_buf',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/util:PoolUtil'
    ]
)
indexlib_cc_library(
    name='SummaryDocument',
    deps=[
        '//aios/autil:data_buffer', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='Token',
    deps=[
        '//aios/autil:data_buffer', '//aios/autil:hash_base',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
indexlib_cc_library(
    name='AttributeDocument',
    srcs=[],
    deps=[
        ':SummaryDocument', '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/index/attribute:constants'
    ]
)
indexlib_cc_library(
    name='SerializedSummaryDocument',
    deps=['//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants']
)
indexlib_cc_library(
    name='SerializedSourceDocument',
    deps=[
        '//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/source:constants'
    ]
)
indexlib_cc_library(
    name='SearchSummaryDocument',
    deps=['//aios/autil:mem_pool_base', '//aios/autil:string_type']
)
indexlib_cc_library(
    name='GroupFieldFormatter',
    deps=[
        ':GroupFieldIter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
indexlib_cc_library(
    name='GroupFieldIter', srcs=[], deps=['//aios/autil:string_type']
)
indexlib_cc_library(
    name='SummaryGroupFieldIter',
    deps=[
        ':GroupFieldIter', ':SummaryDocument',
        '//aios/storage/indexlib/index/summary:config'
    ]
)
indexlib_cc_library(
    name='SummaryGroupFormatter',
    deps=[
        ':GroupFieldFormatter', ':SearchSummaryDocument',
        ':SummaryGroupFieldIter',
        '//aios/storage/indexlib/index/summary:constants'
    ]
)
indexlib_cc_library(
    name='SummaryFormatter',
    deps=[
        ':SerializedSummaryDocument', ':SummaryDocument',
        ':SummaryGroupFormatter', '//aios/storage/indexlib/index/summary:config'
    ]
)
indexlib_cc_library(
    name='NormalDocument',
    deps=[
        ':AttributeDocument', ':IndexDocument', ':SerializedSourceDocument',
        ':SerializedSummaryDocument', '//aios/autil:env_util',
        '//aios/storage/indexlib/document:interface'
    ]
)
indexlib_cc_library(
    name='AttributeDocumentFieldExtractor',
    deps=[
        ':AttributeDocument', ':SerializedSummaryDocument', '//aios/autil:log',
        '//aios/autil:mem_pool_base', '//aios/autil:span',
        '//aios/storage/indexlib/config:ITabletSchema',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/attribute/config',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/util:ErrorLogCollector'
    ]
)
indexlib_cc_library(
    name='ClassifiedDocument',
    deps=[
        ':AttributeDocument', ':IndexDocument', ':SerializedSummaryDocument',
        ':SourceDocument', ':SummaryDocument', ':SummaryFormatter',
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/document:RawDocument'
    ]
)
indexlib_cc_library(
    name='NormalExtendDocument',
    srcs=[],
    deps=[
        ':ClassifiedDocument',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document/normal/tokenize:TokenizeDocument'
    ]
)
indexlib_cc_library(
    name='NullFieldAppender',
    deps=[
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/document:RawDocument'
    ]
)
indexlib_cc_library(
    name='ExtendDocFieldsConvertor',
    deps=[
        ':NormalExtendDocument', '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/range:RangeFieldEncoder',
        '//aios/storage/indexlib/index/inverted_index/config',
        '//aios/storage/indexlib/index/statistics_term:StatisticsTermIndexConfig',
        '//aios/storage/indexlib/util:DocTracer'
    ]
)
indexlib_cc_library(
    name='SingleDocumentParser',
    deps=[
        ':ExtendDocFieldsConvertor', ':NormalDocument', ':NormalExtendDocument',
        ':NullFieldAppender', '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal/rewriter:PackAttributeAppender',
        '//aios/storage/indexlib/document/normal/rewriter:SectionAttributeAppender'
    ]
)
indexlib_cc_library(
    name='NormalDocumentFactory',
    deps=[
        ':NormalDocumentParser',
        '//aios/storage/indexlib/document:DocumentFactoryAdapter'
    ]
)
indexlib_cc_library(
    name='SourceDocument',
    deps=[
        ':GroupFieldIter', '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/index/source:constants'
    ]
)
