load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='convertor',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':TokenizeDocumentConvertor']
)
strict_cc_library(
    name='extend_document',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':NormalExtendDocument']
)
strict_cc_library(
    name='NormalDocumentParser',
    deps=[
        ':NormalExtendDocument', ':SingleDocumentParser',
        ':TokenizeDocumentConvertor',
        '//aios/storage/indexlib/document:BuiltinParserInitParam',
        '//aios/storage/indexlib/document/document_rewriter:IDocumentRewriter'
    ]
)
strict_cc_library(
    name='TokenizeDocumentConvertor',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/analyzer',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document/normal/tokenize',
        '//aios/storage/indexlib/index/ann:constants',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/index/primary_key:constants',
        '//aios/storage/indexlib/index/summary:constants',
        '//aios/storage/indexlib/util:Algorithm'
    ]
)
strict_cc_library(
    name='Field',
    deps=[
        '//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='IndexRawField', deps=[':Field', '//aios/autil:string_type']
)
strict_cc_library(name='IndexTokenizeField', deps=[':Field', ':Section'])
strict_cc_library(
    name='ModifiedTokens',
    deps=[
        ':Token', '//aios/autil:data_buffer', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(name='NullField', deps=[':Field'])
strict_cc_library(
    name='Section',
    deps=[
        ':Token', '//aios/autil:dynamic_buf',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/util:PoolUtil'
    ]
)
strict_cc_library(
    name='SummaryDocument',
    deps=[
        '//aios/autil:data_buffer', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(
    name='Token',
    deps=[
        '//aios/autil:data_buffer', '//aios/autil:hash_base',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='AttributeDocument',
    srcs=[],
    deps=[
        ':SummaryDocument', '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/index/attribute:constants'
    ]
)
strict_cc_library(
    name='FieldMetaDocument',
    deps=[':SummaryDocument', '//aios/storage/indexlib/base:Define']
)
strict_cc_library(
    name='SerializedSummaryDocument',
    deps=['//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants']
)
strict_cc_library(
    name='SerializedSourceDocument',
    deps=[
        '//aios/autil:data_buffer', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/source:constants'
    ]
)
strict_cc_library(
    name='SearchSummaryDocument',
    deps=['//aios/autil:mem_pool_base', '//aios/autil:string_type']
)
strict_cc_library(
    name='GroupFieldFormatter',
    deps=[
        ':GroupFieldIter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
strict_cc_library(
    name='GroupFieldIter', srcs=[], deps=['//aios/autil:string_type']
)
strict_cc_library(
    name='SummaryGroupFieldIter',
    deps=[
        ':GroupFieldIter', ':SummaryDocument',
        '//aios/storage/indexlib/index/summary:config'
    ]
)
strict_cc_library(
    name='SummaryGroupFormatter',
    deps=[
        ':GroupFieldFormatter', ':SearchSummaryDocument',
        ':SummaryGroupFieldIter',
        '//aios/storage/indexlib/index/summary:constants'
    ]
)
strict_cc_library(
    name='SummaryFormatter',
    deps=[
        ':SerializedSummaryDocument', ':SummaryDocument',
        ':SummaryGroupFormatter', '//aios/storage/indexlib/index/summary:config'
    ]
)
strict_cc_library(
    name='SourceFormatter',
    deps=[
        ':GroupFieldFormatter', ':SerializedSourceDocument', ':SourceDocument',
        '//aios/storage/indexlib/index/source:config',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(
    name='NormalDocument',
    deps=[
        ':AttributeDocument', ':FieldMetaDocument', ':IndexDocument',
        ':SerializedSourceDocument', ':SerializedSummaryDocument',
        '//aios/autil:env_util', '//aios/storage/indexlib/document:interface'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='ClassifiedDocument',
    deps=[
        ':AttributeDocument', ':FieldMetaDocument', ':IndexDocument',
        ':SerializedSummaryDocument', ':SourceDocument', ':SourceFormatter',
        ':SummaryDocument', ':SummaryFormatter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/document:RawDocument'
    ]
)
strict_cc_library(
    name='NormalExtendDocument',
    srcs=[],
    deps=[
        ':ClassifiedDocument',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document/normal/tokenize:TokenizeDocument'
    ]
)
strict_cc_library(
    name='NullFieldAppender',
    deps=[
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/document:RawDocument'
    ]
)
strict_cc_library(
    name='ExtendDocFieldsConvertor',
    deps=[
        ':NormalExtendDocument', '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/field_meta:field_meta_convertor',
        '//aios/storage/indexlib/index/common/field_format/range:RangeFieldEncoder',
        '//aios/storage/indexlib/index/inverted_index/config',
        '//aios/storage/indexlib/index/statistics_term:StatisticsTermIndexConfig',
        '//aios/storage/indexlib/util:DocTracer'
    ]
)
strict_cc_library(
    name='SingleDocumentParser',
    deps=[
        ':ExtendDocFieldsConvertor', ':NormalDocument', ':NormalExtendDocument',
        ':NullFieldAppender', '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal/rewriter:PackAttributeAppender',
        '//aios/storage/indexlib/document/normal/rewriter:SectionAttributeAppender',
        '//aios/storage/indexlib/table/normal_table:Common'
    ]
)
strict_cc_library(
    name='NormalDocumentFactory',
    deps=[
        ':NormalDocumentParser',
        '//aios/storage/indexlib/document:DocumentFactoryAdapter'
    ]
)
strict_cc_library(
    name='SourceDocument',
    deps=[
        ':GroupFieldIter', '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/index/source:constants'
    ]
)
