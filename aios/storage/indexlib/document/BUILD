load('//aios/storage:defs.bzl', 'strict_cc_library')
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'IDocument.h', 'IDocumentBatch.h', 'IDocumentFactory.h',
        'IDocumentIterator.h', 'IDocumentParser.h', 'IRawDocumentParser.h'
    ],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log', '//aios/autil:span',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:Locator'
    ]
)
strict_cc_library(
    name='param',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':BuiltinParserInitParam']
)
strict_cc_library(
    name='document',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':DocumentBatch', '//aios/storage/indexlib/document/kv:kv_document']
)
strict_cc_library(
    name='DocumentFactoryAdapter',
    deps=[
        ':ExtendDocument', ':interface', '//aios/autil:env_util',
        '//aios/storage/indexlib/document/raw_document:DefaultRawDocument'
    ]
)
strict_cc_library(
    name='DocumentBatch',
    srcs=[],
    hdrs=['DocumentBatch.h', 'DocumentIterator.h'],
    deps=[':interface', '//aios/autil:log']
)
strict_cc_library(name='DocumentInitParam')
strict_cc_library(
    name='BuiltinParserInitParam', srcs=[], deps=[':DocumentInitParam']
)
strict_cc_library(name='RawDocumentDefine', srcs=[])
strict_cc_library(
    name='RawDocument',
    deps=[
        ':RawDocumentDefine', ':interface', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(
    name='RawDocFieldIterator',
    srcs=[],
    deps=['//aios/autil:log', '//aios/autil:span']
)
strict_cc_library(name='ExtendDocument', deps=[])
strict_cc_library(
    name='IIndexFields',
    srcs=[],
    deps=['//aios/autil:NoCopyable', '//aios/autil:data_buffer']
)
strict_cc_library(
    name='IIndexFieldsParser',
    srcs=[],
    deps=['//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Status']
)
flatbuffer_library_public(
    name='plain_document_fbs',
    srcs=['PlainDocumentMessage.fbs'],
    outs=['PlainDocumentMessage_generated.h'],
    language_flag='-c'
)
strict_cc_library(
    name='plain_document_fbs_h',
    srcs=[],
    hdrs=['PlainDocumentMessage_generated.h'],
    include_prefix='indexlib/document',
    deps=['@flatbuffers']
)
strict_cc_library(
    name='PlainDocument',
    deps=[
        ':IIndexFields', ':IIndexFieldsParser', ':interface',
        ':plain_document_fbs_h', '//aios/autil:log',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
