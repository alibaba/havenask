load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
flatbuffer_library_public(
    name='kv_fbs',
    srcs=['kv_document/kv_message.fbs'],
    outs=['kv_document/kv_message_generated.h'],
    language_flag='-c'
)
cc_library(
    name='kv_fbs_h',
    hdrs=['kv_document/kv_message_generated.h'],
    include_prefix='indexlib/document',
    deps=['@flatbuffers']
)
cc_library(
    name='document',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[':document_parser', ':document_query', ':raw_document']
)
cc_library(
    name='index_locator',
    srcs=['index_locator.cpp'],
    hdrs=['index_locator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/base:Progress',
        '//aios/storage/indexlib/framework:Locator',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc'
    ],
    alwayslink=True
)
cc_library(
    name='locator',
    srcs=['locator.cpp'],
    hdrs=['locator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc'
    ],
    alwayslink=True
)
cc_library(
    name='raw_document',
    srcs=glob([
        '*.cpp', 'extend_document/*.cpp', 'extend_document/tokenize/*.cpp',
        'index_document/normal_document/*.cpp', 'kv_document/*.cpp',
        'kv_document/legacy/*.cpp', 'raw_document/*.cpp'
    ],
              exclude=[
                  'locator.cpp', 'index_locator.cpp', 'document_factory.cpp',
                  'document_factory_wrapper.cpp',
                  'built_in_document_factory.cpp'
              ]),
    hdrs=glob([
        '*.h', 'extend_document/*.h', 'extend_document/tokenize/*.h',
        'index_document/normal_document/*.h', 'kv_document/*.h',
        'kv_document/legacy/*.h', 'raw_document/*.h'
    ],
              exclude=[
                  'locator.h', 'index_locator.h', 'document_factory.h',
                  'document_factory_wrapper.h', 'built_in_document_factory.h',
                  'builtin_parser_init_param.h'
              ]),
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        ':index_locator', ':kv_fbs_h', ':locator', '//aios/autil:hash_map',
        '//aios/autil:object_allocator',
        '//aios/storage/indexlib/document:DocumentInitParam',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document:RawDocumentDefine',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/document/kv:KVDocument',
        '//aios/storage/indexlib/document/normal:AttributeDocument',
        '//aios/storage/indexlib/document/normal:ClassifiedDocument',
        '//aios/storage/indexlib/document/normal:Field',
        '//aios/storage/indexlib/document/normal:GroupFieldFormatter',
        '//aios/storage/indexlib/document/normal:GroupFieldIter',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/document/normal:IndexRawField',
        '//aios/storage/indexlib/document/normal:IndexTokenizeField',
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/document/normal:NullField',
        '//aios/storage/indexlib/document/normal:SearchSummaryDocument',
        '//aios/storage/indexlib/document/normal:Section',
        '//aios/storage/indexlib/document/normal:SerializedSummaryDocument',
        '//aios/storage/indexlib/document/normal:SourceDocument',
        '//aios/storage/indexlib/document/normal:SummaryDocument',
        '//aios/storage/indexlib/document/normal:SummaryGroupFieldIter',
        '//aios/storage/indexlib/document/normal:SummaryGroupFormatter',
        '//aios/storage/indexlib/document/normal:Token',
        '//aios/storage/indexlib/document/normal/tokenize',
        '//aios/storage/indexlib/document/raw_document:DefaultRawDocument',
        '//aios/storage/indexlib/document/raw_document:KeyMapManager',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib:indexlib_plugin',
        '//aios/storage/indexlib/indexlib/common:field_format',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/util:util_all'
    ],
    alwayslink=True
)
cc_library(
    name='document_rewriter',
    srcs=glob(['document_rewriter/*.cpp']),
    hdrs=glob(['document_rewriter/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[':raw_document', '//aios/autil:scope'],
    alwayslink=True
)
cc_library(
    name='document_parser',
    srcs=(
        glob([
            'document_parser/*.cpp', 'document_parser/kv_parser/*.cpp',
            'document_parser/line_data_parser/*.cpp',
            'document_parser/normal_parser/*.cpp'
        ]) + [
            'document_factory.cpp', 'document_factory_wrapper.cpp',
            'built_in_document_factory.cpp'
        ]
    ),
    hdrs=(
        glob([
            'document_parser/*.h', 'document_parser/kv_parser/*.h',
            'document_parser/line_data_parser/*.h',
            'document_parser/normal_parser/*.h'
        ]) + [
            'document_factory.h', 'document_factory_wrapper.h',
            'built_in_document_factory.h', 'builtin_parser_init_param.h'
        ]
    ),
    copts=['-Werror'],
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':document_rewriter', ':raw_document', '//aios/autil:scope',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/indexlib:indexlib_plugin',
        '//aios/storage/indexlib/indexlib/common:multi_region_rehasher'
    ],
    alwayslink=True
)
cc_library(
    name='document_query',
    srcs=glob(['query/*.cpp']),
    hdrs=glob(['query/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[':raw_document', '//aios/autil:url_util'],
    alwayslink=True
)
