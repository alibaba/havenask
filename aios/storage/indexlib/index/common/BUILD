load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='numeric_compress',
    srcs=[],
    hdrs=[],
    deps=[
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressDefine',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressReader',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressSessionReader',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressTraits',
        '//aios/storage/indexlib/index/common/numeric_compress:EquivalentCompressWriter',
        '//aios/storage/indexlib/index/common/numeric_compress:S16Compressor'
    ]
)
strict_cc_library(
    name='data_structure',
    srcs=[],
    hdrs=[],
    deps=[
        '//aios/storage/indexlib/index/common/data_structure:AdaptiveAttributeOffsetDumper',
        '//aios/storage/indexlib/index/common/data_structure:AttributeCompressInfo',
        '//aios/storage/indexlib/index/common/data_structure:EqualValueCompressAdvisor',
        '//aios/storage/indexlib/index/common/data_structure:EqualValueCompressDumper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataAccessor',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataDumper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataIterator',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParam',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataWriter'
    ]
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Constant']
)
strict_cc_library(
    name='type_traits',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':AttrTypeTraits', ':FieldTypeTraits']
)
strict_cc_library(
    name='LiteTerm',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/common:constants'
    ]
)
strict_cc_library(name='Term', deps=[':LiteTerm', '//aios/autil:json'])
strict_cc_library(name='NumberTerm', srcs=[], deps=[':Term'])
strict_cc_library(name='PrimaryKeyHashType', srcs=[])
strict_cc_library(
    name='KeyHasherWrapper',
    deps=[
        ':PrimaryKeyHashType', ':Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:KeyHasherTyped'
    ]
)
strict_cc_library(
    name='DictKeyInfo',
    deps=[
        '//aios/autil:multi_value', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
    name='DictHasher', deps=[':DictKeyInfo', ':KeyHasherWrapper', ':Term']
)
strict_cc_library(name='Types', srcs=[])
strict_cc_library(
    name='Constant',
    srcs=[],
    deps=[':Types', '//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='AtomicValue',
    srcs=[],
    deps=['//aios/storage/indexlib/file_system:byte_slice_rw']
)
strict_cc_library(
    name='AtomicValueTyped',
    srcs=[],
    deps=[
        ':AtomicValue',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(name='MultiValue', deps=[':AtomicValue'])
strict_cc_library(
    name='ShortBuffer',
    deps=[
        ':AtomicValueTyped', ':MultiValue', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='FileCompressParamHelper',
    srcs=[],
    deps=[
        '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/config:FileCompressConfig',
        '//aios/storage/indexlib/config:FileCompressConfigV2',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentStatistics',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParam'
    ]
)
strict_cc_library(
    name='GroupFieldDataWriter',
    deps=[
        ':Constant', ':FileCompressParamHelper',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataAccessor',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataDumper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataWriter'
    ]
)
strict_cc_library(
    name='error_code',
    srcs=[],
    hdrs=['ErrorCode.h'],
    deps=[
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
    name='radix_tree',
    srcs=['RadixTree.cpp', 'RadixTreeNode.cpp'],
    hdrs=['RadixTree.h', 'RadixTreeNode.h'],
    deps=['//aios/autil:log', '//aios/autil:mem_pool_base']
)
strict_cc_library(
    name='typed_slice_list',
    srcs=[],
    hdrs=['TypedSliceList.h'],
    deps=[':radix_tree', '//aios/storage/indexlib/base:Define']
)
strict_cc_library(
    name='fs_writer_param_decider',
    srcs=[],
    hdrs=['DefaultFSWriterParamDecider.h', 'FSWriterParamDecider.h'],
    deps=['//aios/storage/indexlib/file_system:interface']
)
strict_cc_library(
    name='AttributeValueTypeTraits',
    srcs=[],
    deps=[
        '//aios/autil:long_hash_value', '//aios/autil:multi_value',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(name='FieldTypeTraits', srcs=[])
strict_cc_library(
    name='AttrTypeTraits',
    srcs=[],
    deps=[
        ':Types', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/base/proto:querier_proto_base'
    ]
)
strict_cc_library(
    name='SortValueConvertor',
    deps=[
        ':FieldTypeTraits', '//aios/storage/indexlib/base:BinaryStringUtil',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/config:TabletSchema'
    ]
)
strict_cc_library(
    name='BuildWorkItem',
    deps=[
        '//aios/autil:lock', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(
    name='IndexerOrganizerMeta',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='IndexerOrganizerUtil',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/operation_log:constants',
        '//aios/storage/indexlib/table/normal_table/virtual_attribute:Common'
    ]
)
strict_cc_library(
    name='ShardPartitioner',
    deps=[
        ':Types', '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:ErrorLogCollector',
        '//aios/storage/indexlib/util:KeyHasherTyped'
    ]
)
strict_cc_library(
    name='PlainDocMapper',
    deps=[
        ':Types', '//aios/autil:log', '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
