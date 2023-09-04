load('//bazel:defs.bzl', 'gen_cpp_code')
load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':KVIndexFactory']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':kv_reader', ':reader_interface']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/kv/config']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant']
)
strict_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':kv_merger']
)
strict_cc_library(
    name='metrics',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':kv_metrics']
)
hash_table_elements = [
    'int8_t', 'uint8_t', 'int16_t', 'uint16_t', 'int32_t', 'uint32_t',
    'int64_t', 'uint64_t', 'float', 'double', 'indexlib::byte1_t',
    'indexlib::byte2_t', 'indexlib::byte3_t', 'indexlib::byte4_t',
    'indexlib::byte5_t', 'indexlib::byte6_t', 'indexlib::byte7_t',
    'indexlib::byte8_t'
]
template_header1 = '\n#include "indexlib/index/kv/FixedLenCuckooHashTableCreatorRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_header2 = '\n#include "indexlib/index/kv/FixedLenDenseHashTableCreatorRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_header3 = '\n#include "indexlib/index/kv/FixedLenCuckooHashTableFileReaderCreatorRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_header4 = '\n#include "indexlib/index/kv/FixedLenDenseHashTableFileReaderCreatorRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_body = '\nINDEXLIB_KV_HASHTABLE_INSTANTIATION_VALUETYPE(Timestamp0Value<{0}>)\nINDEXLIB_KV_HASHTABLE_INSTANTIATION_VALUETYPE(TimestampValue<{0}>);\n'
template_tail = '\n}}\n'
gen_cpp_code(
    name='gen_fix_len_cuckoo_hash_table',
    element_per_file=1,
    elements_list=[hash_table_elements],
    template=template_body,
    template_header=template_header1,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_fix_len_dense_hash_table',
    element_per_file=1,
    elements_list=[hash_table_elements],
    template=template_body,
    template_header=template_header2,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_fix_len_cucoo_hash_table_file_reader',
    element_per_file=1,
    elements_list=[hash_table_elements],
    template=template_body,
    template_header=template_header3,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_fix_len_dense_hash_table_file_reader',
    element_per_file=1,
    elements_list=[hash_table_elements],
    template=template_body,
    template_header=template_header4,
    template_tail=template_tail
)
strict_cc_library(
    name='kv_common',
    srcs=(((([
        'FixedLenHashTableCreator.cpp', 'KVCommonDefine.cpp',
        'KVFormatOptions.cpp', 'KVTimestamp.cpp', 'KVTypeId.cpp',
        'ValueExtractorUtil.cpp', 'VarLenHashTableCollector.cpp',
        'VarLenHashTableCreator.cpp'
    ] + [':gen_fix_len_cuckoo_hash_table']) + [':gen_fix_len_dense_hash_table'])
           + [':gen_fix_len_cucoo_hash_table_file_reader']) +
          [':gen_fix_len_dense_hash_table_file_reader']),
    hdrs=[
        'FixedLenCuckooHashTableCreator.h',
        'FixedLenCuckooHashTableCreatorRegister.h',
        'FixedLenCuckooHashTableFileReaderCreator.h',
        'FixedLenCuckooHashTableFileReaderCreatorRegister.h',
        'FixedLenDenseHashTableCreator.h',
        'FixedLenDenseHashTableCreatorRegister.h',
        'FixedLenDenseHashTableFileReaderCreator.h',
        'FixedLenDenseHashTableFileReaderCreatorRegister.h',
        'FixedLenHashTableCreator.h', 'FixedLenValueExtractorUtil.h',
        'KVCommonDefine.h', 'KVFormatOptions.h', 'KVTimestamp.h', 'KVTypeId.h',
        'Record.h', 'ValueExtractorUtil.h', 'VarLenHashTableCollector.h',
        'VarLenHashTableCreator.h'
    ],
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:decoder',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/common/hash_table',
        '//aios/storage/indexlib/index/kv/config'
    ]
)
strict_cc_library(
    name='reader_interface',
    srcs=[],
    hdrs=['IKVIterator.h', 'IKVSegmentReader.h', 'KVMetricsCollector.h'],
    deps=[':kv_common']
)
strict_cc_library(
    name='kv_metrics', srcs=[], hdrs=['KVMetricsCollector.h'], deps=[]
)
strict_cc_library(
    name='writer_common',
    srcs=[
        'KeyWriter.cpp', 'RecordComparator.cpp', 'SegmentStatistics.cpp',
        'ValueWriter.cpp'
    ],
    hdrs=[
        'KeyWriter.h', 'MemoryUsage.h', 'RecordComparator.h',
        'SegmentStatistics.h', 'ValueWriter.h'
    ],
    deps=[
        ':kv_common', '//aios/storage/indexlib/framework:SegmentMetrics',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:PackValueComparator'
    ]
)
strict_cc_library(
    name='kv_mem_indexer',
    srcs=[
        'FixedLenKVMemIndexer.cpp', 'FixedLenKVMemoryReader.cpp',
        'InMemoryValueWriter.cpp', 'KVMemIndexerBase.cpp',
        'KVSortDataCollector.cpp', 'VarLenKVMemIndexer.cpp',
        'VarLenKVMemoryReader.cpp'
    ],
    hdrs=[
        'FixedLenKVMemIndexer.h', 'FixedLenKVMemoryReader.h',
        'InMemoryValueWriter.h', 'KVMemIndexerBase.h', 'KVSortDataCollector.h',
        'ReclaimedValueCollector.h', 'VarLenKVMemIndexer.h',
        'VarLenKVMemoryReader.h'
    ],
    deps=[
        ':KVIndexFields', ':reader_interface', ':writer_common',
        '//aios/autil:log', '//aios/autil:mem_pool_container',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/document/kv:kv_document',
        '//aios/storage/indexlib/framework/mem_reclaimer:EpochBasedMemReclaimer',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/data_structure:ExpandableValueAccessor'
    ]
)
strict_cc_library(
    name='kv_merger',
    srcs=[
        'DFSValueWriter.cpp', 'FixedLenKVMerger.cpp', 'KVMerger.cpp',
        'KeyMergeWriter.cpp', 'TTLFilter.cpp', 'VarLenKVMerger.cpp'
    ],
    hdrs=[
        'DFSValueWriter.h', 'FixedLenKVMerger.h', 'KVMerger.h',
        'KeyMergeWriter.h', 'NoneFilter.h', 'RecordFilter.h', 'TTLFilter.h',
        'VarLenKVMerger.h'
    ],
    deps=[
        ':kv_reader', ':writer_common', '//aios/autil:string_helper',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
strict_cc_library(
    name='KVIndexFactory',
    deps=[
        ':KVIndexFieldsParser', ':KVShardRecordIterator', ':kv_mem_indexer',
        ':kv_merger', ':kv_reader', '//aios/autil:log',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index:IndexerParameter'
    ],
    alwayslink=True
)
strict_cc_library(
    name='kv_reader',
    srcs=[
        'AdapterIgnoreFieldCalculator.cpp', 'AdapterKVSegmentIterator.cpp',
        'AdapterKVSegmentReader.cpp', 'FSValueReader.cpp',
        'FieldValueExtractor.cpp', 'FixedLenKVLeafReader.cpp',
        'FixedLenKVSegmentIterator.cpp', 'FixedLenValueReader.cpp',
        'KVDiskIndexer.cpp', 'KVIndexReader.cpp', 'KVKeyIterator.cpp',
        'KVSegmentReaderCreator.cpp', 'KeyReader.cpp',
        'MultiSegmentKVIterator.cpp', 'SimpleMultiSegmentKVIterator.cpp',
        'SingleShardKVIndexReader.cpp', 'SortedMultiSegmentKVIterator.cpp',
        'VarLenKVCompressedLeafReader.cpp', 'VarLenKVLeafReader.cpp',
        'VarLenKVSegmentIterator.cpp', 'VarLenValueReader.cpp'
    ],
    hdrs=[
        'AdapterIgnoreFieldCalculator.h', 'AdapterKVSegmentIterator.h',
        'AdapterKVSegmentReader.h', 'FSValueReader.h', 'FieldValueExtractor.h',
        'FixedLenKVLeafReader.h', 'FixedLenKVSegmentIterator.h',
        'FixedLenValueReader.h', 'KVDiskIndexer.h', 'KVIndexReader.h',
        'KVKeyIterator.h', 'KVReadOptions.h', 'KVSegmentReaderCreator.h',
        'KeyReader.h', 'MultiSegmentKVIterator.h',
        'SimpleMultiSegmentKVIterator.h', 'SingleShardKVIndexReader.h',
        'SortedMultiSegmentKVIterator.h', 'ValueReader.h',
        'VarLenKVCompressedLeafReader.h', 'VarLenKVLeafReader.h',
        'VarLenKVSegmentIterator.h', 'VarLenValueReader.h'
    ],
    deps=[
        ':kv_common', ':kv_mem_indexer', ':reader_interface',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader'
    ]
)
strict_cc_library(name='Common', srcs=[])
strict_cc_library(name='Types', srcs=[])
strict_cc_library(name='Constant', srcs=[], deps=[':Types'])
strict_cc_library(
    name='KVShardRecordIterator',
    deps=[':kv_merger', '//aios/storage/indexlib/index:IShardRecordIterator']
)
strict_cc_library(
    name='KVIndexFields',
    visibility=['//aios/storage/indexlib/index:__subpackages__'],
    deps=[
        ':constants', '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/document:IIndexFields',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:PooledContainer'
    ]
)
strict_cc_library(
    name='KVIndexFieldsParser',
    visibility=['//aios/storage/indexlib/index:__subpackages__'],
    deps=[
        ':KVIndexFields', ':config',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/document:BuiltinParserInitParam',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/document/kv:KVKeyExtractor',
        '//aios/storage/indexlib/document/kv:ValueConvertor',
        '//aios/storage/indexlib/util:DocTracer',
        '//aios/storage/indexlib/util:PooledUniquePtr',
        '//aios/storage/indexlib/util/counter'
    ]
)
