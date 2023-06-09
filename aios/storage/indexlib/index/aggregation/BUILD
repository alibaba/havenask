load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(name='Constants', srcs=[])
indexlib_cc_library(
    name='AggregationIndexConfig',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':Constants', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/index/kv/config:ValueConfig',
        '//aios/storage/indexlib/util:Exception'
    ]
)
indexlib_cc_library(
    name='AggregationKeyHasher',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:hash_base', '//aios/autil:multi_value',
        '//aios/autil:span'
    ]
)
indexlib_cc_library(
    name='AggregationIndexMerger',
    deps=[
        ':agg_reader', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
indexlib_cc_library(
    name='reader_interface',
    srcs=[
        'IndexMeta.cpp', 'RandomAccessValueIterator.cpp',
        'SequentialMultiSegmentValueIterator.cpp',
        'SortedMultiSegmentValueIterator.cpp'
    ],
    hdrs=[
        'IKeyIterator.h', 'ISegmentReader.h', 'IValueIterator.h', 'IndexMeta.h',
        'KeyMeta.h', 'RandomAccessValueIterator.h',
        'SequentialMultiSegmentValueIterator.h',
        'SortedMultiSegmentValueIterator.h', 'ValueStat.h'
    ],
    deps=[
        '//aios/autil:json', '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/autil:span', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:PackValueComparator'
    ]
)
indexlib_cc_library(
    name='agg_mem_indexer',
    srcs=[
        'AggregationMemIndexer.cpp', 'AggregationMemReader.cpp',
        'FixedSizeValueBlock.cpp', 'FixedSizeValueList.cpp', 'KeyVector.cpp',
        'PostingMap.cpp', 'ReadWriteState.cpp'
    ],
    hdrs=[
        'AggregationMemIndexer.h', 'AggregationMemReader.h',
        'FixedSizeValueBlock.h', 'FixedSizeValueList.h', 'IValueList.h',
        'KeyVector.h', 'PostingMap.h', 'ReadWriteState.h'
    ],
    deps=[
        ':AggregationIndexConfig', ':AggregationIndexFields',
        ':reader_interface', '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/framework:SegmentMetrics',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/attribute:decoder',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/util:HashMap',
        '//aios/storage/indexlib/util:PoolUtil',
        '//aios/storage/indexlib/util/slice_array'
    ]
)
indexlib_cc_library(
    name='agg_leaf_reader',
    srcs=['AggregationDiskIndexer.cpp', 'AggregationLeafReader.cpp'],
    hdrs=['AggregationDiskIndexer.h', 'AggregationLeafReader.h'],
    deps=[
        ':AggregationIndexConfig', ':reader_interface',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(
    name='agg_reader',
    srcs=[
        'AggregationReader.cpp', 'Aggregator.cpp',
        'MultiSegmentKeyIterator.cpp', 'RangeIteratorCreator.cpp',
        'ValueIteratorCreator.cpp', 'VersionMergingIteratorCreator.cpp'
    ],
    hdrs=[
        'AggregationFunctor.h', 'AggregationReader.h', 'Aggregator.h',
        'DistinctValueIterator.h', 'Filter.h', 'MultiSegmentKeyIterator.h',
        'RangeIteratorCreator.h', 'SortedVersionMergingIterator.h',
        'ValueIteratorCreator.h', 'ValueIteratorWithFilter.h',
        'VersionMergingIteratorCreator.h'
    ],
    deps=[
        ':AggregationKeyHasher', ':agg_leaf_reader', ':agg_mem_indexer',
        '//aios/autil:result', '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:PackValueComparator'
    ]
)
indexlib_cc_library(
    name='AggregationIndexFactory',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':AggregationIndexConfig', ':AggregationIndexFieldsParser',
        ':AggregationIndexMerger', ':agg_leaf_reader', ':agg_mem_indexer',
        ':agg_reader', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index:IndexerParameter'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='AggregationIndexFields',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':Constants', '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/document:IIndexFields',
        '//aios/storage/indexlib/util:PooledContainer'
    ]
)
indexlib_cc_library(
    name='AggregationIndexFieldsParser',
    deps=[
        ':AggregationIndexConfig', ':AggregationIndexFields',
        ':AggregationKeyHasher',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/document/aggregation:DeleteValueEncoder',
        '//aios/storage/indexlib/document/kv:ValueConvertor',
        '//aios/storage/indexlib/index/attribute:config',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
