load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='kkv_index_merger',
    srcs=[
        'KKVMerger.cpp', 'OnDiskKKVIterator.cpp',
        'OnDiskKKVSegmentIterator.cpp', 'OnDiskSinglePKeyIterator.cpp'
    ],
    hdrs=[
        'KKVMerger.h', 'OnDiskKKVIterator.h', 'OnDiskKKVSegmentIterator.h',
        'OnDiskSinglePKeyIterator.h'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/config',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/built:kkv_disk_indexer',
        '//aios/storage/indexlib/index/kkv/built:kkv_disk_iterator',
        '//aios/storage/indexlib/index/kkv/common:KKVIndexFormat',
        '//aios/storage/indexlib/index/kkv/common:KKVRecordFilter',
        '//aios/storage/indexlib/index/kkv/common:KKVSegmentIteratorBase',
        '//aios/storage/indexlib/index/kkv/common:KKVSegmentStatistics',
        '//aios/storage/indexlib/index/kkv/common:Trait',
        '//aios/storage/indexlib/index/kkv/dump:KKVFileWriterOptionHelper',
        '//aios/storage/indexlib/index/kkv/dump:kkv_data_dumper',
        '//aios/storage/indexlib/index/kkv/pkey_table',
        '//aios/storage/indexlib/util/metrics:progress_metrics'
    ]
)
