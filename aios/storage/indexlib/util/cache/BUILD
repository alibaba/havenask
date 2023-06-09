load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='basic_cache',
    srcs=[
        'BlockCache.cpp', 'MemoryBlockCache.cpp', 'SearchCache.cpp',
        'SearchCacheCreator.cpp', 'SearchCachePartitionWrapper.cpp',
        'SearchCacheTaskItem.cpp'
    ],
    hdrs=[
        'Block.h', 'BlockAccessCounter.h', 'BlockAllocator.h', 'BlockCache.h',
        'BlockCacheOption.h', 'BlockHandle.h', 'CacheResourceInfo.h',
        'CacheType.h', 'HistogramCounter.h', 'MemoryBlockCache.h',
        'SearchCache.h', 'SearchCacheCounter.h', 'SearchCacheCreator.h',
        'SearchCachePartitionWrapper.h', 'SearchCacheTaskItem.h'
    ],
    deps=[
        '//aios/autil:block_cache', '//aios/autil:cache', '//aios/autil:cache2',
        '//aios/autil:murmur_hash', '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/util:Timer',
        '//aios/storage/indexlib/util:key_value_map',
        '//aios/storage/indexlib/util:prime_number_table',
        '//aios/storage/indexlib/util:task',
        '//aios/storage/indexlib/util/counter',
        '//aios/storage/indexlib/util/memory_control',
        '//aios/storage/indexlib/util/metrics:metric_reporter',
        '//aios/storage/indexlib/util/metrics:monitor'
    ]
)
indexlib_cc_library(
    name='cache',
    srcs=['BlockCacheCreator.cpp'],
    hdrs=['BlockCacheCreator.h'],
    deps=([':basic_cache'] + [])
)
