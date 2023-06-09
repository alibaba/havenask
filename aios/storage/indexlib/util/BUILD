load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage:__subpackages__'])
indexlib_cc_library(
    name='ip_convertor',
    srcs=['IpConvertor.cpp'],
    hdrs=['IpConvertor.h'],
    visibility=['//visibility:public'],
    deps=['//aios/autil:string_helper']
)
indexlib_cc_library(
    name='all',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':Algorithm', ':Avl', ':Base62', ':Bitmap', ':BlockFpEncoder',
        ':ByteSimilarityHasher', ':ClassTypedFactory', ':Clock', ':ColumnUtil',
        ':Comparable', ':CoroutineConfig', ':DeVirtual', ':DebugingUtil',
        ':DocTracer', ':ErrorLogCollector', ':ExpandableBitmap',
        ':FloatInt8Encoder', ':FloatUint64Encoder', ':Fp16Encoder', ':Half',
        ':HashBucket', ':HashMap', ':HashString', ':HashUtil',
        ':KeyHasherTyped', ':MathUtil', ':MemBuffer', ':PoolUtil',
        ':PooledUniquePtr', ':Random', ':RegularExpression', ':ShardUtil',
        ':Status', ':TimestampUtil', ':ValueWriter', ':crc32c', ':epochid_util',
        ':future_executor', ':httplib', ':ip_convertor', ':mmap_pool',
        ':mmap_vector', ':object_pool', ':path_util', ':prime_number_table',
        ':retry_util', ':simple_pool', ':task', ':thread_pool',
        '//aios/storage/indexlib/util/buffer_compressor',
        '//aios/storage/indexlib/util/byte_slice_list',
        '//aios/storage/indexlib/util/cache',
        '//aios/storage/indexlib/util/counter',
        '//aios/storage/indexlib/util/memory_control',
        '//aios/storage/indexlib/util/metrics:metric_reporter',
        '//aios/storage/indexlib/util/metrics:monitor',
        '//aios/storage/indexlib/util/slice_array'
    ]
)
indexlib_cc_library(
    name='headers',
    srcs=glob([
        '*.h', 'counter/*.h', 'byte_slice_list/*.h', 'cache/*.h',
        'slice_array/*.h', 'memory_control/*.h', 'buffer_compressor/*.h'
    ]),
    hdrs=[]
)
indexlib_cc_library(name='Timer', srcs=[])
indexlib_cc_library(name='numeric_util', srcs=[], hdrs=['NumericUtil.h'])
indexlib_cc_library(
    name='Bitmap',
    deps=[':PoolUtil', ':numeric_util', '//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='ExpandableBitmap', deps=[':Bitmap', '//aios/autil:log']
)
indexlib_cc_library(
    name='key_value_map',
    srcs=[],
    hdrs=['KeyValueMap.h'],
    deps=['//aios/autil:json', '//aios/autil:string_helper']
)
indexlib_cc_library(
    name='prime_number_table',
    srcs=['PrimeNumberTable.cpp'],
    hdrs=['PrimeNumberTable.h']
)
indexlib_cc_library(
    name='task',
    srcs=['TaskGroup.cpp', 'TaskScheduler.cpp'],
    hdrs=['TaskGroup.h', 'TaskItem.h', 'TaskScheduler.h'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='Base62',
    copts=['-Wno-suggest-override'],
    deps=['//aios/storage/indexlib/base:Status', '@boost//:multiprecision']
)
indexlib_cc_library(
    name='mmap_allocator',
    srcs=['MMapAllocator.cpp'],
    hdrs=['MMapAllocator.h'],
    deps=[
        '//aios/autil:env_util', '//aios/autil:log',
        '//aios/autil:mem_pool_base'
    ]
)
indexlib_cc_library(
    name='mmap_pool',
    srcs=['MmapPool.cpp'],
    hdrs=['MmapPool.h'],
    deps=[':mmap_allocator', '//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='simple_pool',
    srcs=['SimplePool.cpp'],
    hdrs=['SimplePool.h'],
    deps=['//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='simple_heap',
    srcs=[],
    hdrs=['SimpleHeap.h'],
    deps=['//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='object_pool', srcs=[], hdrs=['ObjectPool.h'], deps=[':Exception']
)
indexlib_cc_library(
    name='thread_pool',
    srcs=['GroupedThreadPool.cpp'],
    hdrs=['GroupedThreadPool.h'],
    deps=[
        '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='mmap_vector',
    srcs=[],
    hdrs=['MMapVector.h'],
    deps=[':mmap_allocator', '//aios/autil:log']
)
indexlib_cc_library(
    name='VectorTyped',
    srcs=[],
    deps=[
        ':PoolUtil', '//aios/autil:data_buffer', '//aios/autil:lock',
        '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='crc32c',
    srcs=['CRC32C.cpp', 'Coding.cpp'],
    hdrs=['CRC32C.h', 'Coding.h'],
    deps=['//aios/autil:log']
)
indexlib_cc_library(
    name='epochid_util',
    srcs=['EpochIdUtil.cpp'],
    hdrs=['EpochIdUtil.h'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/autil:string_helper'
    ]
)
indexlib_cc_library(
    name='future_executor',
    srcs=['FutureExecutor.cpp'],
    hdrs=['FutureExecutor.h'],
    deps=[
        '//aios/future_lite', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util/metrics:monitor'
    ]
)
indexlib_cc_library(
    name='path_util',
    srcs=['PathUtil.cpp'],
    hdrs=['PathUtil.h'],
    deps=['//aios/autil:log', '//aios/filesystem/fslib:fslib-framework']
)
indexlib_cc_library(
    name='retry_util',
    srcs=['RetryUtil.cpp'],
    hdrs=['RetryUtil.h'],
    deps=['//aios/autil:log']
)
indexlib_cc_library(name='ColumnUtil', srcs=[])
indexlib_cc_library(name='ShardUtil', srcs=[])
indexlib_cc_library(
    name='Singleton',
    srcs=[],
    deps=[
        '//aios/autil:lock', '//aios/autil:log',
        '//aios/storage/indexlib/base:Define'
    ]
)
indexlib_cc_library(
    name='PoolUtil', srcs=[], deps=['//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='RegularExpression',
    deps=['//aios/autil:common_macros', '//aios/autil:string_type']
)
indexlib_cc_library(
    name='Exception',
    srcs=[],
    deps=[
        ':IoExceptionController', '//aios/autil:exception', '//aios/autil:log'
    ]
)
indexlib_cc_library(name='IoExceptionController')
indexlib_cc_library(name='DeVirtual', srcs=[])
indexlib_cc_library(
    name='CoroutineConfig',
    srcs=[],
    hdrs=[':CoroutineConfigH'],
    include_prefix='indexlib/util'
)
config_setting(name='Coroutine', values={'define': 'indexlib_coroutine=yes'})
genrule(
    name='CoroutineConfigH',
    srcs=['CoroutineConfig.h.in'],
    outs=['CoroutineConfig.h'],
    cmd=select({
        ':Coroutine': 'sed s/TO_BE_REPLACED/1/ $(SRCS) > $(OUTS)',
        '//conditions:default': 'sed s/TO_BE_REPLACED/0/ $(SRCS) > $(OUTS)'
    })
)
indexlib_cc_library(name='Traits', srcs=[])
indexlib_cc_library(name='Random', srcs=[])
indexlib_cc_library(name='Status', srcs=[])
indexlib_cc_library(name='FloatInt8Encoder', srcs=[])
indexlib_cc_library(name='FloatUint64Encoder', srcs=[])
indexlib_cc_library(name='Half', srcs=[])
indexlib_cc_library(name='Fp16Encoder', srcs=[])
indexlib_cc_library(name='HashString', srcs=[])
indexlib_cc_library(name='MathUtil', deps=['//aios/autil:log'])
indexlib_cc_library(name='MemBuffer', srcs=[], deps=[':simple_pool'])
indexlib_cc_library(name='PooledUniquePtr', srcs=[])
indexlib_cc_library(
    name='ValueWriter', srcs=[], deps=['//aios/storage/indexlib/base:Define']
)
indexlib_cc_library(name='HashUtil', srcs=[], deps=['//aios/autil:hash_base'])
indexlib_cc_library(
    name='HashBucket',
    srcs=[],
    deps=[':Bitmap', '//aios/storage/indexlib/base:Define']
)
indexlib_cc_library(
    name='HashMap',
    srcs=[],
    deps=[':HashBucket', ':HashUtil', '//aios/storage/indexlib/base:constants']
)
indexlib_cc_library(name='DebugingUtil')
indexlib_cc_library(
    name='KeyHasherTyped',
    deps=[
        ':key_value_map', '//aios/autil:murmur_hash',
        '//aios/autil:numbers_helper',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(name='Comparable', srcs=[])
indexlib_cc_library(name='Clock', srcs=[])
indexlib_cc_library(name='Avl', srcs=[], deps=[':Comparable'])
indexlib_cc_library(name='ByteSimilarityHasher', deps=[':Singleton'])
indexlib_cc_library(
    name='BlockFpEncoder',
    deps=['//aios/autil:mem_pool_base', '//aios/autil:string_helper']
)
indexlib_cc_library(
    name='TimestampUtil',
    srcs=['TimestampUtil.cpp'],
    hdrs=['TimestampUtil.h', 'TimestampUtilDefine.h'],
    deps=[
        ':Exception', '//aios/autil:multi_value', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(name='ErrorLogCollector', deps=['//aios/autil:log'])
indexlib_cc_library(
    name='EpochBasedReclaimManager',
    deps=['//aios/autil:log', '//aios/autil:thread']
)
indexlib_cc_library(
    name='DocTracer', srcs=[], deps=['//aios/aios/common/beeper']
)
indexlib_cc_library(name='Algorithm', srcs=[], deps=['//aios/alog'])
indexlib_cc_library(
    name='ClassTypedFactory',
    srcs=[],
    deps=[
        '//aios/autil:long_hash_value', '//aios/storage/indexlib/base:FieldType'
    ]
)
indexlib_cc_library(name='httplib', srcs=[])
indexlib_cc_library(
    name='status_to_exception',
    srcs=[],
    hdrs=['Status2Exception.h'],
    deps=[':Exception', '//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='PooledContainer', srcs=[], deps=['//aios/autil:mem_pool_base']
)
