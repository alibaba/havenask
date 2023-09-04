load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='all',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':Algorithm', ':Avl', ':Base62', ':Bitmap', ':BlockFpEncoder',
        ':ByteSimilarityHasher', ':ClassTypedFactory', ':Clock', ':ColumnUtil',
        ':Comparable', ':CoroutineConfig', ':DeVirtual', ':DebugingUtil',
        ':DocTracer', ':ErrorLogCollector', ':ExpandableBitmap',
        ':FloatInt8Encoder', ':FloatUint64Encoder', ':Fp16Encoder',
        ':GroupedThreadPool', ':Half', ':HashBucket', ':HashMap', ':HashString',
        ':HashUtil', ':IpConvertor', ':KeyHasherTyped', ':MMapVector',
        ':MathUtil', ':MemBuffer', ':MmapPool', ':ObjectPool', ':PathUtil',
        ':PoolUtil', ':PooledUniquePtr', ':PrimeNumberTable', ':Random',
        ':RegularExpression', ':RetryUtil', ':ShardUtil', ':SimplePool',
        ':Status', ':TimestampUtil', ':ValueWriter', ':epochid_util',
        ':future_executor', ':httplib', ':task',
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
strict_cc_library(
    name='headers',
    srcs=glob([
        '*.h', 'counter/*.h', 'byte_slice_list/*.h', 'cache/*.h',
        'slice_array/*.h', 'memory_control/*.h', 'buffer_compressor/*.h'
    ]),
    hdrs=[]
)
strict_cc_library(name='Timer', srcs=[])
strict_cc_library(name='NumericUtil', srcs=[], export_name='numeric_util')
strict_cc_library(
    name='Bitmap',
    deps=[':PoolUtil', ':numeric_util', '//aios/autil:mem_pool_base']
)
strict_cc_library(name='ExpandableBitmap', deps=[':Bitmap', '//aios/autil:log'])
strict_cc_library(
    name='KeyValueMap',
    srcs=[],
    export_name='key_value_map',
    deps=['//aios/autil:json', '//aios/autil:string_helper']
)
strict_cc_library(
    name='JsonMap',
    srcs=[],
    export_name='json_map',
    export_visibility=['//visibility:public'],
    deps=['//aios/autil:json', '//aios/storage/indexlib/base:Status']
)
strict_cc_library(name='PrimeNumberTable', export_name='prime_number_table')
strict_cc_library(
    name='task',
    srcs=['TaskGroup.cpp', 'TaskScheduler.cpp'],
    hdrs=['TaskGroup.h', 'TaskItem.h', 'TaskScheduler.h'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(
    name='Base62',
    copts=['-Wno-suggest-override'],
    deps=['//aios/storage/indexlib/base:Status', '@boost//:multiprecision']
)
strict_cc_library(
    name='MMapAllocator',
    export_name='mmap_allocator',
    deps=[
        '//aios/autil:env_util', '//aios/autil:log',
        '//aios/autil:mem_pool_base'
    ]
)
strict_cc_library(
    name='MmapPool',
    export_name='mmap_pool',
    deps=[':MMapAllocator', '//aios/autil:mem_pool_base']
)
strict_cc_library(
    name='SimplePool',
    export_name='simple_pool',
    deps=['//aios/autil:mem_pool_base']
)
strict_cc_library(
    name='SimpleHeap',
    srcs=[],
    export_name='simple_heap',
    deps=['//aios/autil:mem_pool_base']
)
strict_cc_library(
    name='ObjectPool', srcs=[], export_name='object_pool', deps=[':Exception']
)
strict_cc_library(
    name='GroupedThreadPool',
    deps=[
        '//aios/autil:env_util', '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(
    name='MMapVector',
    srcs=[],
    export_name='mmap_vector',
    deps=[':MMapAllocator', '//aios/autil:log']
)
strict_cc_library(
    name='VectorTyped',
    srcs=[],
    deps=[
        ':PoolUtil', '//aios/autil:data_buffer', '//aios/autil:lock',
        '//aios/autil:log'
    ]
)
strict_cc_library(
    name='EpochIdUtil',
    export_name='epochid_util',
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/autil:string_helper'
    ]
)
strict_cc_library(
    name='FutureExecutor',
    export_name='future_executor',
    deps=[
        '//aios/future_lite', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util/metrics:monitor'
    ]
)
strict_cc_library(
    name='PathUtil',
    export_name='path_util',
    deps=['//aios/autil:log', '//aios/filesystem/fslib:fslib-framework']
)
strict_cc_library(
    name='RetryUtil', export_name='retry_util', deps=['//aios/autil:log']
)
strict_cc_library(name='ColumnUtil', srcs=[])
strict_cc_library(name='ShardUtil', srcs=[])
strict_cc_library(
    name='Singleton',
    srcs=[],
    export_name='singleton',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log',
        '//aios/storage/indexlib/base:Define'
    ]
)
strict_cc_library(name='PoolUtil', srcs=[], deps=['//aios/autil:mem_pool_base'])
strict_cc_library(
    name='RegularExpression',
    deps=['//aios/autil:common_macros', '//aios/autil:string_type']
)
strict_cc_library(
    name='Exception',
    srcs=[],
    export_name='exception',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=[
        ':IoExceptionController', '//aios/autil:exception', '//aios/autil:log'
    ]
)
strict_cc_library(name='IoExceptionController')
strict_cc_library(name='DeVirtual', srcs=[])
strict_cc_library(
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
strict_cc_library(
    name='Traits',
    srcs=[],
    export_name='traits',
    export_visibility=['//aios/storage:indexlib_extensions']
)
strict_cc_library(name='Random', srcs=[])
strict_cc_library(
    name='Status',
    srcs=[],
    export_name='status',
    export_visibility=['//aios/storage:indexlib_extensions']
)
strict_cc_library(name='FloatInt8Encoder', srcs=[])
strict_cc_library(name='FloatUint64Encoder', srcs=[])
strict_cc_library(name='Half', srcs=[])
strict_cc_library(name='Fp16Encoder', srcs=[])
strict_cc_library(name='HashString', srcs=[])
strict_cc_library(name='MathUtil', deps=['//aios/autil:log'])
strict_cc_library(
    name='MemBuffer',
    srcs=[],
    export_name='mem_buffer',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=[':SimplePool']
)
strict_cc_library(
    name='PooledUniquePtr',
    srcs=[],
    export_name='pooled_unique_ptr',
    export_visibility=['//aios/storage:indexlib_extensions']
)
strict_cc_library(
    name='ValueWriter',
    srcs=[],
    export_name='value_writer',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=['//aios/storage/indexlib/base:Define']
)
strict_cc_library(name='HashUtil', srcs=[], deps=['//aios/autil:hash_base'])
strict_cc_library(
    name='HashBucket',
    srcs=[],
    deps=[':Bitmap', '//aios/storage/indexlib/base:Define']
)
strict_cc_library(
    name='HashMap',
    srcs=[],
    export_name='hash_map',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=[':HashBucket', ':HashUtil', '//aios/storage/indexlib/base:constants']
)
strict_cc_library(name='DebugingUtil')
strict_cc_library(
    name='KeyHasherTyped',
    deps=[
        ':KeyValueMap', '//aios/autil:murmur_hash',
        '//aios/autil:numbers_helper',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(name='Comparable', srcs=[])
strict_cc_library(name='Clock', srcs=[])
strict_cc_library(name='Avl', srcs=[], deps=[':Comparable'])
strict_cc_library(name='ByteSimilarityHasher', deps=[':Singleton'])
strict_cc_library(
    name='BlockFpEncoder',
    deps=['//aios/autil:mem_pool_base', '//aios/autil:string_helper']
)
strict_cc_library(
    name='TimestampUtil',
    srcs=['TimestampUtil.cpp'],
    hdrs=['TimestampUtil.h', 'TimestampUtilDefine.h'],
    deps=[
        ':Exception', '//aios/autil:multi_value', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(
    name='ErrorLogCollector',
    deps=['//aios/autil:env_util', '//aios/autil:log']
)
strict_cc_library(
    name='EpochBasedReclaimManager',
    deps=['//aios/autil:log', '//aios/autil:thread']
)
strict_cc_library(name='DocTracer', srcs=[], deps=['//aios/aios/common/beeper'])
strict_cc_library(name='Algorithm', srcs=[], deps=['//aios/alog'])
strict_cc_library(
    name='ClassTypedFactory',
    srcs=[],
    deps=[
        '//aios/autil:long_hash_value', '//aios/storage/indexlib/base:FieldType'
    ]
)
strict_cc_library(
    name='httplib',
    srcs=[],
    export_name='http_lib',
    export_visibility=['//visibility:public']
)
strict_cc_library(
    name='Status2Exception',
    srcs=[],
    export_name='status_to_exception',
    export_visibility=['//aios/storage:__subpackages__'],
    deps=[':Exception', '//aios/storage/indexlib/base:Status']
)
strict_cc_library(
    name='PooledContainer',
    srcs=[],
    export_name='pooled_container',
    export_visibility=['//aios/storage:indexlib_extensions'],
    deps=['//aios/autil:mem_pool_base']
)
strict_cc_library(
    name='IpConvertor',
    export_name='ip_convertor',
    export_visibility=['//visibility:public'],
    deps=['//aios/autil:string_helper']
)
strict_cc_library(
    name='ProtoJsonizer',
    export_name='proto_jsonizer',
    export_visibility=['//visibility:public'],
    visibility=['//aios/storage:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '@com_google_protobuf//:protobuf'
    ]
)
