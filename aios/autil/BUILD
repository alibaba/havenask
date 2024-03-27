package(default_visibility=['//aios/autil:__subpackages__'])
cc_library(
    name='synchronized_queue',
    hdrs=['autil/SynchronizedQueue.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='scope',
    hdrs=['autil/Scope.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='defer',
    hdrs=['autil/Defer.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='closure_guard',
    hdrs=['autil/ClosureGuard.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='memory',
    hdrs=['autil/SharedPtrUtil.h', 'autil/memory.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='shared_object_map',
    srcs=['autil/SharedObjectMap.cpp'],
    hdrs=['autil/SharedObjectMap.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':lock', ':mem_pool_base'],
    alwayslink=True
)
cc_library(
    name='object_tracer',
    hdrs=['autil/ObjectTracer.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':lock']
)
cc_library(
    name='bloom_filter',
    srcs=['autil/BloomFilter.cpp'],
    hdrs=['autil/BloomFilter.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':base64', ':hash_func'],
    alwayslink=True
)
cc_library(
    name='common_macros',
    hdrs=['autil/CommonMacros.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='stack_tracer',
    srcs=['autil/StackTracer.cpp'],
    hdrs=['autil/StackTracer.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':env_util', ':lock'],
    alwayslink=True
)
cc_library(
    name='sanitizer_util',
    hdrs=['autil/SanitizerUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='range_util',
    srcs=['autil/RangeUtil.cpp'],
    hdrs=['autil/RangeUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='object_allocator',
    hdrs=['autil/FixedSizeAllocator.h', 'autil/ObjectAllocator.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='url_util',
    srcs=['autil/URLUtil.cpp', 'autil/UrlDecoder.cpp', 'autil/UrlEncode.cpp'],
    hdrs=['autil/URLUtil.h', 'autil/UrlDecoder.h', 'autil/UrlEncode.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='regex',
    srcs=['autil/Regex.cpp'],
    hdrs=['autil/Regex.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log'],
    alwayslink=True
)
cc_library(
    name='dynamic_buf',
    hdrs=['autil/DynamicBuf.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='zlib',
    srcs=['autil/ZlibCompressor.cpp'],
    hdrs=['autil/ZlibCompressor.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':dynamic_buf', ':log'],
    alwayslink=True
)
cc_library(
    name='compression',
    srcs=['autil/CompressionUtil.cpp'],
    hdrs=['autil/CompressionUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':mem_pool_base', ':zlib', '//third_party/lz4', '@snappy'],
    alwayslink=True
)
cc_library(
    name='base64',
    srcs=['autil/legacy/base64.cpp'],
    hdrs=['autil/legacy/base64.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':exception', ':log'],
    alwayslink=True
)
cc_library(
    name='time_span',
    srcs=['autil/TimeSpan.cpp'],
    hdrs=['autil/Libdivide.h', 'autil/TimeSpan.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log'],
    alwayslink=True
)
cc_library(
    name='snapshot_vector',
    hdrs=['autil/SnapshotVector.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='daily_run_mode',
    srcs=['autil/DailyRunMode.cpp'],
    hdrs=['autil/DailyRunMode.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':env_util', ':log'],
    alwayslink=True
)
cc_library(
    name='env_util',
    srcs=['autil/EnvUtilImpl.cpp'],
    hdrs=['autil/EnvUtil.h', 'autil/EnvUtilImpl.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=([':string_helper', ':log', ':singleton'] + select({
        '//:hack_get_set_env': [':hack_env'],
        '//conditions:default': []
    })),
    alwayslink=True
)
cc_library(
    name='diagnostic',
    hdrs=['autil/Diagnostic.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='containers',
    hdrs=['autil/Autovector.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='cache2',
    srcs=[],
    hdrs=[
        'autil/Cache.h', 'autil/HashUtil.h', 'autil/LruCache.h',
        'autil/SynchronizedHashTable.h', 'autil/SynchronizedList.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':cityhash', ':lock', ':log', ':string_type',
        '//aios/memory/lockless_allocator:lockless_api'
    ],
    alwayslink=True
)
cc_library(
    name='cache',
    srcs=[
        'autil/cache/cache_hash.cpp', 'autil/cache/cache_hash.h',
        'autil/cache/cache_wrapper.cpp', 'autil/cache/lru_cache.cpp',
        'autil/cache/sharded_cache.cpp'
    ],
    hdrs=[
        'autil/cache/cache.h', 'autil/cache/cache_wrapper.h',
        'autil/cache/cache_allocator.h', 'autil/cache/sharded_cache.h',
        'autil/cache/lru_cache.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':containers', ':lock', ':string_type',
        '//aios/memory/lockless_allocator:lockless_api', '//third_party/tbb'
    ]
)
cc_library(
    name='lock',
    srcs=['autil/Lock.cpp'],
    hdrs=['autil/Lock.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':log', ':thread_annotations', ':time']
)
cc_library(
    name='thread',
    srcs=[
        'autil/LoopThread.cpp', 'autil/OutputOrderedThreadPool.cpp',
        'autil/Thread.cpp', 'autil/ThreadLocal.cpp', 'autil/ThreadPool.cpp',
        'autil/ThreadPoolManager.cpp'
    ],
    hdrs=[
        'autil/AtomicCounter.h', 'autil/CircularQueue.h',
        'autil/FixedSizeRecyclePool.h', 'autil/LambdaWorkItem.h',
        'autil/LoopThread.h', 'autil/OutputOrderedThreadPool.h',
        'autil/Thread.h', 'autil/ThreadLocal.h', 'autil/ThreadPool.h',
        'autil/ThreadPoolManager.h', 'autil/WorkItem.h', 'autil/WorkItemQueue.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':NoCopyable', ':containers', ':daily_run_mode', ':exception', ':lock',
        ':log', ':sanitizer_util', ':string_helper', ':thread_name_scope'
    ],
    alwayslink=True
)
cc_library(
    name='thread_name_scope',
    srcs=['autil/ThreadNameScope.cpp'],
    hdrs=['autil/ThreadNameScope.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='lock_free',
    srcs=['autil/LockFreeThreadPool.cpp', 'autil/ThreadAutoScaler.cpp'],
    hdrs=[
        'autil/LockFree.h', 'autil/LockFreeQueue.h', 'autil/LockFreeStack.h',
        'autil/LockFreeThreadPool.h', 'autil/ThreadAutoScaler.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=['env_util', ':thread'],
    alwayslink=True
)
cc_library(
    name='block_cache',
    srcs=[
        'autil/Block.cpp', 'autil/BlockCache.cpp', 'autil/BlockPool.cpp',
        'autil/LRUReplacePolicy.cpp', 'autil/MmapBlockAllocator.cpp',
        'autil/SimpleBlockAllocator.cpp'
    ],
    hdrs=[
        'autil/Allocators.h', 'autil/Block.h', 'autil/BlockAllocator.h',
        'autil/BlockCache.h', 'autil/BlockLinkListNode.h', 'autil/BlockPool.h',
        'autil/DoubleLinkList.h', 'autil/DoubleLinkListNode.h',
        'autil/FixedSizeAllocator.h', 'autil/FixedSizeRecyclePool.h',
        'autil/LRUReplacePolicy.h', 'autil/MmapBlockAllocator.h',
        'autil/ReferencedHashMap.h', 'autil/ReplacePolicy.h',
        'autil/SimpleBlockAllocator.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':lock', ':log', ':mem_util', ':sanitizer_util'],
    alwayslink=True
)
cc_library(
    name='codec',
    srcs=glob(['autil/codec/*.cpp']),
    hdrs=glob(['autil/codec/*.h']),
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':json', ':log', ':string_type', '//aios/alog'],
    alwayslink=True
)
cc_library(
    name='metric',
    srcs=glob(['autil/metric/*.cpp']),
    hdrs=glob(['autil/metric/*.h']),
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':time'],
    alwayslink=True
)
cc_library(
    name='time',
    srcs=['autil/TimeUtility.cpp'],
    hdrs=['autil/TimeUtility.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='timeout_terminator',
    srcs=['autil/TimeoutTerminator.cpp'],
    hdrs=['autil/TimeoutTerminator.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':time'],
    alwayslink=True
)
cc_library(
    name='bitmap',
    srcs=['autil/bitmap.cpp'],
    hdrs=['autil/bitmap.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', ':mem_pool_container', ':time'],
    alwayslink=True
)
cc_library(
    name='json_helper',
    srcs=['autil/legacy/RapidJsonHelper.cpp'],
    hdrs=['autil/legacy/RapidJsonCommon.h', 'autil/legacy/RapidJsonHelper.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', ':multi_value', ':multi_value_helper', '@rapidjson'],
    alwayslink=True
)
cc_library(
    name='json_base',
    srcs=[
        'autil/legacy/astream.cpp', 'autil/legacy/json.cpp',
        'autil/legacy/json_string_enhanced.cpp',
        'autil/legacy/string_conversion.cpp', 'autil/legacy/string_utf8.cpp'
    ],
    hdrs=[
        'autil/legacy/any.h', 'autil/legacy/astream.h', 'autil/legacy/json.h',
        'autil/legacy/jsonizable_exception.h', 'autil/legacy/string_tools.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':exception', ':string_helper'],
    alwayslink=True
)
cc_library(
    name='fast_json',
    srcs=['autil/legacy/fast_jsonizable.cpp'],
    hdrs=[
        'autil/legacy/fast_jsonizable.h', 'autil/legacy/fast_jsonizable_dec.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':json_base', ':multi_value', '@rapidjson'],
    alwayslink=True
)
cc_library(
    name='json',
    hdrs=[
        'autil/legacy/jsonizable.h', 'autil/legacy/legacy_jsonizable.h',
        'autil/legacy/legacy_jsonizable_dec.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':fast_json', ':json_base', ':multi_value', ':multi_value_helper',
        '@rapidjson'
    ],
    alwayslink=True
)
cc_library(
    name='log',
    hdrs=['autil/Log.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':time', '//aios/alog'],
    alwayslink=True
)
cc_library(
    name='crc32c',
    srcs=['autil/CRC32C.cpp', 'autil/Coding.cpp'],
    hdrs=['autil/CRC32C.h', 'autil/Coding.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='exception',
    srcs=['autil/legacy/exception.cpp'],
    hdrs=['autil/legacy/exception.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':backtrace', ':env_util'],
    alwayslink=True
)
cc_library(
    name='cipher',
    srcs=[
        'autil/cipher/AESCipherCommon.cpp', 'autil/cipher/AESCipherCreator.cpp',
        'autil/cipher/AESCipherDecrypter.cpp',
        'autil/cipher/AESCipherEncrypter.cpp',
        'autil/cipher/AESCipherStreamDecrypter.cpp',
        'autil/cipher/AESCipherStreamEncrypter.cpp',
        'autil/cipher/AESCipherUtility.cpp',
        'autil/cipher/MemoryDataPipeline.cpp'
    ],
    hdrs=[
        'autil/cipher/AESCipherCommon.h', 'autil/cipher/AESCipherCreator.h',
        'autil/cipher/AESCipherDecrypter.h',
        'autil/cipher/AESCipherEncrypter.h',
        'autil/cipher/AESCipherStreamDecrypter.h',
        'autil/cipher/AESCipherStreamEncrypter.h',
        'autil/cipher/AESCipherUtility.h', 'autil/cipher/ICipherStream.h',
        'autil/cipher/MemoryDataPipeline.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':base64', ':env_util', ':lock', ':log', '@boringssl//:crypto',
        '@boringssl//:ssl'
    ],
    alwayslink=True
)
cc_library(
    name='mem_pool_base',
    srcs=[
        'autil/mem_pool/Pool.cpp', 'autil/mem_pool/RecyclePool.cpp',
        'autil/mem_pool/SimpleAllocatePolicy.cpp',
        'autil/mem_pool/SimpleAllocator.cpp'
    ],
    hdrs=[
        'autil/mem_pool/AllocatePolicy.h',
        'autil/mem_pool/ChunkAllocatorBase.h',
        'autil/mem_pool/MemPoolFactory.h', 'autil/mem_pool/MemoryChunk.h',
        'autil/mem_pool/Pool.h', 'autil/mem_pool/PoolBase.h',
        'autil/mem_pool/RecyclePool.h', 'autil/mem_pool/SimpleAllocatePolicy.h',
        'autil/mem_pool/SimpleAllocator.h', 'autil/mem_pool/SubPoolAllocator.h',
        'autil/mem_pool/pool_allocator.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':common_macros', ':lock', ':log', ':object_tracer', ':sanitizer_util'
    ],
    alwayslink=True
)
cc_library(
    name='allocator',
    srcs=[
        'autil/FixedSizeChunkAllocator.cpp',
        'autil/SimpleSegregatedAllocator.cpp'
    ],
    hdrs=[
        'autil/ChunkAllocator.h', 'autil/FixedSizeChunkAllocator.h',
        'autil/SimpleSegregatedAllocator.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':log'],
    alwayslink=True
)
cc_library(
    name='mem_pool_container',
    srcs=['autil/mem_pool/PoolString.cpp'],
    hdrs=[
        'autil/mem_pool/Map.h', 'autil/mem_pool/PoolMap.h',
        'autil/mem_pool/PoolString.h', 'autil/mem_pool/PoolVector.h',
        'autil/mem_pool/Set.h', 'autil/mem_pool/Vector.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':mem_pool_base'],
    alwayslink=True
)
cc_library(
    name='string_type',
    srcs=['autil/ConstString.cpp', 'autil/ShortString.cpp'],
    hdrs=['autil/ConstString.h', 'autil/ShortString.h'],
    implementation_deps=[':lock'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':log', ':span'],
    alwayslink=True
)
cc_library(
    name='const_string_util',
    srcs=['autil/ConstStringUtil.cpp'],
    hdrs=['autil/ConstStringUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', ':multi_value_helper', ':string_helper', ':string_type'],
    alwayslink=True
)
cc_library(
    name='fmt_string_util',
    hdrs=['autil/FmtStringUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=['@fmt'],
    alwayslink=True
)
cc_library(
    name='string_helper',
    srcs=[
        'autil/FormatInt.cpp', 'autil/StringConvertor.cpp',
        'autil/StringTokenizer.cpp', 'autil/StringUtil.cpp'
    ],
    hdrs=[
        'autil/FormatInt.h', 'autil/StringConvertor.h',
        'autil/StringTokenizer.h', 'autil/StringUtil.h'
    ],
    implementation_deps=['@rapidjson//:rapidjson', ':mem_pool_base'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':string_type'],
    alwayslink=True
)
cc_library(
    name='numbers_helper',
    srcs=[],
    hdrs=['autil/NumbersUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        '@com_google_absl//absl/strings',
        '@com_google_absl//absl/strings:internal'
    ],
    alwayslink=True
)
cc_library(
    name='unit_util',
    srcs=['autil/UnitUtil.cpp'],
    hdrs=['autil/UnitUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':string_helper'],
    alwayslink=True
)
cc_library(
    name='long_hash_value',
    srcs=['autil/LongHashValue.cpp'],
    hdrs=['autil/LongHashValue.h'],
    implementation_deps=[':string_helper'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='pack_data',
    srcs=['autil/PackDataFormatter.cpp'],
    hdrs=['autil/PackDataFormatter.h', 'autil/PackDataReader.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':common_macros', ':multi_value'],
    alwayslink=True
)
cc_library(
    name='multi_value',
    srcs=['autil/MultiValueType.cpp'],
    hdrs=['autil/MultiValueFormatter.h', 'autil/MultiValueType.h'],
    implementation_deps=[':string_helper'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[
        ':common_macros', ':data_buffer', ':hash_base', ':long_hash_value',
        ':span'
    ],
    alwayslink=True
)
cc_library(
    name='hyper_loglog',
    srcs=['autil/Hyperloglog.cpp'],
    hdrs=['autil/Hyperloglog.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':data_buffer'],
    alwayslink=True
)
cc_library(
    name='data_buffer',
    srcs=['autil/DataBuffer.cpp'],
    hdrs=['autil/DataBuffer.h', 'autil/SerializableTypeTraits.h'],
    implementation_deps=[':hash_base'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':exception', ':long_hash_value', ':mem_pool_base', ':string_type'],
    alwayslink=True
)
cc_library(
    name='multi_value_helper',
    srcs=['autil/MultiValueCreator.cpp'],
    hdrs=['autil/MultiValueCreator.h', 'autil/MultiValueWriter.h'],
    implementation_deps=[':hash_base', ':exception'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':mem_pool_base', ':multi_value', ':string_type'],
    alwayslink=True
)
cc_library(
    name='option_parser',
    srcs=['autil/CommandLineParameter.cpp', 'autil/OptionParser.cpp'],
    hdrs=['autil/CommandLineParameter.h', 'autil/OptionParser.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':string_helper'],
    alwayslink=True
)
cc_library(
    name='cityhash',
    srcs=['autil/cityhash/city.cc'],
    hdrs=[
        'autil/cityhash/city.h', 'autil/cityhash/citycrc.h',
        'autil/cityhash/config.h'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='plugin_base',
    srcs=['autil/plugin/InterfaceManager.cpp'],
    hdrs=['autil/plugin/BaseInterface.h', 'autil/plugin/InterfaceManager.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log'],
    alwayslink=True
)
cc_library(
    name='hash_base',
    srcs=['autil/HashAlgorithm.cpp'],
    hdrs=['autil/HashAlgorithm.h', 'autil/HashUtil.h'],
    implementation_deps=[':common_macros'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':cityhash', ':long_hash_value'],
    alwayslink=True
)
cc_library(
    name='hash_func',
    srcs=[
        'autil/DefaultHashFunction.cpp', 'autil/ExtendHashFunction.cpp',
        'autil/Hash64Function.cpp', 'autil/HashFuncFactory.cpp',
        'autil/HashFunctionBase.cpp', 'autil/KsHashFunction.cpp',
        'autil/ModHashFunction.cpp', 'autil/MurmurHash3Function.cpp',
        'autil/NumberHashFunction.cpp', 'autil/RoutingHashFunction.cpp',
        'autil/SmartWaveHashFunction.cpp', 'autil/legacy/md5.cpp'
    ],
    hdrs=[
        'autil/DefaultHashFunction.h', 'autil/ExtendHashFunction.h',
        'autil/Hash64Function.h', 'autil/HashFuncFactory.h',
        'autil/HashFunction.h', 'autil/HashFunctionBase.h',
        'autil/KeyRangeManager.h', 'autil/KsHashFunction.h',
        'autil/ModHashFunction.h', 'autil/MurmurHash3Function.h',
        'autil/NumberHashFunction.h', 'autil/RoutingHashFunction.h',
        'autil/SmartWaveHashFunction.h', 'autil/legacy/md5.h'
    ],
    implementation_deps=['string_type'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':hash_base', ':log', ':murmur_hash3', ':string_helper'],
    alwayslink=True
)
cc_library(
    name='murmur_hash',
    srcs=['autil/MurmurHash.cpp'],
    hdrs=['autil/MurmurHash.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='murmur_hash3',
    srcs=['autil/MurmurHash3.cpp'],
    hdrs=['autil/MurmurHash3.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='NoCopyable',
    hdrs=['autil/NoCopyable.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='span',
    hdrs=['autil/Span.h', 'autil/StringView.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='demangle',
    hdrs=['autil/Demangle.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='thread_annotations',
    hdrs=['autil/ThreadAnnotations.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='result',
    srcs=glob(['autil/result/*.cpp']),
    hdrs=glob(['autil/result/*.h']),
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':NoCopyable', ':common_macros', ':demangle']
)
cc_library(
    name='net',
    srcs=['autil/NetUtil.cpp'],
    hdrs=['autil/NetUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=['string_helper', ':lock', ':log', ':time']
)
cc_library(
    name='mem_util',
    srcs=['autil/MemUtil.cpp'],
    hdrs=['autil/MemUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', ':string_helper']
)
cc_library(
    name='system_util',
    srcs=['autil/SystemUtil.cpp'],
    hdrs=['autil/SystemUtil.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', ':string_helper', '//aios/filesystem/fslib:fslib-framework']
)
cc_library(
    name='hash_map',
    hdrs=['autil/BytellHashmap.h', 'autil/FlatHashmap.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='backtrace',
    hdrs=['autil/Backtrace.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[]
)
cc_library(
    name='file_recorder',
    srcs=['autil/FileRecorder.cpp'],
    hdrs=['autil/FileRecorder.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':string_helper', ':time', '//aios/filesystem/fslib:fslib-framework']
)
cc_library(
    name='lzma',
    srcs=['autil/LzmaCompressor.cpp'],
    hdrs=['autil/LzmaCompressor.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':log', '//third_party/lzma'],
    alwayslink=True
)
cc_library(
    name='singleton',
    hdrs=['autil/Singleton.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':lock'],
    alwayslink=True
)
cc_library(
    name='resource_pool',
    hdrs=['autil/ResourcePool.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='resource_container',
    hdrs=['autil/ResourceContainer.h'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    deps=[':lock'],
    alwayslink=True
)
