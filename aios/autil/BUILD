package(default_visibility=['//aios/autil:__subpackages__'])
load('//bazel:defs.bzl', 'gen_conf_lib')
cc_library(
    name='synchronized_queue',
    hdrs=['autil/SynchronizedQueue.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='scope',
    hdrs=['autil/Scope.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='defer',
    hdrs=['autil/Defer.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='closure_guard',
    hdrs=['autil/ClosureGuard.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='memory',
    hdrs=['autil/memory.h', 'autil/SharedPtrUtil.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='shared_object_map',
    srcs=['autil/SharedObjectMap.cpp'],
    hdrs=['autil/SharedObjectMap.h'],
    deps=[':lock', ':mem_pool_base'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='object_tracer',
    hdrs=['autil/ObjectTracer.h'],
    deps=[':lock'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='bloom_filter',
    srcs=['autil/BloomFilter.cpp'],
    hdrs=['autil/BloomFilter.h'],
    deps=[':hash_func', ':base64'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='common_macros',
    hdrs=['autil/CommonMacros.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='stack_tracer',
    srcs=['autil/StackTracer.cpp'],
    hdrs=['autil/StackTracer.h'],
    deps=[':lock'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='sanitizer_util',
    hdrs=['autil/SanitizerUtil.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='range_util',
    srcs=['autil/RangeUtil.cpp'],
    hdrs=['autil/RangeUtil.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='object_allocator',
    hdrs=['autil/ObjectAllocator.h', 'autil/FixedSizeAllocator.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='url_util',
    srcs=['autil/UrlDecoder.cpp', 'autil/UrlEncode.cpp', 'autil/URLUtil.cpp'],
    hdrs=['autil/UrlDecoder.h', 'autil/UrlEncode.h', 'autil/URLUtil.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='regex',
    srcs=['autil/Regex.cpp'],
    hdrs=['autil/Regex.h'],
    deps=[':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='dynamic_buf',
    hdrs=['autil/DynamicBuf.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='zlib',
    srcs=['autil/ZlibCompressor.cpp'],
    hdrs=['autil/ZlibCompressor.h'],
    deps=[':log', ':dynamic_buf'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='compression',
    srcs=['autil/CompressionUtil.cpp'],
    hdrs=['autil/CompressionUtil.h'],
    deps=[':mem_pool_base', ':zlib', '//third_party/lz4', '@snappy//:snappy'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='base64',
    srcs=['autil/legacy/base64.cpp'],
    hdrs=['autil/legacy/base64.h'],
    deps=[':exception', ':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='time_span',
    srcs=['autil/TimeSpan.cpp'],
    hdrs=['autil/TimeSpan.h', 'autil/Libdivide.h'],
    deps=[':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='snapshot_vector',
    hdrs=['autil/SnapshotVector.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='daily_run_mode',
    srcs=['autil/DailyRunMode.cpp'],
    hdrs=['autil/DailyRunMode.h'],
    deps=[':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='env_util',
    srcs=['autil/EnvUtil.cpp'],
    hdrs=['autil/EnvUtil.h'],
    deps=[':string_helper', ':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='diagnostic',
    hdrs=['autil/Diagnostic.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='containers',
    hdrs=['autil/Autovector.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='cache2',
    srcs=[],
    hdrs=[
        'autil/Cache.h', 'autil/LruCache.h', 'autil/HashUtil.h',
        'autil/SynchronizedHashTable.h', 'autil/SynchronizedList.h'
    ],
    deps=([':log', ':lock', ':string_type', ':cityhash'] + []),
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='cache',
    hdrs=[
        'autil/cache/cache.h', 'autil/cache/cache_wrapper.h',
        'autil/cache/cache_allocator.h', 'autil/cache/sharded_cache.h',
        'autil/cache/lru_cache.h'
    ],
    srcs=[
        'autil/cache/cache_wrapper.cpp', 'autil/cache/cache_hash.cpp',
        'autil/cache/cache_hash.h', 'autil/cache/sharded_cache.cpp',
        'autil/cache/lru_cache.cpp'
    ],
    deps=([':containers', ':lock', ':string_type', '//third_party/tbb'] + []),
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='lock',
    srcs=['autil/Lock.cpp'],
    hdrs=['autil/Lock.h'],
    deps=[':common_macros', ':time', ':log', ':thread_annotations'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='tfThreadPool',
    srcs=['autil/TfThreadPool.cpp'],
    hdrs=['autil/TfThreadPool.h'],
    deps=[':thread', '@org_tensorflow//tensorflow/core:lib', ':NoCopyable'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public']
)
cc_library(
    name='thread',
    srcs=[
        'autil/LoopThread.cpp', 'autil/OutputOrderedThreadPool.cpp',
        'autil/Thread.cpp', 'autil/ThreadLocal.cpp', 'autil/ThreadPool.cpp',
        'autil/ThreadPoolManager.cpp'
    ],
    hdrs=[
        'autil/CircularQueue.h', 'autil/LoopThread.h',
        'autil/OutputOrderedThreadPool.h', 'autil/Thread.h',
        'autil/ThreadLocal.h', 'autil/ThreadPool.h',
        'autil/ThreadPoolManager.h', 'autil/WorkItem.h',
        'autil/LambdaWorkItem.h', 'autil/AtomicCounter.h',
        'autil/FixedSizeRecyclePool.h', 'autil/WorkItemQueue.h'
    ],
    deps=[
        ':NoCopyable', ':string_helper', ':daily_run_mode', ':containers',
        ':log', ':lock', ':exception', ':thread_name_scope', ':sanitizer_util'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
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
    srcs=['autil/ThreadAutoScaler.cpp', 'autil/LockFreeThreadPool.cpp'],
    hdrs=[
        'autil/LockFree.h', 'autil/LockFreeQueue.h', 'autil/LockFreeStack.h',
        'autil/ThreadAutoScaler.h', 'autil/LockFreeThreadPool.h'
    ],
    deps=[':thread'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='block_cache',
    srcs=[
        'autil/Block.cpp', 'autil/BlockCache.cpp', 'autil/BlockPool.cpp',
        'autil/MmapBlockAllocator.cpp', 'autil/SimpleBlockAllocator.cpp',
        'autil/LRUReplacePolicy.cpp'
    ],
    hdrs=[
        'autil/Allocators.h', 'autil/Block.h', 'autil/BlockAllocator.h',
        'autil/BlockCache.h', 'autil/BlockLinkListNode.h', 'autil/BlockPool.h',
        'autil/MmapBlockAllocator.h', 'autil/SimpleBlockAllocator.h',
        'autil/ReferencedHashMap.h', 'autil/FixedSizeAllocator.h',
        'autil/FixedSizeRecyclePool.h', 'autil/ReplacePolicy.h',
        'autil/LRUReplacePolicy.h', 'autil/DoubleLinkList.h',
        'autil/DoubleLinkListNode.h'
    ],
    deps=[':common_macros', ':log', ':lock', ':sanitizer_util', ':mem_util'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='codec',
    srcs=glob(['autil/codec/*.cpp']),
    hdrs=glob(['autil/codec/*.h']),
    deps=[
        ':string_type', ':common_macros', ':log', ':json', '//aios/alog:alog'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='metric',
    srcs=glob(['autil/metric/*.cpp']),
    hdrs=glob(['autil/metric/*.h']),
    deps=[':time'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
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
    deps=[':time', ':common_macros'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='bitmap',
    srcs=['autil/bitmap.cpp'],
    hdrs=['autil/bitmap.h'],
    deps=[':log', ':time', ':mem_pool_container'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='json_helper',
    srcs=['autil/legacy/RapidJsonHelper.cpp'],
    hdrs=['autil/legacy/RapidJsonCommon.h', 'autil/legacy/RapidJsonHelper.h'],
    deps=[
        '@rapidjson//:rapidjson', ':multi_value', ':multi_value_helper', ':log'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='json_base',
    srcs=[
        'autil/legacy/json.cpp', 'autil/legacy/astream.cpp',
        'autil/legacy/string_utf8.cpp', 'autil/legacy/string_conversion.cpp',
        'autil/legacy/json_string_enhanced.cpp'
    ],
    hdrs=[
        'autil/legacy/any.h', 'autil/legacy/jsonizable_exception.h',
        'autil/legacy/json.h', 'autil/legacy/string_tools.h',
        'autil/legacy/astream.h'
    ],
    deps=[':string_helper', ':exception'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='fast_json',
    srcs=['autil/legacy/fast_jsonizable.cpp'],
    hdrs=[
        'autil/legacy/fast_jsonizable.h', 'autil/legacy/fast_jsonizable_dec.h'
    ],
    deps=['@rapidjson//:rapidjson', ':json_base', ':multi_value'],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='json',
    hdrs=[
        'autil/legacy/legacy_jsonizable.h',
        'autil/legacy/legacy_jsonizable_dec.h', 'autil/legacy/jsonizable.h'
    ],
    deps=[
        '@rapidjson//:rapidjson', ':multi_value', ':multi_value_helper',
        ':json_base', ':fast_json'
    ],
    include_prefix='autil',
    strip_include_prefix='autil',
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='log',
    hdrs=['autil/Log.h'],
    deps=[':time', '//aios/alog:alog'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='exception',
    srcs=['autil/legacy/exception.cpp'],
    hdrs=['autil/legacy/exception.h'],
    deps=[':backtrace'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='cipher',
    srcs=[
        'autil/cipher/AESCipherUtility.cpp',
        'autil/cipher/AESCipherEncrypter.cpp',
        'autil/cipher/AESCipherDecrypter.cpp',
        'autil/cipher/AESCipherStreamEncrypter.cpp',
        'autil/cipher/AESCipherStreamDecrypter.cpp',
        'autil/cipher/AESCipherCreator.cpp', 'autil/cipher/AESCipherCommon.cpp',
        'autil/cipher/MemoryDataPipeline.cpp'
    ],
    hdrs=[
        'autil/cipher/AESCipherUtility.h', 'autil/cipher/AESCipherEncrypter.h',
        'autil/cipher/AESCipherDecrypter.h',
        'autil/cipher/AESCipherStreamEncrypter.h',
        'autil/cipher/AESCipherStreamDecrypter.h',
        'autil/cipher/AESCipherCreator.h', 'autil/cipher/AESCipherCommon.h',
        'autil/cipher/MemoryDataPipeline.h'
    ],
    deps=[
        ':log', ':lock', ':base64', ':env_util', '@boringssl//:crypto',
        '@boringssl//:ssl'
    ],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
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
        'autil/mem_pool/ChunkAllocatorBase.h', 'autil/mem_pool/MemoryChunk.h',
        'autil/mem_pool/Pool.h', 'autil/mem_pool/PoolBase.h',
        'autil/mem_pool/RecyclePool.h', 'autil/mem_pool/SimpleAllocatePolicy.h',
        'autil/mem_pool/SimpleAllocator.h', 'autil/mem_pool/SubPoolAllocator.h',
        'autil/mem_pool/pool_allocator.h'
    ],
    deps=[':log', ':lock', ':common_macros', ':sanitizer_util'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='allocator',
    srcs=[
        'autil/SimpleSegregatedAllocator.cpp',
        'autil/FixedSizeChunkAllocator.cpp'
    ],
    hdrs=[
        'autil/SimpleSegregatedAllocator.h', 'autil/FixedSizeChunkAllocator.h',
        'autil/ChunkAllocator.h'
    ],
    deps=[':log', ':common_macros'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='mem_pool_container',
    srcs=['autil/mem_pool/PoolString.cpp'],
    hdrs=[
        'autil/mem_pool/PoolMap.h', 'autil/mem_pool/PoolString.h',
        'autil/mem_pool/PoolVector.h', 'autil/mem_pool/Set.h',
        'autil/mem_pool/Vector.h', 'autil/mem_pool/Map.h'
    ],
    deps=[':mem_pool_base'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='string_type',
    srcs=['autil/ShortString.cpp', 'autil/ConstString.cpp'],
    hdrs=['autil/ShortString.h', 'autil/ConstString.h'],
    deps=[':log', ':common_macros', ':span'],
    implementation_deps=[':lock'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='const_string_util',
    srcs=['autil/ConstStringUtil.cpp'],
    hdrs=['autil/ConstStringUtil.h'],
    deps=[':string_helper', ':string_type', ':multi_value_helper', ':log'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='fmt_string_util',
    hdrs=['autil/FmtStringUtil.h'],
    deps=['@fmt'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='string_helper',
    srcs=[
        'autil/StringUtil.cpp', 'autil/FormatInt.cpp',
        'autil/StringConvertor.cpp', 'autil/StringTokenizer.cpp'
    ],
    hdrs=[
        'autil/StringUtil.h', 'autil/FormatInt.h', 'autil/StringConvertor.h',
        'autil/StringTokenizer.h'
    ],
    deps=[':string_type'],
    implementation_deps=['@rapidjson//:rapidjson', ':mem_pool_base'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='numbers_helper',
    srcs=[],
    hdrs=['autil/NumbersUtil.h'],
    deps=[
        '@com_google_absl//absl/strings:internal',
        '@com_google_absl//absl/strings:strings'
    ],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='unit_util',
    srcs=['autil/UnitUtil.cpp'],
    hdrs=['autil/UnitUtil.h'],
    deps=[':string_helper'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='long_hash_value',
    srcs=['autil/LongHashValue.cpp'],
    hdrs=['autil/LongHashValue.h'],
    implementation_deps=[':string_helper'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='pack_data',
    srcs=['autil/PackDataFormatter.cpp'],
    hdrs=['autil/PackDataReader.h', 'autil/PackDataFormatter.h'],
    deps=[':common_macros', ':multi_value'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='multi_value',
    srcs=['autil/MultiValueType.cpp'],
    hdrs=['autil/MultiValueType.h', 'autil/MultiValueFormatter.h'],
    deps=[':long_hash_value', ':common_macros', ':hash_base', ':span'],
    implementation_deps=[':string_helper'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='hyper_loglog',
    srcs=['autil/Hyperloglog.cpp'],
    hdrs=['autil/Hyperloglog.h'],
    deps=[':data_buffer'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='data_buffer',
    srcs=['autil/DataBuffer.cpp'],
    hdrs=['autil/DataBuffer.h'],
    deps=[
        ':exception', ':mem_pool_base', ':long_hash_value', ':string_type',
        ':multi_value'
    ],
    implementation_deps=[':hash_base'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='multi_value_helper',
    srcs=['autil/MultiValueCreator.cpp'],
    hdrs=['autil/MultiValueCreator.h', 'autil/MultiValueWriter.h'],
    deps=[':mem_pool_base', ':string_type', ':multi_value'],
    implementation_deps=[':hash_base', ':exception'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='option_parser',
    srcs=['autil/OptionParser.cpp', 'autil/CommandLineParameter.cpp'],
    hdrs=['autil/OptionParser.h', 'autil/CommandLineParameter.h'],
    deps=[':string_helper'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='cityhash',
    srcs=['autil/cityhash/city.cc'],
    hdrs=[
        'autil/cityhash/city.h', 'autil/cityhash/citycrc.h',
        'autil/cityhash/config.h'
    ],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='plugin_base',
    srcs=['autil/plugin/InterfaceManager.cpp'],
    hdrs=['autil/plugin/InterfaceManager.h', 'autil/plugin/BaseInterface.h'],
    deps=[':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil',
    alwayslink=True
)
cc_library(
    name='hash_base',
    srcs=['autil/HashAlgorithm.cpp'],
    hdrs=['autil/HashAlgorithm.h', 'autil/HashUtil.h'],
    deps=[':long_hash_value', ':cityhash'],
    implementation_deps=[':common_macros'],
    alwayslink=True,
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='hash_func',
    srcs=[
        'autil/HashFunctionBase.cpp', 'autil/DefaultHashFunction.cpp',
        'autil/HashFuncFactory.cpp', 'autil/ModHashFunction.cpp',
        'autil/KsHashFunction.cpp', 'autil/ExtendHashFunction.cpp',
        'autil/NumberHashFunction.cpp', 'autil/Hash64Function.cpp',
        'autil/RoutingHashFunction.cpp', 'autil/legacy/md5.cpp'
    ],
    hdrs=[
        'autil/HashFunctionBase.h', 'autil/KsHashFunction.h',
        'autil/ModHashFunction.h', 'autil/DefaultHashFunction.h',
        'autil/NumberHashFunction.h', 'autil/HashFunction.h',
        'autil/Hash64Function.h', 'autil/HashFuncFactory.h',
        'autil/RoutingHashFunction.h', 'autil/ExtendHashFunction.h',
        'autil/KeyRangeManager.h', 'autil/legacy/md5.h'
    ],
    deps=[':hash_base', ':string_helper', ':log'],
    alwayslink=True,
    implementation_deps=['string_type'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='murmur_hash',
    srcs=['autil/MurmurHash.cpp'],
    hdrs=['autil/MurmurHash.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='NoCopyable',
    hdrs=['autil/NoCopyable.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='span',
    hdrs=['autil/Span.h', 'autil/StringView.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='demangle',
    hdrs=['autil/Demangle.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='thread_annotations',
    hdrs=['autil/ThreadAnnotations.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='result',
    hdrs=glob(['autil/result/*.h']),
    srcs=glob(['autil/result/*.cpp']),
    deps=[':NoCopyable', ':common_macros', ':demangle'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='process_mutex',
    hdrs=['autil/ProcessMutex.h'],
    srcs=['autil/ProcessMutex.cpp'],
    deps=[':result', ':log'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='net',
    srcs=['autil/NetUtil.cpp'],
    hdrs=['autil/NetUtil.h'],
    deps=[':log', ':time', ':lock', 'string_helper'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='mem_util',
    srcs=['autil/MemUtil.cpp'],
    hdrs=['autil/MemUtil.h'],
    deps=[':log', ':string_helper'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='hash_map',
    hdrs=['autil/FlatHashmap.h', 'autil/BytellHashmap.h'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='backtrace',
    hdrs=['autil/Backtrace.h'],
    deps=[],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
cc_library(
    name='file_recorder',
    srcs=['autil/FileRecorder.cpp'],
    hdrs=['autil/FileRecorder.h'],
    deps=[':time', ':string_helper', '//aios/filesystem/fslib:fslib-framework'],
    visibility=['//visibility:public'],
    include_prefix='autil',
    strip_include_prefix='autil'
)
