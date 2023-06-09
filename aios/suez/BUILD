package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:bundle.bzl', 'bundle_files', 'bundle_tar')
cc_proto(
    name='admin_proto',
    srcs=glob(['suez/admin/Admin.proto']),
    import_prefix='suez/admin',
    strip_import_prefix='suez/admin',
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_proto(
    name='table_service',
    srcs=glob(['suez/service/Service.proto']),
    import_prefix='suez',
    strip_import_prefix='suez',
    visibility=['//visibility:public'],
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='control_service',
    srcs=glob(['suez/service/Control.proto']),
    import_prefix='suez',
    strip_import_prefix='suez',
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='suez_heartbeat',
    srcs=glob(['suez/heartbeat/*.proto']),
    import_prefix='suez/heartbeat',
    strip_import_prefix='suez/heartbeat',
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='suez_debug_service',
    srcs=glob(['suez/worker/Debug.proto']),
    import_prefix='suez/worker',
    strip_import_prefix='suez/worker',
    deps=[':suez_heartbeat', '//aios/network/arpc:proto']
)
cc_proto(
    name='suez_selfkiller_service',
    srcs=glob(['suez/worker/KillSelf.proto']),
    import_prefix='suez/worker',
    strip_import_prefix='suez/worker',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='suez_inner_sdk',
    srcs=glob(['suez/sdk/*.cpp'],
              exclude=[
                  'suez/sdk/SearchManager.cpp', 'suez/sdk/TableWriter.cpp',
                  'suez/sdk/RemoteTableWriter.cpp',
                  'suez/sdk/RemoteTableWriterRequestGenerator.cpp',
                  'suez/sdk/RemoteTableWriterClosure.cpp'
              ]),
    hdrs=glob(['suez/sdk/*.h'],
              exclude=[
                  'suez/sdk/SearchManager.h', 'suez/sdk/TableWriter.h',
                  'suez/sdk/WriteResult.h', 'suez/sdk/RemoteTableWriterParam.h',
                  'suez/sdk/RemoteTableWriter.h',
                  'suez/sdk/RemoteTableWriterRequestGenerator.h',
                  'suez/sdk/RemoteTableWriterClosure.h'
              ]),
    implementation_deps=['//aios/apps/facility/build_service:bs_workflow'],
    include_prefix='suez',
    strip_include_prefix='suez',
    deps=[
        '//aios/autil:json', '//aios/autil:lock', '//aios/autil:log',
        '//aios/autil:range_util', '//aios/autil:string_helper',
        '//aios/network/arpc', '//aios/network/gig:multi_call',
        '//aios/network/http_arpc', '//aios/storage/indexlib:interface',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '@com_google_protobuf//:protobuf'
    ]
)
cc_library(
    name='suez_search_sdk',
    srcs=['suez/sdk/SearchManager.cpp'],
    hdrs=['suez/sdk/SearchManager.h'],
    include_prefix='suez',
    strip_include_prefix='suez',
    visibility=['//visibility:public'],
    deps=([
        ':suez_inner_sdk', '//aios/apps/facility/build_service:bs_workflow',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog'
    ] + ['//aios/sap_easy:sap_easy_util']),
    alwayslink=1
)
cc_library(
    name='table_wal',
    srcs=glob(['suez/table/wal/*.cpp', 'suez/table/wal/*.h']),
    hdrs=[
        'suez/table/wal/GlobalQueueManager.h', 'suez/table/wal/QueueWAL.h',
        'suez/table/wal/RealtimeSwiftWAL.h', 'suez/table/wal/WALConfig.h',
        'suez/table/wal/WALStrategy.h'
    ],
    include_prefix='suez',
    strip_include_prefix='suez',
    deps=[
        '//aios/apps/facility/build_service:bs_config', '@swift//:swift',
        '//aios/autil:log', '//aios/autil:result', '//aios/autil:thread'
    ]
)
cc_library(
    name='table_writer_sdk',
    srcs=[
        'suez/sdk/RawDocument2SwiftFieldFilter.cpp',
        'suez/sdk/RawDocument2SwiftFieldFilter.h', 'suez/sdk/TableWriter.cpp'
    ],
    hdrs=['suez/sdk/TableWriter.h', 'suez/sdk/WriteResult.h'],
    include_prefix='suez',
    strip_include_prefix='suez',
    visibility=['//visibility:public'],
    deps=[
        ':inner_def', ':suez_inner_sdk', ':table_wal',
        '//aios/apps/facility/build_service:bs_workflow', '//aios/autil:result',
        '//aios/autil:scope', '//aios/autil:time'
    ]
)
cc_library(
    name='remote_table_writer_sdk',
    srcs=[
        'suez/sdk/RemoteTableWriter.cpp',
        'suez/sdk/RemoteTableWriterClosure.cpp',
        'suez/sdk/RemoteTableWriterRequestGenerator.cpp'
    ],
    hdrs=[
        'suez/sdk/RemoteTableWriter.h', 'suez/sdk/RemoteTableWriterClosure.h',
        'suez/sdk/RemoteTableWriterParam.h',
        'suez/sdk/RemoteTableWriterRequestGenerator.h'
    ],
    implementation_deps=[
        '//aios/network/gig:multi_call', '//aios/autil:range_util',
        ':table_service_cc_proto'
    ],
    include_prefix='suez',
    strip_include_prefix='suez',
    visibility=['//visibility:public'],
    deps=[':suez_common', '//aios/autil:NoCopyable', '//aios/autil:result']
)
cc_library(
    name='suez_common',
    srcs=glob(['suez/common/*.cpp']),
    hdrs=glob(['suez/common/*.h']),
    include_prefix='suez',
    strip_include_prefix='suez',
    deps=[
        ':suez_inner_sdk', '//aios/autil:file_recorder', '//aios/autil:time',
        '//aios/worker_framework'
    ]
)
cc_library(
    name='suez_service_impl',
    srcs=glob(['suez/service/*.cpp', 'suez/service/table/*.cpp'],
              exclude=['suez/service/KVBatchGetGenerator.cpp']),
    hdrs=glob(['suez/service/*.h', 'suez/service/table/*.h'],
              exclude=['suez/service/KVBatchGetGenerator.h']),
    include_prefix='suez',
    strip_include_prefix='suez',
    deps=([
        ':control_service_cc_proto', ':remote_table_writer_sdk', ':suez_common',
        ':suez_heartbeat_cc_proto', ':suez_search_sdk',
        ':table_service_cc_proto', ':table_writer_sdk',
        '//aios/autil:closure_guard', '//aios/autil:log',
        '//aios/storage/indexlib/table:builtin_define',
        '//aios/storage/indexlib/table/aggregation:table',
        '//aios/storage/indexlib/legacy/tools:index_partition_querier_lib',
        '//aios/table', '@com_google_absl//absl/container:flat_hash_map'
    ] + []),
    alwayslink=1
)
cc_library(
    name='inner_def',
    srcs=['suez/table/InnerDef.cpp'],
    hdrs=['suez/sdk/Magic.h', 'suez/table/InnerDef.h'],
    include_prefix='suez',
    strip_include_prefix='suez'
)
cc_library(
    name='suez_worker',
    srcs=(
        glob([
            'suez/heartbeat/*.cpp', 'suez/search/*.cpp', 'suez/deploy/*.cpp',
            'suez/table/*.cpp'
        ]) +
        glob(['suez/worker/*.cpp'],
             exclude=['suez/worker/SapServerWorkerHandlerFactoryAdapter.cpp'])
    ),
    hdrs=(
        glob([
            'suez/table/*.h', 'suez/deploy/*.h', 'suez/search/*.h',
            'suepz/worker/*.h', 'suez/heartbeat/*.h'
        ]) +
        glob(['suez/worker/*.h'],
             exclude=['suez/worker/SapServerWorkerHandlerFactoryAdapter.h'])
    ),
    include_prefix='suez',
    strip_include_prefix='suez',
    visibility=[
        '//aios/ha3:__pkg__', '//aios/igraph:__pkg__', '//abfs_turing:__pkg__',
        '//be_turing:__pkg__', '//d2_framework:__pkg__', '//rtp_turing:__pkg__',
        '//torch_turing:__pkg__', '//aios/apps/engine/dolphin:__pkg__',
        '//aios/suez:__subpackages__',
        '//integration_test/storage_local:__subpackages__',
        '//integration_test/suez:__subpackages__',
        '//aios/ha3/ha3/turing/common/test:__subpackages__'
    ],
    deps=([
        ':remote_table_writer_sdk', ':suez_debug_service_cc_proto',
        ':suez_selfkiller_service_cc_proto', ':suez_service_impl',
        ':table_writer_sdk', '//aios/apps/facility/build_service:bs_merge',
        '//aios/apps/facility/build_service:bs_workflow',
        '//aios/autil:NoCopyable', '//aios/autil:env_util',
        '//aios/autil:hash_func', '//aios/autil:mem_util',
        '//aios/autil:object_tracer', '//aios/autil:thread',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/network/gig:multi_call', '//aios/storage/indexlib:tablet',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/indexlib/index:kv',
        '//aios/storage/indexlib/table:all',
        '//aios/storage/indexlib/table/index_task:merge_task',
        '//aios/suez/suez/drc:suez_drc'
    ] + ['//aios/future_lite/future_lite/executors:simple_async_io_executor']),
    alwayslink=1
)
cc_library(
    name='suez_kv_batch_get_generator',
    srcs=['suez/service/KVBatchGetGenerator.cpp'],
    hdrs=['suez/service/KVBatchGetGenerator.h'],
    visibility=['//visibility:public'],
    deps=[
        ':table_service_cc_proto', '//aios/network/gig:multi_call',
        '@com_google_absl//absl/container:flat_hash_set'
    ]
)
