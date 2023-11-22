package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='table_service',
    srcs=['Service.proto'],
    import_prefix='suez/service',
    visibility=['//visibility:public'],
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='control_service',
    srcs=['SuezControl.proto'],
    import_prefix='suez/service',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='suez_kv_batch_get_generator',
    srcs=['KVBatchGetGenerator.cpp'],
    hdrs=['KVBatchGetGenerator.h'],
    include_prefix='suez/service',
    visibility=['//visibility:public'],
    deps=[
        ':table_service_cc_proto', '//aios/network/gig:multi_call',
        '@com_google_absl//absl/container:flat_hash_set'
    ]
)
cc_library(
    name='builtin_service',
    srcs=glob(['*.cpp'], exclude=['KVBatchGetGenerator.cpp']),
    hdrs=glob(['*.h'], exclude=['KVBatchGetGenerator.h']),
    include_prefix='suez/service',
    deps=([
        ':control_service_cc_proto', ':table_service_cc_proto',
        '//aios/autil:closure_guard', '//aios/autil:log',
        '//aios/storage/indexlib/indexlib/tools:index_partition_querier_lib',
        '//aios/storage/indexlib/table:builtin_define', '//aios/suez/common',
        '//aios/suez/sdk', '//aios/suez/sdk:remote_table_writer',
        '//aios/suez/sdk:table_writer', '//aios/table',
        '@com_google_absl//absl/container:flat_hash_map'
    ] + []),
    alwayslink=1
)
