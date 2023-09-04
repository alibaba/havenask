package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='killself_service',
    srcs=['KillSelf.proto'],
    import_prefix='suez/worker',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='suez_worker_lib',
    srcs=(
        glob(['*.cpp'], exclude=['SapServerWorkerHandlerFactoryAdapter.cpp']) +
        []
    ),
    hdrs=(
        glob(['*.h'], exclude=['SapServerWorkerHandlerFactoryAdapter.h']) + []
    ),
    include_prefix='suez/worker',
    deps=([
        ':killself_service_cc_proto',
        '//aios/suez/heartbeat:debug_proto_cc_proto', '//aios/autil:env_util',
        '//aios/autil:hash_func', '//aios/autil:mem_util',
        '//aios/autil:object_tracer', '//aios/autil:thread',
        '//aios/suez/sdk:sdk', '//aios/suez/sdk:remote_table_writer',
        '//aios/suez/heartbeat:heartbeat', '//aios/suez/search:search',
        '//aios/suez/table:table'
    ] + ['//aios/future_lite/future_lite/executors:simple_async_io_executor']),
    alwayslink=1
)
