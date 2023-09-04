package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:defs.bzl', 'cc_and_py_proto')
cc_proto(
    name='admin',
    srcs=['Admin.proto'],
    import_prefix='suez/admin',
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_and_py_proto(
    name='cluster_service',
    srcs=['ClusterService.proto'],
    import_prefix='suez/admin',
    deps=['//aios/network/arpc:proto']
)
cc_and_py_proto(
    name='scheduler_service',
    srcs=['SchedulerService.proto'],
    import_prefix='suez/admin',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='suez_admin',
    srcs=glob(['*.cpp'], exclude=['LocalCarbonAdapter.cpp']),
    hdrs=glob(['*.h'], exclude=['LocalCarbonAdapter.cpp']),
    include_prefix='suez/admin',
    deps=([
        ':admin_cc_proto', ':cluster_service_cc_proto',
        ':scheduler_service_cc_proto',
        '//aios/apps/facility/build_service:bs_config',
        '//aios/autil:closure_guard', '//aios/carbon', '//aios/catalog/service',
        '//aios/catalog/tools', '//aios/suez/common',
        '//aios/suez/sdk:hb_interface', '//aios/suez/sdk:utils',
        '@jsoncpp_git//:jsoncpp'
    ] + []),
    alwayslink=1
)
