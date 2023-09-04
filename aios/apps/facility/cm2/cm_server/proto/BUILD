package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='cm_server',
    srcs=['cm_ctrl.proto', 'shadow_cluster.proto', 'subset.proto'],
    import_prefix='cm_server/proto',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/cm2/cm_basic/basic_struct:proto',
        '//aios/network/arpc:proto'
    ]
)
