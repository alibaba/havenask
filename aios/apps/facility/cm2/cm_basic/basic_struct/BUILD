package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='proto',
    srcs=glob(['proto/*.proto']),
    import_prefix='cm_basic/basic_struct',
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='xds_proto',
    srcs=glob([
        'proto/envoy/**/*.proto', 'proto/google/**/*.proto',
        'proto/udpa/**/*.proto', 'proto/validate/**/*.proto'
    ]),
    import_prefix='cm_basic/basic_struct',
    use_grpc_plugin=True,
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=[]
)
cc_library(
    name='basic_struct',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=[
        ':proto_cc_proto', '//aios/apps/facility/cm2/cm_basic/common',
        '//aios/apps/facility/cm2/cm_basic/util', '//aios/autil:time'
    ]
)
