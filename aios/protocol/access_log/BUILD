package(default_visibility=['//aios/protocol/access_log:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='proto',
    srcs=['PbAccessLog.proto'],
    include=None,
    import_prefix='access_log',
    deps=[]
)
cc_library(
    name='access_log',
    srcs=glob(['*.cpp', 'local/*.cpp', 'swift/*.cpp']),
    hdrs=glob(['*.h', 'local/*.h', 'swift/*.h']),
    implementation_deps=['//aios/autil:base64', '//aios/autil:string_helper'],
    include_prefix='access_log',
    visibility=['//visibility:public'],
    deps=[
        ':proto_cc_proto', '//aios/apps/facility/swift:swift_client',
        '//aios/autil:log', '//aios/autil:range_util',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
