package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_unpack')
cc_library(
    name='sql_ops_turbojet_calc',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/calc',
    deps=[
        '//aios/ha3/ha3/sql/ops/turbojetCalc:sql_ops_turbojet_calc',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/resource:sql_resource', '//navi'
    ],
    alwayslink=True
)
bundle_unpack(
    name='bundle',
    testonly=1,
    srcs=['//aios/ha3/ha3/sql/ops/turbojetCalc:bundle'],
    visibility=[':__subpackages__']
)
