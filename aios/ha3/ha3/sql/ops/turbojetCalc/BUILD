package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='sql_ops_turbojet_calc',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/calc:sql_ops_calc_table',
        '//aios/ha3/ha3/sql/ops/condition:sql_ops_condition',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '@suez_turing.turbojet.turing_ops_util//:turbojet', '//navi:navi',
        '//aios/autil:plugin_base', '//aios/table:table'
    ],
    include_prefix='ha3/sql/ops/turbojetCalc'
)
