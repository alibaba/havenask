package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_condition',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/condition',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:json_helper', '//aios/ha3/ha3/sql/common:sql_common',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ]
)
