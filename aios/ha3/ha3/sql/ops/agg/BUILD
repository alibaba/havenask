package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_agg_base',
    srcs=glob(['*.cpp'],
              exclude=[
                  'AggFuncManager.cpp', 'Aggregator.cpp', 'AggNormal.cpp',
                  'AggBase.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'AggFuncManager.h', 'Aggregator.h', 'AggBase.h', 'AggNormal.h'
              ]),
    include_prefix='ha3/sql/ops/agg',
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/sql/iquan/cpp/common:iquan_common', '//aios/table:table',
        '//aios/autil:plugin_base'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='sql_ops_agg_fuc_manager',
    srcs=glob(['AggFuncManager.cpp']),
    hdrs=glob(['AggFuncManager.h']),
    include_prefix='ha3/sql/ops/agg',
    deps=[
        '//aios/ha3/ha3/sql/ops/agg/builtin:sql_ops_agg_builtin',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/sql/iquan/cpp/common:iquan_common', '//aios/table:table',
        '//aios/autil:plugin_base'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='sql_ops_agg_op',
    srcs=glob(['Aggregator.cpp', 'AggBase.cpp', 'AggNormal.cpp']),
    hdrs=glob(['Aggregator.h', 'AggBase.h', 'AggNormal.h']),
    include_prefix='ha3/sql/ops/agg',
    deps=[
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_base',
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_fuc_manager',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/sql/iquan/cpp/common:iquan_common', '//aios/table:table'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
