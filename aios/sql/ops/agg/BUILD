package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_agg_base',
    srcs=glob(['*.cpp'],
              exclude=[
                  'AggFuncFactoryR.cpp', 'Aggregator.cpp', 'AggNormal.cpp',
                  'AggBase.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'AggFuncFactoryR.h', 'Aggregator.h', 'AggBase.h',
                  'AggNormal.h'
              ]),
    include_prefix='sql/ops/agg',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/autil:plugin_base', '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/util:sql_ops_util', '//aios/table'
    ],
    alwayslink=True
)
cc_library(
    name='sql_ops_agg_func_factory',
    srcs=glob(['AggFuncFactoryR.cpp']),
    hdrs=glob(['AggFuncFactoryR.h']),
    include_prefix='sql/ops/agg',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:plugin_base', '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/agg/builtin:sql_ops_agg_builtin', '//aios/table'
    ],
    alwayslink=True
)
cc_library(
    name='sql_ops_agg_op',
    srcs=glob(['Aggregator.cpp', 'AggBase.cpp', 'AggNormal.cpp']),
    hdrs=glob(['Aggregator.h', 'AggBase.h', 'AggNormal.h']),
    include_prefix='sql/ops/agg',
    visibility=['//visibility:public'],
    deps=[
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/agg:sql_ops_agg_base',
        '//aios/sql/ops/agg:sql_ops_agg_func_factory',
        '//aios/sql/resource:sql_resource', '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_agg_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/sql:cp_test_agg_plugin_testdata', '//aios/sql:testdata'],
    deps=[
        ':sql_ops_agg_base', ':sql_ops_agg_op', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
cc_test(
    name='ha3_sql_agg_benchmark',
    srcs=glob(['test/*Benchmark.cpp']),
    copts=['-fno-access-control'],
    tags=['manual'],
    deps=[
        ':sql_ops_agg_base', ':sql_ops_agg_op',
        '//aios/sql/ops/test:ops_testlib', '//aios/table/test:table_testlib',
        '//aios/unittest_framework:unittest_benchmark'
    ]
)
