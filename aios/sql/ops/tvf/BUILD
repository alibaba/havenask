package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_tvf_base',
    srcs=glob(['*.cpp'],
              exclude=['TvfFuncFactoryR.cpp', 'TvfWrapperR.cpp', 'TvfR.cpp']),
    hdrs=glob(['*.h'], exclude=['TvfWrapperR.h', 'TvfFuncFactoryR.h',
                                'TvfR.h']),
    include_prefix='sql/ops/tvf',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/util:sql_ops_util'
    ],
    alwayslink=True
)
cc_library(
    name='sql_ops_tvf_factory',
    srcs=glob(['TvfFuncFactoryR.cpp']),
    hdrs=glob(['TvfFuncFactoryR.h']),
    include_prefix='sql/ops/tvf',
    deps=[
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/tvf/builtin:sql_ops_tvf_builtin', '//aios/table'
    ]
)
cc_library(
    name='sql_ops_tvf_wrapper',
    srcs=glob(['TvfWrapperR.cpp']),
    hdrs=glob(['TvfWrapperR.h']),
    include_prefix='sql/ops/tvf',
    deps=[
        '//aios/sql/framework:sql_push_down_op',
        '//aios/sql/ops/tvf:sql_ops_tvf_base',
        '//aios/sql/ops/tvf:sql_ops_tvf_factory', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource'
    ]
)
cc_library(
    name='sql_ops_tvf_mock',
    srcs=glob([]),
    hdrs=glob(['test/*.h']),
    include_prefix='sql/ops/tvf',
    deps=['//aios/sql/ops/tvf:sql_ops_tvf_base']
)
cc_test(
    name='ha3_sql_tvf_base_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/sql:cp_test_tvf_plugin_error_register_testdata',
        '//aios/sql:cp_test_tvf_plugin_error_testdata',
        '//aios/sql:cp_test_tvf_plugin_testdata', '//aios/sql:testdata'
    ],
    deps=[
        ':sql_ops_tvf_base', ':sql_ops_tvf_mock', ':sql_ops_tvf_wrapper',
        '//aios/ha3:ha3_test_h', '//aios/sql/ops/test:ops_testlib',
        '//aios/unittest_framework'
    ]
)
