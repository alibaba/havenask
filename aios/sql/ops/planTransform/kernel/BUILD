package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_plan_transform',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/planTransform',
    deps=[
        '//aios/navi', '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/planTransform:sql_graph_transform',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_plan_transform_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/filesystem/fslib:fslib_plugin_hdfs', '//aios/sql:testdata',
        '//aios/sql/iquan/java:iquan_client_jar'
    ],
    deps=[
        ':sql_ops_plan_transform', '//aios/navi:navi_ops_testlib',
        '//aios/unittest_framework'
    ]
)
