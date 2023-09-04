package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_graph_transform',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/planTransform',
    deps=[
        '//aios/navi', '//aios/sql/common:sql_common',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/iquan/cpp/jni:iquan_jni'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_graph_transform_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[':testdata'],
    deps=[
        ':sql_graph_transform', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/ha3:ha3_test_h', '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/unittest_framework'
    ]
)
filegroup(name='testdata', testonly=True, srcs=glob(['test/testdata/**']))
