package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_parser',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/parser',
    deps=[
        '//aios/memory/lockless_allocator:lockless_api', '//aios/navi',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_parser_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/filesystem/fslib:fslib_plugin_hdfs', '//aios/sql:testdata',
        '//aios/sql/iquan/java:iquan_client_jar'
    ],
    deps=[
        ':sql_ops_parser', '//aios/navi:navi_ops_testlib',
        '//aios/unittest_framework'
    ]
)
