package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_join_base',
    srcs=glob(['*.cpp'], exclude=['LookupR.cpp', 'LookupNormalR.cpp']),
    hdrs=glob(['*.h'],
              exclude=['LookupR.h', 'LookupNormalR.h', 'LookupJoinBatch.h']),
    include_prefix='sql/ops/join',
    deps=[
        '//aios/sql/ops/calc:sql_ops_calc_table',
        '//aios/sql/ops/util:sql_ops_util'
    ],
    alwayslink=True
)
cc_library(
    name='lookup_join_r',
    srcs=['LookupNormalR.cpp', 'LookupR.cpp'],
    hdrs=['LookupJoinBatch.h', 'LookupNormalR.h', 'LookupR.h'],
    include_prefix='sql/ops/join',
    deps=[':sql_ops_join_base', '//aios/sql/ops/scan:sql_ops_normal_scan'],
    alwayslink=True
)
cc_test(
    name='ha3_sql_ops_join_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    linkopts=['-Wl,--as-needed'],
    shard_count=3,
    deps=[
        ':lookup_join_r', ':sql_ops_join_base', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
