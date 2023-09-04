package(
    default_visibility=[
        '//aios/offline_sql_client:__subpackages__',
        '//aios/sql:__subpackages__'
    ]
)
cc_library(
    name='sql_request_data',
    srcs=glob([
        'SqlRequestData.cpp', 'SqlRequestType.cpp', 'SqlQueryRequest.cpp',
        'SqlQueryPattern.cpp'
    ]),
    hdrs=glob([
        'SqlRequestData.h', 'SqlRequestType.h', 'SqlQueryRequest.h',
        'SqlQueryPattern.h', 'ErrorResult.h'
    ]),
    include_prefix='sql/data',
    deps=[
        '//aios/autil:compression', '//aios/autil:data_buffer',
        '//aios/autil:log', '//aios/ha3:ha3_proto',
        '//aios/ha3/ha3/common:ha3_error_result', '//aios/navi',
        '//aios/sql/common:sql_auth', '//aios/sql/common:sql_kv_parser',
        '//aios/sql/common:sql_log'
    ],
    alwayslink=True
)
cc_library(
    name='sql_table_data',
    srcs=glob(['TableData.cpp', 'TableType.cpp']),
    hdrs=glob(['TableData.h', 'TableType.h']),
    include_prefix='sql/data',
    deps=['//aios/autil:log', '//aios/navi', '//aios/table'],
    alwayslink=True
)
cc_library(
    name='sql_graph_data',
    srcs=glob(['SqlGraphData.cpp', 'SqlGraphType.cpp']),
    hdrs=glob(['SqlGraphData.h', 'SqlGraphType.h']),
    include_prefix='sql/data',
    deps=['//aios/autil:log', '//aios/navi', '//aios/sql/common:sql_log'],
    alwayslink=True
)
cc_library(
    name='sql_plan_data',
    srcs=glob(['SqlPlanData.cpp', 'SqlPlanType.cpp']),
    hdrs=glob(['SqlPlanData.h', 'SqlPlanType.h']),
    include_prefix='sql/data',
    deps=[
        '//aios/navi', '//aios/sql/common:sql_log',
        '//aios/sql/iquan/cpp/jni:iquan_jni'
    ],
    alwayslink=True
)
cc_library(
    name='sql_data',
    deps=[
        ':sql_graph_data', ':sql_plan_data', ':sql_request_data',
        ':sql_table_data'
    ],
    alwayslink=True
)
cc_test(
    name='sql_data_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_data', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/table/table/test:table_testlib', '//aios/unittest_framework'
    ]
)
