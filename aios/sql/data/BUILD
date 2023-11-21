package(
    default_visibility=[
        '//aios/apps/engine/vector_service:__subpackages__',
        '//aios/offline_sql_client:__subpackages__',
        '//aios/sql:__subpackages__'
    ]
)
cc_library(
    name='sql_request_format_data',
    srcs=[
        'SqlFormatType.cpp', 'SqlQueryPattern.cpp', 'SqlQueryRequest.cpp',
        'SqlRequestType.cpp'
    ],
    hdrs=[
        'ErrorResult.h', 'SqlFormatData.h', 'SqlFormatType.h',
        'SqlQueryPattern.h', 'SqlQueryRequest.h', 'SqlRequestData.h',
        'SqlRequestType.h'
    ],
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
    srcs=['TableType.cpp'],
    hdrs=['TableData.h', 'TableType.h'],
    include_prefix='sql/data',
    deps=['//aios/autil:log', '//aios/navi', '//aios/table'],
    alwayslink=True
)
cc_library(
    name='sql_graph_data',
    srcs=['SqlGraphType.cpp'],
    hdrs=['SqlGraphData.h', 'SqlGraphType.h'],
    include_prefix='sql/data',
    deps=['//aios/autil:log', '//aios/navi', '//aios/sql/common:sql_log'],
    alwayslink=True
)
cc_library(
    name='sql_plan_data',
    srcs=['SqlPlanType.cpp'],
    hdrs=['SqlPlanData.h', 'SqlPlanType.h'],
    include_prefix='sql/data',
    deps=[
        '//aios/navi', '//aios/sql/common:sql_log',
        '//aios/sql/iquan/cpp/jni:iquan_jni'
    ],
    alwayslink=True
)
cc_library(
    name='sql_query_config_data',
    srcs=['SqlQueryConfigData.cpp'],
    hdrs=['SqlQueryConfigData.h'],
    include_prefix='sql/data',
    deps=[
        '//aios/autil:data_buffer', '//aios/navi',
        '//aios/sql/proto:sql_query_config_cc_proto'
    ],
    alwayslink=True
)
cc_library(
    name='sql_data',
    deps=[
        ':sql_graph_data', ':sql_plan_data', ':sql_query_config_data',
        ':sql_request_format_data', ':sql_table_data'
    ]
)
cc_test(
    name='sql_data_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_data', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/table/test:table_testlib', '//aios/unittest_framework'
    ]
)
