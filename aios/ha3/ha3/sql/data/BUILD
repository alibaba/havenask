package(
    default_visibility=[
        '//aios/ha3:__subpackages__',
        '//aios/offline_sql_client:__subpackages__'
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
        'SqlQueryPattern.h'
    ]),
    include_prefix='ha3/sql/data',
    deps=[
        '//aios/autil:log', '//aios/autil:data_buffer',
        '//aios/ha3/ha3/sql/common:sql_auth',
        '//aios/ha3/ha3/sql/common:sql_kv_parser',
        '//aios/ha3/ha3/sql/common:sql_log', '//navi:navi'
    ],
    alwayslink=True
)
cc_library(
    name='sql_table_data',
    srcs=glob(['TableData.cpp', 'TableType.cpp']),
    hdrs=glob(['TableData.h', 'TableType.h']),
    include_prefix='ha3/sql/data',
    deps=['//aios/autil:log', '//navi:navi', '//aios/table:table'],
    alwayslink=True
)
cc_library(
    name='sql_graph_data',
    srcs=glob(['SqlGraphData.cpp', 'SqlGraphType.cpp']),
    hdrs=glob(['SqlGraphData.h', 'SqlGraphType.h']),
    include_prefix='ha3/sql/data',
    deps=[
        '//aios/autil:log', '//navi:navi', '//aios/ha3/ha3/sql/common:sql_log'
    ],
    alwayslink=True
)
cc_library(
    name='sql_plan_data',
    srcs=glob(['SqlPlanData.cpp', 'SqlPlanType.cpp']),
    hdrs=glob(['SqlPlanData.h', 'SqlPlanType.h']),
    include_prefix='ha3/sql/data',
    deps=[
        '//navi:navi', '//aios/sql/iquan/cpp/jni:iquan_jni',
        '//aios/ha3/ha3/sql/common:sql_log'
    ],
    alwayslink=True
)
cc_library(
    name='sql_data',
    deps=[
        ':sql_plan_data', ':sql_request_data', ':sql_table_data',
        ':sql_graph_data'
    ],
    alwayslink=True
)
