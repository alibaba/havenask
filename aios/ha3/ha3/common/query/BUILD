package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_query_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/autil:log', '//aios/autil:data_buffer',
        '//aios/autil:string_helper', '//aios/autil:time',
        '//aios/autil:common_macros', '//aios/ha3:ha3_isearch_h',
        '//aios/ha3/ha3/common:ha3_error_result_headers',
        '//aios/apps/facility/build_service:bs_analyzer',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ],
    include_prefix='ha3/common',
    alwayslink=True
)
cc_library(
    name='ha3_query',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_query_headers', '//aios/ha3/ha3/common:ha3_error_result',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ],
    alwayslink=True
)
