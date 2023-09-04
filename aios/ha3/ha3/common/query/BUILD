package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_query_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/common',
    deps=[
        '//aios/apps/facility/build_service:bs_analyzer',
        '//aios/autil:common_macros', '//aios/autil:data_buffer',
        '//aios/autil:log', '//aios/autil:string_helper', '//aios/autil:time',
        '//aios/ha3:ha3_isearch_h',
        '//aios/ha3/ha3/common:ha3_error_result_headers',
        '//aios/suez_turing/suez/turing/expression/util/index_info'
    ],
    alwayslink=True
)
cc_library(
    name='ha3_query',
    srcs=glob(['*.cpp']),
    visibility=['//visibility:public'],
    deps=[
        ':ha3_query_headers', '//aios/ha3/ha3/common:ha3_error_result',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata',
        '//aios/suez_turing/suez/turing/expression/util/index_info'
    ],
    alwayslink=True
)
