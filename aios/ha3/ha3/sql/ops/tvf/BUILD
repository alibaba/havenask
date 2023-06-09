package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_tvf_base',
    srcs=glob(['*.cpp'], exclude=['TvfFuncManager.cpp', 'TvfWrapper.cpp']),
    hdrs=glob(['*.h'], exclude=['TvfFuncManager.h', 'TvfWrapper.h']),
    include_prefix='ha3/sql/ops/tvf',
    deps=[
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/sql/iquan/cpp/common:iquan_common'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='sql_ops_tvf_manager',
    srcs=glob(['TvfFuncManager.cpp']),
    hdrs=glob(['TvfFuncManager.h']),
    include_prefix='ha3/sql/ops/tvf',
    deps=[
        '//aios/ha3/ha3/sql/ops/tvf/builtin:sql_ops_tvf_builtin',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/apps/facility/build_service:bs_plugin', '//aios/table:table'
    ]
)
cc_library(
    name='sql_ops_tvf_wrapper',
    srcs=glob(['TvfWrapper.cpp']),
    hdrs=glob(['TvfWrapper.h']),
    include_prefix='ha3/sql/ops/tvf',
    deps=[
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_base',
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_manager',
        '//aios/ha3/ha3/sql/framework:sql_push_down_op'
    ]
)
