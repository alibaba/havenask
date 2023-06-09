package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_framework',
    srcs=glob(['*.cpp'],
              exclude=[
                  'TsdbSqlResultUtil.cpp', 'TsdbTableFormatter.cpp',
                  'TsdbResult.h'
              ]),
    hdrs=glob(['*.h'], exclude=['TsdbSqlResultUtil.h']),
    include_prefix='ha3/sql/framework',
    deps=([
        '//aios/ha3:ha3_fbs_headers', '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/data:sql_data', '//aios/matchdoc',
        '//aios/network/gig:multi_call', '//navi',
        '//aios/ha3/ha3/common:ha3_error_result'
    ] + []),
    alwayslink=True
)
cc_library(
    name='sql_push_down_op',
    srcs=[],
    hdrs=['PushDownOp.h'],
    include_prefix='ha3/sql/framework',
    deps=[],
    alwayslink=True
)
