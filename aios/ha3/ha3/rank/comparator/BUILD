package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_comparator_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/rank',
    deps=['//aios/suez_turing/suez/turing/expression:turing_expression']
)
cc_library(
    name='ha3_comparator',
    srcs=glob(['*.cpp']),
    visibility=['//visibility:public'],
    deps=[
        ':ha3_comparator_headers', '//aios/sql/common:sql_log',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ]
)
