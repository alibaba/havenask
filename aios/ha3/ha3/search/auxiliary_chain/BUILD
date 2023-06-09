package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_auxiliary_chain_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/common/query:ha3_query_headers',
        '//aios/ha3/ha3/common:common_def'
    ],
    include_prefix='ha3/search'
)
cc_library(
    name='ha3_auxiliary_chain',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_auxiliary_chain_headers', '//aios/ha3/ha3/common/query:ha3_query',
        '//aios/storage/indexlib/indexlib/index:index'
    ]
)
