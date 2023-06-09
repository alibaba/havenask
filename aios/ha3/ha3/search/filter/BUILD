package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_filter_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/search',
    deps=[
        '//aios/autil:log', '//aios/matchdoc',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ]
)
cc_library(
    name='ha3_filter',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_filter_headers', '//aios/matchdoc',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ]
)
