package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_filter_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/search',
    deps=[
        '//aios/autil:log', '//aios/matchdoc',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ]
)
cc_library(
    name='ha3_filter',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_filter_headers', '//aios/matchdoc',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ]
)
