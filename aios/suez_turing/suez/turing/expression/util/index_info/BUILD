package(default_visibility=['//aios/suez_turing:__subpackages__'])
cc_library(
    name='index_info',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='suez/turing/expression/util',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:json', '//aios/matchdoc',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/suez_turing/suez/turing/expression:common_h'
    ]
)
