package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_searchinfo_headers',
    hdrs=glob(['*.h']),
    deps=[],
    include_prefix='ha3/common/searchinfo'
)
cc_library(
    name='ha3_searchinfo',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_searchinfo_headers', '//aios/autil:data_buffer',
        '//aios/autil:string_helper'
    ]
)
