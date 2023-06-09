package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='modelconfig',
    hdrs=glob(['ModelConfig.h']),
    deps=['//aios/ha3:ha3_isearch_h', '//aios/autil:json'],
    include_prefix='ha3/turing/common'
)
cc_library(
    name='sort_descriptions',
    hdrs=glob(['SortDescs.h']),
    deps=[
        '//aios/ha3:ha3_isearch_h', '//aios/autil:json',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog'
    ],
    include_prefix='ha3/turing/common'
)
cc_library(
    name='ha3_meta_data',
    hdrs=glob(['*.h'], exclude=['ModelConfig.h']),
    srcs=glob(['*.cpp'], exclude=['Ha3TableConverterExec.cpp']),
    deps=[
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/autil:json', '//aios/ha3/ha3/common:ha3_common_headers'
    ],
    include_prefix='ha3/turing/common'
)
