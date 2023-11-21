package(default_visibility=['//aios/suez_turing:__subpackages__'])
cc_library(
    name='core',
    srcs=glob(['core/*.cpp']),
    hdrs=glob(['core/*.h']),
    include_prefix='suez/turing/navi/',
    strip_include_prefix='core',
    visibility=['//visibility:public'],
    deps=[
        '//aios/navi', '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/suez:suez_index_provider_r',
        '//aios/suez_navi:suez_navi_resource',
        '//aios/suez_turing:multi_table_wrapper',
        '//aios/suez_turing:turing_config',
        '//aios/suez_turing/suez/turing/expression:turing_expression_navi_resource'
    ],
    alwayslink=1
)
