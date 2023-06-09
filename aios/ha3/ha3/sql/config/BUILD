package(
    default_visibility=[
        '//aios/ha3:__subpackages__',
        '//aios/offline_sql_client:__subpackages__'
    ]
)
cc_library(
    name='sql_config',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/config',
    deps=[
        '//aios/ha3/ha3/sql/common:sql_common_h',
        '//aios/ha3/ha3/sql/common:sql_log', '//navi'
    ],
    alwayslink=True
)
