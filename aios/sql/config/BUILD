package(
    default_visibility=[
        '//aios/offline_sql_client:__subpackages__',
        '//aios/sql:__subpackages__'
    ]
)
cc_library(
    name='sql_config',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/config',
    deps=[
        '//aios/navi', '//aios/sql/common:sql_common_h',
        '//aios/sql/common:sql_log'
    ],
    alwayslink=True
)
filegroup(name='testdata', testonly=True, srcs=glob(['test/testdata/**']))
cc_test(
    name='ha3_sql_config_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[':testdata'],
    deps=[
        ':sql_config', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/navi:navi_ops_testlib', '//aios/unittest_framework'
    ]
)
