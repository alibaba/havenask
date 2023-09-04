package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
cc_library(
    name='iquan_catalog',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='iquan/common/catalog',
    visibility=['//visibility:public'],
    deps=['//aios/sql/iquan/cpp/common:iquan_common']
)
cc_test(
    name='catalog_test',
    srcs=glob(['test/*.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/sql/iquan/cpp:test_alog_conf'],
    deps=[
        ':iquan_catalog', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/filesystem/fslib:fslib-framework', '//aios/unittest_framework'
    ]
)
