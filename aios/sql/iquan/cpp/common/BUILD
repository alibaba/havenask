package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
cc_library(
    name='iquan_common',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='iquan/common',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:env_util', '//aios/autil:json',
        '//aios/autil:string_helper'
    ]
)
filegroup(name='testdata', testonly=True, srcs=glob(['test/testdata/**']))
cc_test(
    name='iquan_common_test',
    srcs=glob(['test/*.cpp']),
    copts=['-fno-access-control'],
    data=['testdata', '//aios/sql/iquan/cpp:test_alog_conf'],
    deps=[
        ':iquan_common', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/unittest_framework'
    ]
)
