package(default_visibility=['//aios/table:__subpackages__'])
cc_library(
    name='table_testlib',
    hdrs=['MatchDocUtil.h', 'TableTestUtil.h'],
    include_prefix='table/test',
    visibility=['//visibility:public'],
    deps=['//aios/table']
)
cc_test(
    name='table_test',
    srcs=glob(['*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[':table_testlib', '//aios/table', '//aios/unittest_framework']
)
