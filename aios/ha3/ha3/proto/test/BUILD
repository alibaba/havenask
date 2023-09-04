package(default_visibility=['//aios/ha3:__subpackages__'])
cc_test(
    name='ha3_proto_test',
    srcs=glob(['*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/ha3:leak_suppression'],
    deps=['//aios/ha3:ha3_proto', '//aios/unittest_framework']
)
