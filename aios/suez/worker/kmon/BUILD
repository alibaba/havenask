package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='service',
    srcs=glob(['*.proto']),
    import_prefix='suez/worker',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='lib',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='suez/worker',
    deps=[
        ':service_cc_proto', '//aios/autil:closure_guard', '//aios/autil:lock',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/suez/sdk',
        '//aios/suez/worker:env_param'
    ],
    alwayslink=1
)
cc_test(
    name='test',
    srcs=glob(['test/*.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[':lib', '//aios/unittest_framework']
)
