package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
cc_library(
    name='util',
    srcs=glob(['*.cpp', '*.c'], exclude=['FakeZkWrapper.cpp']),
    hdrs=glob(['*.h']),
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=[
        '//aios/apps/facility/cm2/cm_basic/common', '//aios/autil:lock',
        '//aios/autil:string_helper', '//third_party/zookeeper'
    ]
)
cc_library(
    name='fake_zk_wrapper',
    srcs=['FakeZkWrapper.cpp'],
    linkstatic=1,
    visibility=['//visibility:public'],
    deps=[':util', '//aios/filesystem/fslib:fslib-framework']
)
