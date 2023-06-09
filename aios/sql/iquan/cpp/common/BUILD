package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
load('//bazel:defs.bzl', 'copy_target_to')
cc_library(
    name='iquan_common',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    deps=['//aios/autil:json', '//aios/autil:string_helper'],
    visibility=['//visibility:public'],
    include_prefix='iquan/common'
)
