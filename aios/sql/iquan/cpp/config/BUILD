package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
load('//bazel:defs.bzl', 'copy_target_to')
cc_library(
    name='iquan_config',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    deps=['//aios/sql/iquan/cpp/common:iquan_common'],
    visibility=['//visibility:public'],
    include_prefix='iquan/config'
)
