package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library_permissions'
)
transitive_deps = [
    '//aios/apps/facility/cm2/cm_sub/config:config',
    '//aios/apps/facility/cm2/cm_sub/conhash:conhash'
]
cc_shared_library_permissions(
    name='permissions',
    targets=([':cm_sub'] + transitive_deps),
    visibility=['//visibility:public']
)
cc_library(
    name='cm_sub',
    srcs=glob(['*.cpp', 'lb_policy/*.cpp']),
    hdrs=glob(['*.h', 'lb_policy/*.h']),
    visibility=['//visibility:public'],
    deps=(
        transitive_deps + [
            '//aios/apps/facility/cm2/cm_basic/basic_struct:basic_struct',
            '//aios/apps/facility/cm2/cm_basic/config:config',
            '//aios/apps/facility/cm2/cm_basic/util:util',
            '//aios/apps/facility/cm2/cm_basic/common:common',
            '//aios/apps/facility/cm2/cm_basic/leader_election:leader_election',
            '//aios/alog:alog', '//aios/autil:thread', '//aios/autil:hash_base',
            '//aios/autil:net', '//third_party/zookeeper:zookeeper',
            '//third_party/mxml:mxml', '@snappy//:snappy',
            '//aios/network/arpc:arpc'
        ]
    ),
    alwayslink=True
)
