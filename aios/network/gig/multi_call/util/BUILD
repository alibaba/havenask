load('//bazel:defs.bzl', 'if_arm')
package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(
    name='util_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/network/arpc:arpc',
        '//aios/network/anet:anet'
    ]
)
cc_library(
    name='util',
    srcs=glob(['*.cpp']),
    deps=[
        ':util_headers', '@grpc//:grpc++',
        '//aios/network/gig/multi_call/common:common',
        '//aios/network/gig/multi_call/interface:closure_headers',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/network/arpc:arpc'
    ],
    copts=if_arm([], ['-mrdrnd'])
)
