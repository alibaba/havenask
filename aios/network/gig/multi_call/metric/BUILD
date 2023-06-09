package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(
    name='metric_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/gig/multi_call/util:util_headers',
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/kmonitor:kmonitor_client_cpp'
    ]
)
cc_library(
    name='metric',
    srcs=glob(['*.cpp']),
    deps=([
        ':metric_headers', '//aios/network/gig/multi_call/common:common',
        '//aios/network/gig/multi_call/util:util',
        '//aios/kmonitor:kmonitor_client_cpp'
    ] + [])
)
