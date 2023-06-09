package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(name='controller_headers', hdrs=glob(['*.h']), deps=[])
cc_library(
    name='controller',
    srcs=glob(['*.cpp']),
    deps=[
        ':controller_headers', '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig:agent_proto_cc_proto'
    ]
)
