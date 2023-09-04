cc_library(
    name='leader_client',
    srcs=['LeaderClient.cpp'],
    hdrs=['LeaderClient.h'],
    deps=[
        '//aios/network/arpc:arpc', '//aios/autil:NoCopyable',
        '//aios/autil:thread', '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/worker_framework:leader_locator'
    ],
    include_prefix='leader_client',
    visibility=['//visibility:public']
)
