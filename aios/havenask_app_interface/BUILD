package(default_visibility=['//aios/havenask_app_interface:__subpackages__'])
cc_library(
    name='havenask_app_lib',
    srcs=glob([
        'application/Server.h', 'application/Server.cpp', 'application/main.cpp'
    ]),
    deps=[
        '//aios/filesystem/fslib:fslib-framework', '//aios/autil:env_util',
        '//aios/autil:log', '//aios/autil:option_parser',
        '//aios/network/gig:multi_call', '//aios/suez:suez_worker'
    ],
    visibility=['//visibility:public'],
    alwayslink=1
)
