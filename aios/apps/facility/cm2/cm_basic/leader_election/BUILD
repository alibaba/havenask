package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
cc_library(
    name='leader_election',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=[
        '//aios/apps/facility/cm2/cm_basic/common',
        '//aios/apps/facility/cm2/cm_basic/util', '//aios/autil:string_helper'
    ]
)
