package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
cc_library(
    name='conhash',
    srcs=['maglev_conhash.cpp', 'weight_conhash.cpp'],
    hdrs=['maglev_conhash.h', 'weight_conhash.h'],
    implementation_deps=[
        '//aios/apps/facility/cm2/cm_basic/util:util', '//aios/autil:hash_base',
        '//aios/autil:lock'
    ],
    visibility=[
        '//aios/apps/facility/cm2:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ]
)
