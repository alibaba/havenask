package(default_visibility=['//aios/apps/facility/cm2:__subpackages__'])
cc_library(
    name='conhash',
    srcs=['weight_conhash.cpp', 'maglev_conhash.cpp'],
    hdrs=['weight_conhash.h', 'maglev_conhash.h'],
    implementation_deps=[
        '//aios/apps/facility/cm2/cm_basic/util:util', '//aios/autil:hash_base',
        '//aios/autil:lock'
    ]
)
