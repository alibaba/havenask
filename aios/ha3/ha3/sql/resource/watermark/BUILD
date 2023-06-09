package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='watermark',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/resource/watermark',
    deps=[
        '//aios/autil:log', '//aios/autil:thread', '//aios/autil:lock',
        '//aios/ha3/ha3/sql/common:generic_waiter',
        '//aios/ha3/ha3/sql/common:sql_log',
        '//aios/apps/facility/build_service:bs_workflow',
        '//aios/storage/indexlib:interface', '//aios/suez/suez/drc:suez_drc'
    ],
    visibility=['//visibility:public']
)
