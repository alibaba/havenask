package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='watermark',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/resource/watermark',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:bs_workflow', '//aios/autil:lock',
        '//aios/autil:log', '//aios/autil:thread',
        '//aios/sql/common:generic_waiter', '//aios/sql/common:sql_log',
        '//aios/storage/indexlib:interface',
        '//aios/suez/drc:swift_reader_creator'
    ]
)
