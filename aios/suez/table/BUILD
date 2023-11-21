package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='table',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='suez/table',
    deps=[
        '//aios/apps/facility/build_service:bs_build_task',
        '//aios/apps/facility/build_service:bs_merge', '//aios/suez/common',
        '//aios/suez/deploy', '//aios/suez/drc', '//aios/suez/heartbeat',
        '//aios/suez/sdk:index_provider', '//aios/suez/sdk:table_writer'
    ]
)
