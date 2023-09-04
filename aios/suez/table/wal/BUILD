package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='wal',
    srcs=[
        'GlobalQueueManager.cpp', 'NoneWAL.h', 'QueueWAL.cpp',
        'RealtimeSwiftWAL.cpp', 'SyncWal.h', 'WALConfig.cpp', 'WALStrategy.cpp'
    ],
    hdrs=[
        'CommonDefine.h', 'GlobalQueueManager.h', 'QueueWAL.h',
        'RealtimeSwiftWAL.h', 'WALConfig.h', 'WALStrategy.h'
    ],
    include_prefix='suez/table/wal',
    deps=[
        '//aios/apps/facility/build_service:bs_config',
        '//aios/apps/facility/swift:swift_client', '//aios/autil:log',
        '//aios/autil:result', '//aios/autil:thread'
    ]
)
