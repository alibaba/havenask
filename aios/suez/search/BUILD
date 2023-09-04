package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='search',
    srcs=[
        'ConfigItem.cpp', 'ConfigManager.cpp', 'CounterReporter.cpp',
        'SearchManagerUpdater.cpp'
    ],
    hdrs=[
        'ConfigItem.h', 'ConfigManager.h', 'CounterReporter.h',
        'SearchManagerUpdater.h'
    ],
    include_prefix='suez/search',
    deps=[
        '//aios/suez/common', '//aios/suez/deploy', '//aios/suez/heartbeat',
        '//aios/suez/sdk', '//aios/suez/service:builtin_service'
    ]
)
