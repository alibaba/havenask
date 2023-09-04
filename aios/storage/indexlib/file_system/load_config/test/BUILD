load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='load_config_unittest',
    srcs=[
        'CacheLoadStrategyTest.cpp', 'LoadConfigListTest.cpp',
        'LoadConfigTest.cpp', 'MmapLoadStrategyTest.cpp',
        'WarmupStrategyTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/file_system/load_config',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
