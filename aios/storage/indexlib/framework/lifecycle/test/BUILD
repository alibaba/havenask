load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='LifecycleStrategyTest',
    srcs=['LifecycleStrategyTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/framework/lifecycle',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
