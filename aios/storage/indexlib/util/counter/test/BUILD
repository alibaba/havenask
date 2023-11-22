load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='indexlib_counter_unittest',
    srcs=['CounterMapTest.cpp', 'CounterTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:thread', '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util/counter',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
