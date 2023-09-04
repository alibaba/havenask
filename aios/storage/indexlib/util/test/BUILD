load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='error_log_collector_unittest',
    srcs=['ErrorLogCollectorTest.cpp'],
    data=['testdata/test_alog.conf'],
    deps=[
        '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:ErrorLogCollector',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='util_perf_test',
    srcs=['ExHashMapPerfTest.cpp', 'HashMapMultiThreadPerfTest.cpp'],
    deps=[
        '//aios/autil:thread', '//aios/storage/indexlib/util:HashMap',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='KeyValueMapTest',
    srcs=['KeyValueMapTest.cpp'],
    deps=[
        '//aios/storage/indexlib/util:key_value_map',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='GroupedThreadPoolTest',
    srcs=['GroupedThreadPoolTest.cpp'],
    deps=[
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:GroupedThreadPool',
        '//aios/unittest_framework'
    ]
)
