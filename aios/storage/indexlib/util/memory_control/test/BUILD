load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='memory_control_unittest',
    srcs=[
        'BlockMemoryQuotaControllerTest.cpp', 'BuildResourceCalculatorTest.cpp',
        'BuildResourceMetricsTest.cpp', 'MemoryQuotaSynchronizerTest.cpp',
        'QuotaControlTest.cpp', 'WaitMemoryQuotaControllerTest.cpp'
    ],
    deps=[
        '//aios/autil:thread', '//aios/storage/indexlib/util/memory_control',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
