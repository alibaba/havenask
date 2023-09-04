load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='util_unittest',
    srcs=[
        'IndexlibMetricControlTest.cpp', 'KmonitorTagvNormalizerTest.cpp',
        'TaggedMetricReporterGroupTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    deps=[
        '//aios/autil:thread',
        '//aios/storage/indexlib/util/metrics:metric_reporter',
        '//aios/storage/indexlib/util/metrics:monitor',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
