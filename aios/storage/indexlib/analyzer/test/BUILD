load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='analyzer_unittest',
    srcs=[
        'AnalyzerTest.cpp', 'SimpleTokenizerTest.cpp', 'StopWordFilterTest.cpp',
        'TextBufferTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=['//aios/storage/indexlib/analyzer', '//aios/unittest_framework']
)
