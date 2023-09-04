load('//aios/storage/indexlib/index/ann/aitheta2:defs.bzl', 'aitheta_cc_test')
aitheta_cc_test(
    name='aitheta_recall_reporter_test',
    srcs=['AithetaRecallReporterTest.cpp'],
    copts=['-fno-access-control'],
    data=[],
    deps=[
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_reader',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
