load('//aios/storage/indexlib/index/ann/aitheta2:defs.bzl', 'aitheta_cc_test')
aitheta_cc_test(
    name='SegmentMetaTest',
    srcs=['SegmentMetaTest.cpp'],
    copts=['-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        '//aios/storage/indexlib/index/ann/aitheta2/impl:segment_base',
        '//aios/storage/indexlib/index/ann/aitheta2/util/parallel_merge:ParallelReduceMeta',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
