load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='parallel_merge_util_test',
    srcs=['ParallelMergeUtilTest.cpp'],
    data=[],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:segment_base',
        '//aios/storage/indexlib/index/ann/aitheta2/util/parallel_merge:parallel_merge_util',
        '//aios/unittest_framework', '@proxima2//:proxima2_headers'
    ]
)
