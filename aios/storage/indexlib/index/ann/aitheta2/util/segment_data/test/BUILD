load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='segment_data_util_test',
    srcs=['SegmentDataReaderTest.cpp', 'SegmentDataWriterTest.cpp'],
    data=[],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/ann/aitheta2/util/segment_data:segment_data_reader',
        '//aios/storage/indexlib/index/ann/aitheta2/util/segment_data:segment_data_writer',
        '//aios/unittest_framework'
    ]
)
