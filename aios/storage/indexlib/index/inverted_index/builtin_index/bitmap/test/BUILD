load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_fast_test(
    name='bitmap_index_test',
    srcs=[
        'BitmapIndexReaderTest.cpp', 'BitmapPostingDecoderTest.cpp',
        'BitmapPostingIteratorTest.cpp', 'BitmapPostingMergerTest.cpp',
        'BitmapPostingWriterTest.cpp', 'InMemBitmapIndexDecoderTest.cpp',
        'InMemBitmapIndexSegmentReaderTest.cpp',
        'OnDiskBitmapIndexIteratorTest.cpp',
        'SingleBitmapPostingIteratorTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        ':BitmapPostingMaker',
        '//aios/storage/indexlib/framework/mock:FakeSegment',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:InMemBitmapIndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:OnDiskBitmapIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/test:PostingMaker',
        '//aios/storage/indexlib/index/test:FakeDocMapper',
        '//aios/storage/indexlib/index/test:IndexTestUtil',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
strict_cc_library(
    name='BitmapPostingMaker',
    testonly=True,
    deps=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/test:PostingMaker'
    ]
)
