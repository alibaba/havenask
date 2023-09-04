package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='block_array_unittest',
    srcs=['BlockArrayReaderTest.cpp', 'BlockArrayWriterTest.cpp'],
    shard_count=2,
    deps=[
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/common/block_array:BlockArrayReader',
        '//aios/storage/indexlib/index/common/block_array:BlockArrayWriter',
        '//aios/storage/indexlib/util:simple_pool', '//aios/unittest_framework'
    ]
)
