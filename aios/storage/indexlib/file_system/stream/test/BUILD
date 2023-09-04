load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
package(default_visibility=['//visibility:private'])
strict_cc_fast_test(
    name='file_stream_test',
    srcs=[
        'BlockFileStreamTest.cpp', 'CompressFileStreamTest.cpp',
        'FileStreamCreatorTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:containers', '//aios/autil:exception',
        '//aios/autil:mem_pool_base', '//aios/autil:multi_value',
        '//aios/autil:string_helper', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/stream:file_stream',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/unittest_framework'
    ]
)
