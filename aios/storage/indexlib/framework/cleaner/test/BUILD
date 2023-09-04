load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='cleaner_unittest',
    srcs=[
        'InMemoryIndexCleanerTest.cpp', 'OnDiskIndexCleanerTest.cpp',
        'TabletReaderCleanerTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:ReadResource',
        '//aios/storage/indexlib/framework:SegmentFenceDirFinder',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:TabletReaderContainer',
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/framework:VersionCommitter',
        '//aios/storage/indexlib/framework:VersionLoader',
        '//aios/storage/indexlib/framework/cleaner:InMemoryIndexCleaner',
        '//aios/storage/indexlib/framework/cleaner:OnDiskIndexCleaner',
        '//aios/storage/indexlib/framework/cleaner:TabletReaderCleaner',
        '//aios/storage/indexlib/framework/test:FakeTabletReader',
        '//aios/unittest_framework'
    ]
)
