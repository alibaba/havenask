package(
    default_visibility=['//aios/apps/facility/build_service:__subpackages__']
)
cc_library(
    name='bs_reader_test_helper',
    srcs=([
        'BinaryFileDocumentReaderTestBase.cpp', 'CustomRawDocumentReader.cpp',
        'CustomRawDocumentReaderFactory.cpp', 'CustomizedRawDocParser.cpp',
        'FakeFileReader.cpp', 'FakeRawDocumentReader.cpp',
        'FakeRawDocumentReaderWithProgress.cpp',
        'FakeSwiftRawDocumentReader.cpp', 'FileDocumentReaderTestBase.cpp'
    ] + []),
    hdrs=([
        'BinaryFileDocumentReaderTestBase.h', 'CustomRawDocumentReader.h',
        'CustomRawDocumentReaderFactory.h', 'CustomizedRawDocParser.h',
        'FakeFileReader.h', 'FakeRawDocumentReader.h',
        'FakeRawDocumentReaderWithProgress.h', 'FakeSwiftRawDocumentReader.h',
        'FileDocumentReaderTestBase.h', 'MockFilePattern.h',
        'MockRawDocumentReader.h', 'MockSwiftClient.h',
        'MockSwiftClientCreator.h'
    ] + []),
    copts=['-std=c++17'],
    include_prefix='build_service/reader/test',
    visibility=['//visibility:public'],
    deps=([
        '//aios/apps/facility/build_service:bs_testbase',
        '//aios/apps/facility/build_service/build_service/reader:bs_reader',
        '//aios/apps/facility/swift/testlib:mock_swift_client'
    ] + [])
)
