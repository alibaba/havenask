cc_test(
    name='drc_test',
    srcs=[
        'DrcConfigTest.cpp', 'KVUpdate2AddTest.cpp', 'LogRecordTest.cpp',
        'LogReplicationPipelineTest.cpp', 'MockLogRewriter.h', 'MockSink.h',
        'MockSource.h', 'MockWorkerState.h', 'SourceUpdate2AddTest.cpp',
        'SwiftSourceTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/storage/indexlib/table/kv_table/test:kv_table_test_helper',
        '//aios/storage/indexlib/table/normal_table/test:normal_table_test_helper',
        '//aios/suez/drc', '//aios/unittest_framework'
    ]
)
