cc_test(
    name='remote_table_writer_test',
    srcs=[
        'RemoteTableWriterClosureTest.cpp',
        'RemoteTableWriterRequestGeneratorTest.cpp', 'RemoteTableWriterTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:range_util', '//aios/network/gig:multi_call',
        '//aios/suez/sdk:remote_table_writer',
        '//aios/suez/service:table_service_cc_proto',
        '//aios/unittest_framework'
    ]
)
cc_test(
    name='table_writer_test',
    srcs=['TableWriterTest.cpp'],
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        '//aios/apps/facility/build_service:bs_workflow',
        '//aios/storage/indexlib/framework/mock:tablet',
        '//aios/suez/sdk:table_writer',
        '//aios/suez/table/test:mock_wal_strategy', '//aios/unittest_framework'
    ]
)
cc_test(
    name='sdk_test',
    srcs=[
        'JsonNodeRefTest.cpp', 'PartitionIdTest.cpp', 'PathDefineTest.cpp',
        'SchedulerInfoTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=['//aios/suez/sdk:hb_interface', '//aios/unittest_framework']
)
cc_test(
    name='table_reader_test',
    srcs=['TableReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/framework/mock:tablet',
        '//aios/storage/indexlib/indexlib/testlib',
        '//aios/suez/common/test:table_meta_util', '//aios/suez/table',
        '//aios/unittest_framework'
    ]
)
