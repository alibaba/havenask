package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='suez_mock_partition',
    hdrs=['MockSuezPartition.h'],
    include_prefix='suez/table/test',
    visibility=['//visibility:public'],
    deps=['//aios/suez/table']
)
cc_test(
    name='suez_table_test',
    srcs=(
        glob(['*.cpp', '*.h'],
             exclude=[
                 'ZkLeaderElectionManagerTest.cpp',
                 'ZkVersionSynchronizerTest.cpp', 'SuezTabletPartitionTest.cpp',
                 'TabletAdapterTest.cpp', 'ReadWriteTableManagerTest.cpp'
             ]) + []
    ),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    shard_count=3,
    deps=[
        ':mock_wal_strategy', ':suez_mock_partition',
        '//aios/apps/facility/build_service:mock_builder_v2',
        '//aios/apps/facility/build_service:mock_processor',
        '//aios/apps/facility/build_service/build_service/reader/test:bs_reader_test_helper',
        '//aios/storage/indexlib/framework:tablet',
        '//aios/storage/indexlib/framework/mock:tablet',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/suez/common/test:table_meta_util', '//aios/suez/table',
        '//aios/unittest_framework'
    ]
)
cc_test(
    name='suez_table_zk_related_test',
    srcs=['ZkLeaderElectionManagerTest.cpp', 'ZkVersionSynchronizerTest.cpp'],
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    flaky=True,
    deps=[
        ':suez_mock_partition',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/suez/common/test:table_meta_util', '//aios/suez/table',
        '//aios/unittest_framework'
    ]
)
cc_test(
    name='suez_bs_version_synchronizer_test',
    srcs=['BsVersionSynchronizerTest.cpp'],
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        '//aios/autil:env_util', '//aios/suez/common/test:table_meta_util',
        '//aios/suez/table', '//aios/unittest_framework'
    ]
)
cc_library(
    name='mock_wal_strategy',
    hdrs=['MockWALStrategy.h'],
    copts=['-fno-access-control'],
    include_prefix='suez/table/test',
    deps=['//aios/suez/table/wal', '//aios/unittest_framework']
)
