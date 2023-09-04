cc_test(
    name='suez_worker_test',
    srcs=glob(['*.cpp', '*.h']),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        '//aios/network/gig:multi_call',
        '//aios/storage/indexlib/table/kv_table/test:kv_table_test_helper',
        '//aios/suez/common/test:table_meta_util',
        '//aios/suez/table/test:suez_mock_partition',
        '//aios/suez/worker:suez_worker_lib', '//aios/unittest_framework'
    ]
)
