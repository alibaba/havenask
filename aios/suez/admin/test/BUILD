cc_test(
    name='suez_admin_test',
    srcs=glob(['*.cpp', '*.h']),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    shard_count=5,
    deps=[
        '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/suez/admin:suez_admin', '//aios/unittest_framework'
    ]
)
