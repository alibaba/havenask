cc_test(
    name='suez_wal_test',
    srcs=glob(['*.cpp', '*.h']),
    copts=['-fno-access-control'],
    deps=[
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/suez/table/wal', '//aios/unittest_framework'
    ]
)
