package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='message_writer',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/resource',
    deps=[
        '//aios/apps/facility/swift:swift_client',
        '//aios/sql/common:sql_common', '//aios/sql/common:sql_log',
        '//aios/storage/indexlib:interface',
        '//aios/suez:remote_table_writer_sdk'
    ],
    alwayslink=True
)
cc_test(
    name='message_writer_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    flaky=True,
    deps=[
        ':message_writer',
        '//aios/apps/facility/swift/client/fake_client:fake_client_minimal',
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/navi:navi_log_testlib', '//aios/unittest_framework'
    ]
)
