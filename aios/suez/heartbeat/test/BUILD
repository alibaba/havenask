cc_test(
    name='heartbeat_test',
    srcs=glob(['*.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/suez:testdata'],
    deps=[
        '//aios/autil/autil/legacy/test:json_testlib', '//aios/suez/heartbeat',
        '//aios/unittest_framework'
    ]
)
