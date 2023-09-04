load('@rules_cc//cc:defs.bzl', 'cc_test')
package(
    default_visibility=[
        '//aios/apps/facility/build_service/build_service/web_toolkit/base/test:__subpackages__'
    ]
)
cc_test(
    name='web_unittest',
    srcs=['SimpleHttpServerTest.cpp'],
    copts=['-fno-access-control', '-Wno-suggest-override'],
    data=['testdata/demo.html', 'testdata/monitor.html'],
    shard_count=5,
    deps=[
        '//aios/apps/facility/build_service:bs_testbase',
        '//aios/apps/facility/build_service:web_toolkit'
    ]
)
