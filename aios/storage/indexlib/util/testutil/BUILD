load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='unittest',
    testonly=True,
    srcs=[],
    hdrs=['unittest.h'],
    data=['//aios/storage/indexlib:test_log_conf'],
    deps=['//aios/unittest_framework']
)
strict_cc_library(
    name='exception_runner',
    srcs=['ExceptionRunner.cpp'],
    hdrs=['ExceptionRunner.h'],
    deps=[
        '//aios/autil:log', '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/util:Exception',
        '@com_google_googletest//:gtest'
    ]
)
