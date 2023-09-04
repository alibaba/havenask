load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='table_test_helper',
    testonly=True,
    srcs=[
        'DocumentParser.cpp', 'RawDocument.cpp', 'ResultChecker.cpp',
        'TableTestHelper.cpp'
    ],
    hdrs=[
        'DocumentParser.h', 'LocalTabletMergeControllerForTest.h',
        'RawDocument.h', 'Result.h', 'ResultChecker.h', 'TableTestHelper.h'
    ],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Tablet',
        '//aios/storage/indexlib/framework:TabletDeployer',
        '//aios/storage/indexlib/framework/test:tablet_test_helper',
        '//aios/storage/indexlib/table/index_task:LocalTabletMergeController'
    ]
)
