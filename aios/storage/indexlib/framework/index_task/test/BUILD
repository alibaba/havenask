load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='FakeResource',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskResourceCreator',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource'
    ]
)
strict_cc_fast_test(
    name='index_task_unittest',
    srcs=[
        'IndexOperationDescriptionTest.cpp', 'IndexOperationTest.cpp',
        'IndexTaskHistoryTest.cpp', 'IndexTaskResourceManagerTest.cpp',
        'IndexTaskTest.cpp', 'LocalExecuteEngineTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        ':FakeResource',
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/framework:TabletLoader',
        '//aios/storage/indexlib/framework:TabletReader',
        '//aios/storage/indexlib/framework:TabletWriter',
        '//aios/storage/indexlib/framework:VersionCommitter',
        '//aios/storage/indexlib/framework/index_task:IIndexOperationCreator',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskPlanCreator',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContextCreator',
        '//aios/storage/indexlib/framework/index_task:IndexTaskHistory',
        '//aios/storage/indexlib/framework/index_task:IndexTaskPlan',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/framework/index_task:LocalExecuteEngine',
        '//aios/storage/indexlib/framework/index_task/testlib:trivial_task',
        '//aios/storage/indexlib/framework/mock:MockTabletFactory',
        '//aios/storage/indexlib/util/memory_control',
        '//aios/unittest_framework'
    ]
)
