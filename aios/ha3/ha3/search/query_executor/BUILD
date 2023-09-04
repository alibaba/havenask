package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='ha3_query_executor_headers',
    hdrs=glob(['*.h']),
    include_prefix='ha3/search',
    deps=[
        '//aios/ha3/ha3/common:common_def',
        '//aios/ha3/ha3/common/query:ha3_query_headers',
        '//aios/storage/indexlib/table/normal_table:table',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata'
    ]
)
cc_library(
    name='ha3_query_executor',
    srcs=glob(['*.cpp']),
    deps=[
        ':ha3_query_executor_headers',
        '//aios/ha3/ha3/common:timeout_terminator',
        '//aios/ha3/ha3/common/query:ha3_query',
        '//aios/ha3/ha3/search/filter:ha3_filter_headers',
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk',
        '//aios/storage/indexlib/table/normal_table:table',
        '//aios/suez_turing/suez/turing/expression/provider/matchdata'
    ]
)
cc_library(
    name='ha3_query_executor_testlib',
    testonly=True,
    srcs=[
        'test/FakeQueryExecutor.cpp', 'test/FakeTimeoutTerminator.cpp',
        'test/IndexPartitionReaderWrapperCreator.cpp',
        'test/QueryExecutorConstructor.cpp', 'test/QueryExecutorMock.cpp',
        'test/QueryExecutorTestHelper.cpp'
    ],
    hdrs=[
        'test/FakeQueryExecutor.h', 'test/FakeTimeoutTerminator.h',
        'test/IndexPartitionReaderWrapperCreator.h',
        'test/QueryExecutorConstructor.h', 'test/QueryExecutorMock.h',
        'test/QueryExecutorTestHelper.h'
    ],
    include_prefix='ha3/search',
    visibility=['//visibility:public'],
    deps=[
        ':ha3_query_executor', '//aios/ha3/ha3/common:timeout_terminator',
        '//aios/ha3/ha3_sdk/testlib/index:ha3_sdk_testlib_index'
    ]
)
