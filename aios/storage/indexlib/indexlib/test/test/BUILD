load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='test-headers',
    testonly=1,
    hdrs=[
        'document_creator_unittest.h', 'document_parser_unittest.h',
        'partition_state_machine_unittest.h', 'schema_maker_unittest.h',
        'single_field_partition_data_provider_unittest.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/test/test'
)
cc_fast_test(
    name='unittest',
    srcs=[
        'document_creator_unittest.cpp', 'document_parser_unittest.cpp',
        'partition_state_machine_unittest.cpp', 'schema_maker_unittest.cpp',
        'single_field_partition_data_provider_unittest.cpp'
    ],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':test-headers', '//aios/storage/indexlib/indexlib/test:document',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine',
        '//aios/storage/indexlib/indexlib/test:schema_maker',
        '//aios/storage/indexlib/indexlib/test:single_field_partition_data_provider',
        '//aios/storage/indexlib/indexlib/test:testbase_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
