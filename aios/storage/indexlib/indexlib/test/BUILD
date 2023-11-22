load('//bazel:defs.bzl', 'gen_conf_lib')
gen_conf_lib(
    name='test_h',
    srcs=['test.h.in'],
    outs=['test.h'],
    args={
        '@DOTEST_LOGGER_CONF@': 'aios/storage/indexlib/test/logger.conf',
        '@TOP_SRCDIR@': 'aios/storage/indexlib'
    },
    include_prefix='indexlib/test'
)
cc_library(
    name='testbase_helper',
    hdrs=['multi_thread_test_base.h', 'unittest.h'],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':test_h', '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/file_system', '//aios/unittest_framework'
    ],
    alwayslink=True
)
cc_library(
    name='query',
    srcs=[
        'atomic_query.cpp', 'docid_query.cpp', 'or_query.cpp', 'query.cpp',
        'query_parser.cpp', 'range_query_parser.cpp', 'source_query.cpp'
    ],
    hdrs=[
        'atomic_query.h', 'docid_query.h', 'or_query.h', 'query.h',
        'query_parser.h', 'range_query_parser.h', 'source_query.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/index/normal/framework:reader',
        '//aios/storage/indexlib/indexlib/partition:partition_reader'
    ]
)
cc_library(
    name='document',
    srcs=[
        'ExtendDocField2IndexDocField.cpp', 'document_convertor.cpp',
        'document_creator.cpp', 'document_parser.cpp', 'raw_document.cpp',
        'result.cpp', 'result_checker.cpp', 'sub_document_extractor.cpp'
    ],
    hdrs=[
        'ExtendDocField2IndexDocField.h', 'document_convertor.h',
        'document_creator.h', 'document_parser.h', 'raw_document.h', 'result.h',
        'result_checker.h', 'sub_document_extractor.h', 'tokenize_section.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/indexlib/document:document_parser',
        '//aios/storage/indexlib/indexlib/index:kv_util'
    ]
)
cc_library(
    name='searcher',
    srcs=['kkv_searcher.cpp', 'kv_searcher.cpp', 'searcher.cpp'],
    hdrs=['kkv_searcher.h', 'kv_searcher.h', 'searcher.h'],
    copts=['-Werror'],
    implementation_deps=['//aios/storage/indexlib/table/kv_table:reader'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':document', ':query',
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/indexlib/index:kv_reader_interface',
        '//aios/storage/indexlib/indexlib/index:source',
        '//aios/storage/indexlib/indexlib/index:summary',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_reader',
        '//aios/storage/indexlib/indexlib/partition:partition_reader'
    ]
)
cc_library(
    name='schema_maker',
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/legacy/config/test:schema_maker']
)
cc_library(
    name='index_meta_maker',
    srcs=['version_maker.cpp'],
    hdrs=['version_maker.h'],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=['//aios/storage/indexlib/indexlib/index_base']
)
cc_library(
    name='single_field_partition_data_provider',
    testonly=True,
    srcs=['single_field_partition_data_provider.cpp'],
    hdrs=['single_field_partition_data_provider.h'],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':document', ':schema_maker',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
cc_library(
    name='simple_table_reader',
    hdrs=['simple_table_reader.h'],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[':document', '//aios/storage/indexlib/indexlib:indexlib_table']
)
cc_library(
    name='partition_state_machine',
    srcs=[
        'fake_partition_data_creator.cpp', 'partition_data_maker.cpp',
        'partition_metrics.cpp', 'partition_state_machine.cpp',
        'table_manager.cpp'
    ],
    hdrs=[
        'fake_partition_data_creator.h', 'partition_data_maker.h',
        'partition_metrics.h', 'partition_state_machine.h',
        'slow_dump_segment_container.h', 'table_manager.h'
    ],
    copts=['-fno-access-control', '-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        ':directory-creator', ':schema_maker', ':searcher',
        ':simple_table_reader',
        '//aios/storage/indexlib/framework:TabletDeployer',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition'
    ]
)
cc_library(
    name='directory-creator',
    srcs=['directory_creator.cpp'],
    hdrs=['directory_creator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/test',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/indexlib:indexlib_misc'
    ],
    alwayslink=True
)
cc_library(
    name='test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        ':directory-creator', ':partition_state_machine', ':schema_maker',
        ':searcher', ':simple_table_reader',
        '//aios/storage/indexlib/util/testutil:exception_runner'
    ]
)
