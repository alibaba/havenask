cc_library(
    name='testlib',
    srcs=['indexlib_partition_creator.cpp'],
    hdrs=['indexlib_partition_creator.h', 'mock_index_partition.h'],
    copts=['-Werror'],
    include_prefix='indexlib/testlib',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document',
        '//aios/storage/indexlib/indexlib/test:document',
        '//aios/storage/indexlib/indexlib/test:schema_maker',
        '//aios/storage/indexlib/indexlib/test:test_h'
    ],
    alwayslink=True
)
cc_library(
    name='fake-partition-reader',
    srcs=[
        'fake_bitmap_index_reader.cpp', 'fake_bitmap_posting_iterator.cpp',
        'fake_in_doc_position_iterator.cpp', 'fake_in_doc_position_state.cpp',
        'fake_in_doc_section_meta.cpp', 'fake_index_partition_reader.cpp',
        'fake_partition_reader_snapshot_creator.cpp',
        'fake_posting_iterator.cpp', 'fake_posting_maker.cpp',
        'fake_text_index_reader.cpp'
    ],
    hdrs=[
        'fake_attribute_reader.h', 'fake_bitmap_index_reader.h',
        'fake_bitmap_posting_iterator.h', 'fake_in_doc_position_iterator.h',
        'fake_in_doc_position_state.h', 'fake_in_doc_section_meta.h',
        'fake_index_partition.h', 'fake_index_partition_reader.h',
        'fake_index_partition_writer.h', 'fake_index_reader.h',
        'fake_multi_value_attribute_reader.h', 'fake_online_partition.h',
        'fake_partition_reader_snapshot.h',
        'fake_partition_reader_snapshot_creator.h', 'fake_posting_iterator.h',
        'fake_posting_maker.h', 'fake_primary_key_index_reader.h',
        'fake_section_attribute_reader.h', 'fake_summary_reader.h',
        'fake_text_index_reader.h'
    ],
    copts=['-Werror'],
    implementation_deps=[
        '//aios/storage/indexlib/framework:Tablet',
        '//aios/storage/indexlib/table/normal_table:table'
    ],
    include_prefix='indexlib/testlib',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/index/normal/framework:index_framework',
        '//aios/storage/indexlib/indexlib/partition:partition_interface',
        '//aios/storage/indexlib/indexlib/partition:reader_sdk'
    ]
)
