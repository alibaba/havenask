cc_library(
    name='format',
    hdrs=[
        'block_primary_key_formatter.h', 'hash_primary_key_formatter.h',
        'ordered_primary_key_iterator.h', 'primary_key_formatter.h',
        'primary_key_formatter_creator.h', 'primary_key_hash_table.h',
        'primary_key_iterator.h', 'primary_key_loader.h', 'primary_key_pair.h',
        'primary_key_segment_formatter.h', 'sorted_primary_key_formatter.h'
    ],
    include_prefix='indexlib/index/normal/primarykey',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/common/block_array:reader',
        '//aios/storage/indexlib/index/common/block_array:writer',
        '//aios/storage/indexlib/indexlib/index:util'
    ]
)
cc_library(
    name='reader',
    srcs=[
        'combine_segments_primary_key_load_strategy.cpp',
        'default_primary_key_load_strategy.cpp',
        'legacy_primary_key_reader.cpp', 'primary_key_load_plan.cpp',
        'primary_key_load_strategy_creator.cpp', 'segment_data_adapter.cpp'
    ],
    hdrs=[
        'block_primary_key_pair_segment_iterator.h',
        'combine_segments_primary_key_iterator.h',
        'combine_segments_primary_key_load_strategy.h',
        'composite_primary_key_reader.h', 'default_primary_key_load_strategy.h',
        'hash_primary_key_iterator.h',
        'in_mem_primary_key_segment_reader_typed.h',
        'legacy_primary_key_reader.h', 'legacy_primary_key_reader_interface.h',
        'on_disk_ordered_primary_key_iterator.h', 'primary_key_index_reader.h',
        'primary_key_iterator_creator.h', 'primary_key_load_plan.h',
        'primary_key_load_strategy.h', 'primary_key_load_strategy_creator.h',
        'primary_key_merge_iterator.h', 'primary_key_pair_segment_iterator.h',
        'primary_key_posting_iterator.h', 'primary_key_segment_reader_typed.h',
        'segment_data_adapter.h', 'sequential_primary_key_iterator.h',
        'sorted_primary_key_pair_segment_iterator.h'
    ],
    include_prefix='indexlib/index/normal/primarykey',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':format', '//aios/storage/indexlib/index/common/block_array:reader',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/primary_key:primary_key_reader',
        '//aios/storage/indexlib/index/primary_key:util',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_reader',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/index/normal/framework:reader'
    ]
)
cc_library(
    name='writer',
    hdrs=glob([
        'primary_key_index_dumper.h', 'primary_key_writer.h',
        'primary_key_writer_typed.h'
    ]),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/primarykey',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':format',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_writer',
        '//aios/storage/indexlib/indexlib/index/normal/framework:writer'
    ]
)
cc_library(
    name='merger',
    srcs=glob([]),
    hdrs=glob([
        'primary_key_index_merger_typed.h', 'primary_key_merge_iterator.h'
    ]),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/primarykey',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':format',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_merger',
        '//aios/storage/indexlib/indexlib/index/normal/framework:merger'
    ]
)
cc_library(
    name='primarykey',
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/primarykey',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':merger', ':reader', ':writer']
)
