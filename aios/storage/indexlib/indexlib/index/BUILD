load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'gen_cpp_code')
load('//bazel:bundle.bzl', 'bundle_files')
value_type_list = [
    'ft_int8', 'ft_int16', 'ft_int32', 'ft_int64', 'ft_uint8', 'ft_uint16',
    'ft_uint32', 'ft_uint64', 'ft_float', 'ft_double', 'ft_byte1', 'ft_byte2',
    'ft_byte3', 'ft_byte4', 'ft_byte5', 'ft_byte6', 'ft_byte7', 'ft_byte8'
]
template = '\n#include "indexlib/index/kv/hash_table_fix_creator_impl.h"\n\nnamespace indexlib::index {{\nHASHTABLEFIXCREATOR_EXTERN_TEMPLATE_INTERNAL({0});\n}}\n'
gen_cpp_code(
    'gen_hash_table_fix_creator_impl', [value_type_list], '', template, '', 5
)
cc_library(
    name='util',
    srcs=glob(['util/*.cpp']),
    hdrs=glob(['util/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/kmonitor:kmonitor_cpp_adapter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/indexlib:indexlib_codegen',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document:raw_document',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='calculator',
    srcs=glob(['calculator/*.cpp']),
    hdrs=glob(['calculator/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':index', ':kkv', ':kv', ':util']
)
cc_library(
    name='data_structure',
    srcs=glob(['data_structure/*.cpp'],
              exclude=['data_structure/*merger*.cpp']),
    hdrs=glob(['data_structure/*.h'], exclude=['data_structure/*merger*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':util', '//aios/storage/indexlib/index/common:fs_writer_param_decider',
        '//aios/storage/indexlib/util/metrics:progress_metrics'
    ]
)
cc_library(
    name='index_external',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
cc_library(
    name='trie',
    srcs=glob(['normal/trie/*.cpp']),
    hdrs=glob(['normal/trie/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//visibility:public'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/indexlib/index/normal/primarykey'
    ]
)
cc_library(
    name='accessor',
    srcs=(
        glob(['normal/accessor/*.cpp'],
             exclude=[
                 'normal/accessor/patch_merger.cpp',
                 'normal/accessor/patch_file_merger.cpp'
             ]) + ['data_structure/var_len_data_merger.cpp']
    ),
    hdrs=(
        glob(['normal/accessor/*.h']) +
        ['data_structure/var_len_data_merger.h']
    ),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    deps=[
        '//aios/storage/indexlib/indexlib/index/normal/attribute:document_merge_heap',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:patch_reader',
        '//aios/storage/indexlib/indexlib/index/normal/framework:index_framework'
    ]
)
cc_library(
    name='patch_mergers',
    srcs=[
        'normal/accessor/patch_file_merger.cpp',
        'normal/accessor/patch_merger.cpp'
    ],
    hdrs=[
        'normal/accessor/patch_file_merger.h', 'normal/accessor/patch_merger.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/index/normal/util',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
cc_library(
    name='summary',
    srcs=glob(['normal/summary/*.cpp']),
    hdrs=glob(['normal/summary/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':accessor', '//aios/autil:bloom_filter',
        '//aios/storage/indexlib/index/primary_key:reader',
        '//aios/storage/indexlib/index/summary:reader',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_reader'
    ]
)
cc_library(
    name='source',
    srcs=glob(['normal/source/*.cpp']),
    hdrs=glob(['normal/source/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':accessor']
)
cc_library(
    name='segment_metrics_updater',
    srcs=glob(['segment_metrics_updater/*.cpp']),
    hdrs=glob(['segment_metrics_updater/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':index', ':util', '//aios/autil:const_string_util']
)
cc_library(
    name='index_metrics',
    srcs=['normal/inverted_index/index_metrics.cpp'],
    hdrs=['normal/inverted_index/index_metrics.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/index/normal/util']
)
cc_library(
    name='index',
    srcs=glob([
        'merger_util/*.cpp', 'merger_util/reclaim_map/*.cpp',
        'merger_util/truncate/*.cpp', 'normal/*.cpp',
        'normal/adaptive_bitmap/*.cpp', 'normal/inverted_index/accessor/*.cpp',
        'normal/inverted_index/customized_index/*.cpp',
        'normal/inverted_index/builtin_index/bitmap/*.cpp',
        'normal/inverted_index/builtin_index/date/*.cpp',
        'normal/inverted_index/builtin_index/number/*.cpp',
        'normal/inverted_index/builtin_index/expack/*.cpp',
        'normal/inverted_index/builtin_index/pack/*.cpp',
        'normal/inverted_index/builtin_index/range/*.cpp',
        'normal/inverted_index/builtin_index/text/*.cpp',
        'normal/inverted_index/builtin_index/string/*.cpp',
        'normal/inverted_index/builtin_index/spatial/*.cpp',
        'normal/inverted_index/builtin_index/dynamic/*.cpp'
    ]),
    hdrs=glob([
        'merger_util/*.h', 'merger_util/reclaim_map/*.h',
        'merger_util/truncate/*.h', '*.h', 'data_structure/*.h', 'normal/*.h',
        'normal/adaptive_bitmap/*.h', 'normal/inverted_index/*.h',
        'normal/inverted_index/accessor/*.h',
        'normal/inverted_index/customized_index/*.h',
        'normal/inverted_index/builtin_index/bitmap/*.h',
        'normal/inverted_index/builtin_index/date/*.h',
        'normal/inverted_index/builtin_index/number/*.h',
        'normal/inverted_index/builtin_index/expack/*.h',
        'normal/inverted_index/builtin_index/pack/*.h',
        'normal/inverted_index/builtin_index/range/*.h',
        'normal/inverted_index/builtin_index/text/*.h',
        'normal/inverted_index/builtin_index/string/*.h',
        'normal/inverted_index/builtin_index/spatial/*.h',
        'normal/inverted_index/builtin_index/dynamic/*.h'
    ]),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//visibility:public'],
    deps=[
        ':accessor', ':data_structure', ':patch_mergers', ':source', ':summary',
        ':trie', ':util', '//aios/autil:const_string_util',
        '//aios/autil:allocator', '//aios/kmonitor:kmonitor_cpp_adapter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/common/field_format/date:DateQueryParser',
        '//aios/storage/indexlib/index/inverted_index:BuildingIndexReader',
        '//aios/storage/indexlib/index/inverted_index:CompositePostingIterator',
        '//aios/storage/indexlib/index/inverted_index:IndexDataWriter',
        '//aios/storage/indexlib/index/inverted_index:IndexIterator',
        '//aios/storage/indexlib/index/inverted_index:IndexOutputSegmentResource',
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index:KeyIteratorTyped',
        '//aios/storage/indexlib/index/inverted_index:MultiSegmentPostingIterator',
        '//aios/storage/indexlib/index/inverted_index:OnDiskIndexIterator',
        '//aios/storage/indexlib/index/inverted_index:OneDocMerger',
        '//aios/storage/indexlib/index/inverted_index:PositionIteratorTyped',
        '//aios/storage/indexlib/index/inverted_index:PostingWriterImpl',
        '//aios/storage/indexlib/index/inverted_index:SeekAndFilterIterator',
        '//aios/storage/indexlib/index/inverted_index:SegmentPostings',
        '//aios/storage/indexlib/index/inverted_index:SegmentTermInfo',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:TimeRangeInfo',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/range:RangeInfo',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingDumper',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:InMemBitmapIndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:OnDiskBitmapIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:BufferedByteSlice',
        '//aios/storage/indexlib/index/inverted_index/format:BufferedByteSliceReader',
        '//aios/storage/indexlib/index/inverted_index/format:BufferedSegmentIndexDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:ComplexDocid',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineFormatter',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlinePostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:DocListEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:DocListEncoderImproved',
        '//aios/storage/indexlib/index/inverted_index/format:DocListFormat',
        '//aios/storage/indexlib/index/inverted_index/format:DocListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:DocListSkipListFormat',
        '//aios/storage/indexlib/index/inverted_index/format:FlushInfo',
        '//aios/storage/indexlib/index/inverted_index/format:InMemDictInlineDocListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemDocListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemPositionListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemPostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:NormalInDoc',
        '//aios/storage/indexlib/index/inverted_index/format:PatchFormat',
        '//aios/storage/indexlib/index/inverted_index/format:PositionBitmapReader',
        '//aios/storage/indexlib/index/inverted_index/format:PositionBitmapWriter',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormat',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:PositionSkipListFormat',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoderImpl',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormat',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListOptimizeUtil',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:SkipListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/spatial:SpatialPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader',
        '//aios/storage/indexlib/index/primary_key:reader',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document:raw_document',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:reader_interface',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/index/normal/framework:index_framework',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/format:index_format_option',
        '//aios/storage/indexlib/indexlib/index/normal/primarykey',
        '//aios/storage/indexlib/indexlib/index/normal/util',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/index/ann/aitheta2/util/parallel_merge:ParallelReduceMeta'
    ],
    alwayslink=True
)
filegroup(
    name='index_headers',
    srcs=glob(['kv/*.h', 'kkv/*.h']),
    visibility=['//aios/storage/indexlib:__subpackages__']
)
bundle_files(
    name='index_source_code_for_codegen',
    srcs=glob(['kv/*.h', 'kkv/*.h', 'kv/*.cpp', 'kkv/*.cpp']),
    prefix='usr/local/codegen_source_code/indexlib/index',
    strip_prefix='.',
    visibility=['//aios/storage/indexlib:__subpackages__']
)
cc_library(
    name='kv_metrics',
    hdrs=['kv/kv_metrics_collector.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/indexlib/util:util_all']
)
cc_library(
    name='kv_util',
    srcs=[],
    hdrs=['kv/kv_define.h', 'kv/multi_region_special_value.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':kv_metrics', ':util',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common:field_format',
        '//aios/storage/indexlib/indexlib/config'
    ],
    alwayslink=True
)
cc_library(
    name='kv_reader_interface',
    srcs=['kv/kv_index_options.cpp', 'kv/kv_reader.cpp'],
    hdrs=['kv/kv_index_options.h', 'kv/kv_read_options.h', 'kv/kv_reader.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':kv_metrics', ':kv_util',
        '//aios/storage/indexlib/index/common:AttributeValueTypeTraits',
        '//aios/storage/indexlib/index/kv:reader',
        '//aios/storage/indexlib/indexlib:indexlib_codegen',
        '//aios/storage/indexlib/indexlib/common:multi_region_rehasher',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='kv',
    srcs=(
        glob(['kv/*.cpp'],
             exclude=['kv/kv_reader.cpp', 'kv/kv_index_options.cpp']) +
        [':gen_hash_table_fix_creator_impl']
    ),
    hdrs=glob(['kv/*.h'],
              exclude=[
                  'kv/kv_reader.h', 'kv/kv_index_options.h',
                  'kv/kv_read_options.h', 'kv/kv_metrics_collector.h'
              ]),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//visibility:public'],
    deps=[
        ':kv_metrics', ':kv_reader_interface', ':util',
        '//aios/kmonitor:kmonitor_cpp_adapter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/indexlib:indexlib_codegen',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document:raw_document',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_writer',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/partition:on-disk-partition-data',
        '//aios/storage/indexlib/util/metrics:progress_metrics'
    ],
    alwayslink=True
)
cc_library(
    name='kkv',
    srcs=glob(['kkv/*.cpp']),
    hdrs=glob(['kkv/*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index',
    visibility=['//visibility:public'],
    deps=[
        ':kv', '//aios/kmonitor:kmonitor_cpp_adapter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/indexlib:indexlib_codegen',
        '//aios/storage/indexlib/indexlib:indexlib_h',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document:raw_document',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:reader_interface',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='index-all',
    copts=['-Werror'],
    linkstatic=True,
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':index', ':kkv', ':kv'],
    alwayslink=True
)
