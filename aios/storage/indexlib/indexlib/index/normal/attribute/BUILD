load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'gen_cpp_code')
cc_library(
    name='metrics',
    srcs=['attribute_metrics.cpp'],
    hdrs=['attribute_metrics.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/index:index_metrics']
)
cc_library(
    name='attribute_format',
    srcs=glob(['format/*.cpp']),
    hdrs=glob(['format/*.h', 'accessor/attribute_data_info.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':metrics',
        '//aios/storage/indexlib/index/common:fs_writer_param_decider',
        '//aios/storage/indexlib/indexlib/common:field_format',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/index:data_structure',
        '//aios/storage/indexlib/indexlib/index:util'
    ]
)
cc_library(
    name='base',
    srcs=['update_field_extractor.cpp'],
    hdrs=['update_field_extractor.h'],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/document:raw_document']
)
cc_library(
    name='reader_interface',
    srcs=['accessor/attribute_iterator_base.cpp'],
    hdrs=[
        'accessor/attribute_iterator_base.h',
        'accessor/attribute_iterator_typed.h', 'accessor/attribute_reader.h',
        'accessor/pack_attribute_iterator.h',
        'accessor/pack_attribute_iterator_typed.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config'
    ]
)
cc_library(
    name='patch_reader',
    srcs=[
        'accessor/attr_field_value.cpp', 'accessor/attribute_patch_reader.cpp',
        'accessor/attribute_segment_patch_iterator_creator.cpp',
        'accessor/pack_attribute_patch_reader.cpp',
        'accessor/single_value_attribute_patch_reader.cpp',
        'accessor/var_num_attribute_patch_file.cpp'
    ],
    hdrs=[
        'accessor/attr_field_value.h', 'accessor/attribute_patch_iterator.h',
        'accessor/attribute_patch_reader.h',
        'accessor/attribute_segment_patch_iterator_creator.h',
        'accessor/multi_field_patch_iterator.h',
        'accessor/pack_attribute_patch_reader.h',
        'accessor/single_value_attribute_patch_reader.h',
        'accessor/single_value_attribute_segment_patch_iterator.h',
        'accessor/sub_doc_patch_iterator.h',
        'accessor/var_num_attribute_patch_file.h',
        'accessor/var_num_attribute_patch_reader.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':attribute_format', '//aios/storage/indexlib/indexlib:indexlib_misc',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/index/normal/util',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
cc_library(
    name='segment_reader',
    srcs=['accessor/attribute_segment_reader.cpp'],
    hdrs=[
        'accessor/attribute_segment_reader.h',
        'accessor/in_mem_single_value_attribute_reader.h',
        'accessor/in_mem_var_num_attribute_reader.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':attribute_format', ':patch_reader',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
simple_value_default_type_lite = [('int8_t', 'Int8'), ('uint8_t', 'UInt8'),
                                  ('int16_t', 'Int16'), ('uint16_t', 'UInt16'),
                                  ('int32_t', 'Int32'), ('uint32_t', 'UInt32'),
                                  ('int64_t', 'Int64'), ('uint64_t', 'UInt64'),
                                  ('float', 'Float'), ('double', 'Double')]
template_header = '\n#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory_register.h"\nnamespace indexlib { namespace index {\n'
template_tail = '\n}}\n'
template1 = '\nREGISTER_SIMPLE_MULTI_VALUE_TO_ATTRIBUTE_READER_FACTORY({0}, {1});\n'
template2 = '\nREGISTER_SIMPLE_SINGLE_VALUE_TO_ATTRIBUTE_READER_FACTORY({0}, {1});\n'
template3 = '\nREGISTER_SPECIAL_MULTI_VALUE_TO_ATTRIBUTE_READER_FACTORY({0}, {1}, {2});\n'
template4 = '\nREGISTER_SPECIAL_SINGLE_VALUE_TO_ATTRIBUTE_READER_FACTORY({0}, {1}, {2});\n'
gen_cpp_code(
    name='gen_simple_multi_value_reader',
    element_per_file=1,
    elements_list=[(
        simple_value_default_type_lite + [('autil::MultiChar', 'MultiString')]
    )],
    template=template1,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_simple_single_value_reader',
    element_per_file=1,
    elements_list=[
        (simple_value_default_type_lite + [('autil::uint128_t', 'UInt128')])
    ],
    template=template2,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_multi_value_reader',
    element_per_file=1,
    elements_list=[[('double', 'Line', 'ft_line'),
                    ('double', 'Polygon', 'ft_polygon'),
                    ('double', 'Location', 'ft_location')]],
    template=template3,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_single_value_reader',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header,
    template_tail=template_tail
)
cc_library(
    name='attribute_reader',
    srcs=(((([
        'accessor/attribute_offset_reader.cpp',
        'accessor/attribute_reader_factory.cpp',
        'accessor/join_docid_attribute_iterator.cpp',
        'accessor/join_docid_attribute_reader.cpp',
        'accessor/multi_pack_attribute_reader.cpp',
        'accessor/pack_attribute_reader.cpp',
        'accessor/section_attribute_reader_impl.cpp',
        'accessor/section_data_reader.cpp',
        'accessor/var_num_attribute_defrag_slice_array.cpp',
        'accessor/attribute_reader_factory_register.cpp'
    ] + [':gen_simple_multi_value_reader']) +
            [':gen_simple_single_value_reader']) +
           [':gen_special_multi_value_reader']) +
          [':gen_special_single_value_reader']),
    hdrs=[
        'accessor/attribute_offset_reader.h',
        'accessor/attribute_reader_creator.h',
        'accessor/attribute_reader_factory.h',
        'accessor/attribute_reader_factory_register.h',
        'accessor/attribute_reader_traits.h',
        'accessor/building_attribute_reader.h',
        'accessor/join_docid_attribute_iterator.h',
        'accessor/join_docid_attribute_reader.h',
        'accessor/multi_pack_attribute_reader.h',
        'accessor/multi_value_attribute_segment_reader.h',
        'accessor/pack_attribute_reader.h', 'accessor/patch_apply_option.h',
        'accessor/primary_key_attribute_reader.h',
        'accessor/primary_key_attribute_segment_reader.h',
        'accessor/section_attribute_reader_impl.h',
        'accessor/section_data_reader.h',
        'accessor/single_value_attribute_reader.h',
        'accessor/single_value_attribute_segment_reader.h',
        'accessor/var_num_attribute_defrag_slice_array.h',
        'accessor/var_num_attribute_reader.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':attribute_format', ':patch_reader', ':reader_interface',
        ':segment_reader',
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:InDocMultiSectionMeta',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeReaderImpl',
        '//aios/storage/indexlib/indexlib/index/normal/framework:reader'
    ]
)
cc_library(
    name='attribute_modifier',
    srcs=[
        'accessor/attribute_build_work_item.cpp',
        'accessor/attribute_modifier.cpp', 'accessor/attribute_updater.cpp',
        'accessor/attribute_updater_factory.cpp',
        'accessor/built_attribute_segment_modifier.cpp',
        'accessor/composite_join_docid_attribute_reader.cpp',
        'accessor/inplace_attribute_modifier.cpp',
        'accessor/multi_field_patch_iterator.cpp',
        'accessor/pack_attr_update_doc_size_calculator.cpp',
        'accessor/pack_field_patch_iterator.cpp',
        'accessor/pack_field_patch_work_item.cpp',
        'accessor/patch_attribute_modifier.cpp',
        'accessor/patch_iterator_creator.cpp',
        'accessor/single_field_patch_iterator.cpp',
        'accessor/single_field_patch_work_item.cpp',
        'accessor/sub_doc_patch_iterator.cpp', 'attribute_update_bitmap.cpp',
        'attribute_update_info.cpp'
    ],
    hdrs=[
        'accessor/attribute_build_work_item.h', 'accessor/attribute_modifier.h',
        'accessor/attribute_patch_work_item.h',
        'accessor/attribute_segment_modifier.h', 'accessor/attribute_updater.h',
        'accessor/attribute_updater_creator.h',
        'accessor/attribute_updater_factory.h',
        'accessor/built_attribute_segment_modifier.h',
        'accessor/composite_join_docid_attribute_reader.h',
        'accessor/date_attribute_updater.h',
        'accessor/float_attribute_updater_creator.h',
        'accessor/inplace_attribute_modifier.h',
        'accessor/pack_attr_update_doc_size_calculator.h',
        'accessor/pack_field_patch_iterator.h',
        'accessor/pack_field_patch_work_item.h',
        'accessor/patch_attribute_modifier.h', 'accessor/patch_iterator.h',
        'accessor/patch_iterator_creator.h',
        'accessor/single_field_patch_iterator.h',
        'accessor/single_field_patch_work_item.h',
        'accessor/single_value_attribute_updater.h',
        'accessor/string_attribute_updater.h',
        'accessor/time_attribute_updater.h',
        'accessor/timestamp_attribute_updater.h',
        'accessor/var_num_attribute_updater.h', 'attribute_update_bitmap.h',
        'attribute_update_info.h', 'segment_update_bitmap.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':appender', ':attribute_reader', ':base', ':container',
        ':patch_reader',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap'
    ]
)
cc_library(
    name='offline_reader',
    srcs=[],
    hdrs=[
        'accessor/uniq_encode_var_num_attribute_segment_reader_for_offline.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':attribute_reader']
)
cc_library(
    name='document_merge_heap',
    srcs=['accessor/document_merge_info_heap.cpp'],
    hdrs=[
        'accessor/document_merge_info.h', 'accessor/document_merge_info_heap.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/index:util']
)
cc_library(
    name='attribute_merger',
    srcs=[
        'accessor/attribute_merger.cpp',
        'accessor/attribute_merger_factory.cpp',
        'accessor/attribute_patch_merger.cpp',
        'accessor/attribute_patch_merger_factory.cpp',
        'accessor/dedup_pack_attribute_patch_file_merger.cpp',
        'accessor/dedup_patch_file_merger.cpp',
        'accessor/docid_join_value_attribute_merger.cpp',
        'accessor/fixed_value_attribute_merger.cpp',
        'accessor/pack_attribute_merger.cpp',
        'accessor/uniq_encoded_pack_attribute_merger.cpp'
    ],
    hdrs=[
        'accessor/attribute_merger.h', 'accessor/attribute_merger_creator.h',
        'accessor/attribute_merger_factory.h',
        'accessor/attribute_merger_resource.h',
        'accessor/attribute_patch_merger.h',
        'accessor/attribute_patch_merger_factory.h',
        'accessor/date_attribute_merger.h',
        'accessor/dedup_pack_attribute_patch_file_merger.h',
        'accessor/dedup_patch_file_merger.h',
        'accessor/docid_join_value_attribute_merger.h',
        'accessor/fixed_value_attribute_merger.h',
        'accessor/float_attribute_merger_creator.h',
        'accessor/line_attribute_merger.h',
        'accessor/location_attribute_merger.h',
        'accessor/pack_attribute_merger.h', 'accessor/patch_file_merger.h',
        'accessor/polygon_attribute_merger.h',
        'accessor/primary_key_attribute_merger.h',
        'accessor/single_value_attribute_merger.h',
        'accessor/single_value_attribute_patch_merger.h',
        'accessor/string_attribute_merger.h',
        'accessor/time_attribute_merger.h',
        'accessor/timestamp_attribute_merger.h',
        'accessor/uniq_encoded_pack_attribute_merger.h',
        'accessor/uniq_encoded_var_num_attribute_merger.h',
        'accessor/var_num_attribute_merger.h',
        'accessor/var_num_attribute_merger_creator.h',
        'accessor/var_num_attribute_patch_merger.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':attribute_modifier', ':document_merge_heap', ':offline_reader',
        ':patch_reader', '//aios/storage/indexlib/indexlib/index:patch_mergers'
    ]
)
cc_library(
    name='attribute_writer',
    srcs=[
        'accessor/attribute_fs_writer_param_decider.cpp',
        'accessor/attribute_writer.cpp',
        'accessor/attribute_writer_factory.cpp',
        'accessor/in_memory_attribute_segment_writer.cpp',
        'accessor/pack_attribute_writer.cpp'
    ],
    hdrs=[
        'accessor/attribute_fs_writer_param_decider.h',
        'accessor/attribute_writer.h', 'accessor/attribute_writer_creator.h',
        'accessor/attribute_writer_factory.h',
        'accessor/date_attribute_writer.h',
        'accessor/float_attribute_writer_creator.h',
        'accessor/in_memory_attribute_segment_writer.h',
        'accessor/line_attribute_writer.h',
        'accessor/location_attribute_writer.h',
        'accessor/multi_string_attribute_writer.h',
        'accessor/pack_attribute_writer.h',
        'accessor/polygon_attribute_writer.h',
        'accessor/single_value_attribute_writer.h',
        'accessor/string_attribute_writer.h',
        'accessor/time_attribute_writer.h',
        'accessor/timestamp_attribute_writer.h',
        'accessor/var_num_attribute_writer.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':attribute_reader', ':base', ':reader_interface', ':segment_reader',
        '//aios/storage/indexlib/indexlib/index:data_structure',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
cc_library(
    name='container',
    srcs=[
        'accessor/attribute_reader_container.cpp',
        'accessor/multi_field_attribute_reader.cpp',
        'accessor/offline_attribute_segment_reader_container.cpp'
    ],
    hdrs=[
        'accessor/attribute_reader_container.h',
        'accessor/multi_field_attribute_reader.h',
        'accessor/offline_attribute_segment_reader_container.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':attribute_reader', ':offline_reader']
)
cc_library(
    name='data_iterator',
    srcs=[
        'accessor/attribute_data_iterator.cpp',
        'accessor/var_num_attribute_data_iterator.cpp'
    ],
    hdrs=[
        'accessor/attribute_data_iterator.h',
        'accessor/single_value_data_iterator.h',
        'accessor/var_num_attribute_data_iterator.h',
        'accessor/var_num_data_iterator.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/index_base']
)
cc_library(
    name='appender',
    srcs=[
        'default_attribute_field_appender.cpp',
        'virtual_attribute_data_appender.cpp'
    ],
    hdrs=[
        'default_attribute_field_appender.h',
        'virtual_attribute_data_appender.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':attribute_writer', '//aios/storage/indexlib/indexlib/index_base']
)
