load('//aios/storage:defs.bzl', 'strict_cc_library')
load('//bazel:defs.bzl', 'gen_cpp_code')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/attribute:__subpackages__'
    ]
)
simple_value_default_type_lite = [('int8_t', 'Int8'), ('uint8_t', 'UInt8'),
                                  ('int16_t', 'Int16'), ('uint16_t', 'UInt16'),
                                  ('int32_t', 'Int32'), ('uint32_t', 'UInt32'),
                                  ('int64_t', 'Int64'), ('uint64_t', 'UInt64'),
                                  ('float', 'Float'), ('double', 'Double')]
template_header = '\n#include "indexlib/index/attribute/merger/AttributeMergerFactoryRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_tail = '\n}}\n'
template1 = '\nREGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template2 = '\nREGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template3 = '\nREGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
template4 = '\nREGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
gen_cpp_code(
    name='gen_simple_single_value_merger',
    element_per_file=1,
    elements_list=[
        (simple_value_default_type_lite + [('autil::uint128_t', 'UInt128')])
    ],
    template=template2,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_single_value_merger',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_simple_multi_value_merger',
    element_per_file=1,
    elements_list=[(
        simple_value_default_type_lite + [('autil::MultiChar', 'MultiString')]
    )],
    template=template1,
    template_header=template_header,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_multi_value_merger',
    element_per_file=1,
    elements_list=[[('double', 'Line', 'ft_line'),
                    ('double', 'Polygon', 'ft_polygon'),
                    ('double', 'Location', 'ft_location')]],
    template=template3,
    template_header=template_header,
    template_tail=template_tail
)
strict_cc_library(
    name='AttributeMerger',
    srcs=((((['AttributeMerger.cpp', 'AttributeMergerFactoryRegister.cpp'] +
             [':gen_simple_single_value_merger']) +
            [':gen_simple_multi_value_merger']) +
           [':gen_special_single_value_merger']) +
          [':gen_special_multi_value_merger']),
    hdrs=[
        'AttributeMerger.h', 'AttributeMergerCreator.h',
        'AttributeMergerFactoryRegister.h', 'MultiValueAttributeMerger.h',
        'MultiValueAttributeMergerCreator.h', 'SingleValueAttributeMerger.h',
        'UniqEncodedMultiValueAttributeMerger.h'
    ],
    deps=[
        'SegmentOutputMapper', ':DocumentMergeInfoHeap',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute:AttributeDataInfo',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/attribute:MultiSliceAttributeDiskIndexer',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributeFormatter',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributeUpdatableFormatter',
        '//aios/storage/indexlib/index/attribute/format:SingleValueDataAppender',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchReader',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchReaderCreator',
        '//aios/storage/indexlib/index/attribute/patch:DedupPatchFileMerger',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/common/patch:PatchMerger'
    ]
)
strict_cc_library(
    name='DocumentMergeInfo',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(
    name='DocumentMergeInfoHeap',
    deps=[
        ':DocumentMergeInfo', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
strict_cc_library(
    name='SegmentOutputMapper',
    srcs=[],
    deps=[
        ':DocumentMergeInfo', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index:DocMapper'
    ]
)
