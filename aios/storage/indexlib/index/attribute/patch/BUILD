load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
load('//bazel:defs.bzl', 'gen_cpp_code')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/attribute:__subpackages__'
    ]
)
indexlib_cc_library(
    name='DedupPatchFileMerger',
    deps=[
        ':AttributePatchFileFinder', ':AttributePatchMergerFactory',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/index/attribute:SegmentUpdateBitmap',
        '//aios/storage/indexlib/index/common/patch:PatchFileMerger'
    ]
)
indexlib_cc_library(
    name='AttributePatchFileFinder',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common/patch:SrcDestPatchFileFinder']
)
indexlib_cc_library(
    name='AttributePatchReader',
    deps=[
        ':IAttributePatch', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos'
    ]
)
indexlib_cc_library(
    name='IAttributePatch',
    srcs=[],
    deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='DefaultValueAttributePatch',
    deps=[
        ':AttributePatchReader', ':IAttributePatch',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/common/field_format/attribute:AttributeConvertorFactory',
        '//aios/storage/indexlib/index/common/field_format/attribute:DefaultAttributeValueInitializer'
    ]
)
indexlib_cc_library(
    name='SingleValueAttributePatchReader',
    deps=[
        ':AttributePatchReader',
        '//aios/storage/indexlib/config:CompressTypeOption',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute/config:AttributeConfig',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributePatchFormatter',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos'
    ]
)
indexlib_cc_library(
    name='MultiValueAttributePatchFile',
    deps=[
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos'
    ]
)
indexlib_cc_library(
    name='MultiValueAttributePatchReader',
    srcs=[],
    deps=[
        ':AttributePatchReader', ':MultiValueAttributePatchFile',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos'
    ]
)
simple_value_default_type_lite = [('int8_t', 'Int8'), ('uint8_t', 'UInt8'),
                                  ('int16_t', 'Int16'), ('uint16_t', 'UInt16'),
                                  ('int32_t', 'Int32'), ('uint32_t', 'UInt32'),
                                  ('int64_t', 'Int64'), ('uint64_t', 'UInt64'),
                                  ('float', 'Float'), ('double', 'Double')]
template_header_updater = '\n#include "indexlib/index/attribute/patch/AttributeUpdaterFactoryRegister.h"\nnamespace indexlibv2::index {\n'
template_tail = '\n}\n'
template1 = '\nREGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template2 = '\nREGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template3 = '\nREGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
template4 = '\nREGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
gen_cpp_code(
    name='gen_simple_single_value_updater',
    element_per_file=1,
    elements_list=[
        (simple_value_default_type_lite + [('autil::uint128_t', 'UInt128')])
    ],
    template=template2,
    template_header=template_header_updater,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_single_value_updater',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header_updater,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_simple_multi_value_updater',
    element_per_file=1,
    elements_list=[(
        simple_value_default_type_lite + [('autil::MultiChar', 'MultiString')]
    )],
    template=template1,
    template_header=template_header_updater,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_multi_value_updater',
    element_per_file=1,
    elements_list=[[('double', 'Line', 'ft_line'),
                    ('double', 'Polygon', 'ft_polygon'),
                    ('double', 'Location', 'ft_location')]],
    template=template3,
    template_header=template_header_updater,
    template_tail=template_tail
)
indexlib_cc_library(
    name='AttributeUpdater',
    srcs=(((([
        'AttributeUpdater.cpp', 'AttributeUpdaterFactory.cpp',
        'AttributeUpdaterFactoryRegister.cpp'
    ] + [':gen_simple_single_value_updater']) +
            [':gen_simple_multi_value_updater']) +
           [':gen_special_single_value_updater']) +
          [':gen_special_multi_value_updater']),
    hdrs=[
        'AttributeUpdater.h', 'AttributeUpdaterCreator.h',
        'AttributeUpdaterFactory.h', 'AttributeUpdaterFactoryRegister.h',
        'MultiValueAttributeUpdater.h', 'SingleValueAttributeUpdater.h'
    ],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index/attribute:AttributeFactory',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributePatchFormatter',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
indexlib_cc_library(
    name='AttributePatchMerger',
    deps=[
        '//aios/storage/indexlib/index/attribute:SegmentUpdateBitmap',
        '//aios/storage/indexlib/index/attribute/config:AttributeConfig',
        '//aios/storage/indexlib/index/common/patch:PatchMerger'
    ]
)
indexlib_cc_library(
    name='SingleValueAttributePatchMerger',
    srcs=[],
    deps=[
        ':AttributePatchMerger', ':SingleValueAttributePatchReader',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index/attribute:SegmentUpdateBitmap',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributePatchFormatter',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchMerger'
    ]
)
indexlib_cc_library(
    name='MultiValueAttributePatchMerger',
    srcs=[],
    deps=[
        ':AttributePatchMerger', ':MultiValueAttributePatchReader',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index/attribute:SegmentUpdateBitmap',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchMerger',
        '//aios/storage/indexlib/util:MemBuffer'
    ]
)
indexlib_cc_library(
    name='AttributePatchMergerFactory',
    deps=[
        ':AttributePatchMerger', ':MultiValueAttributePatchMerger',
        ':SingleValueAttributePatchMerger', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/attribute:AttributeUpdateBitmap'
    ]
)
indexlib_cc_library(
    name='PatchIterator',
    srcs=[],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index/attribute:AttributeFieldValue'
    ]
)
indexlib_cc_library(
    name='AttributePatchIterator',
    srcs=[],
    deps=[
        ':PatchIterator', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='SingleFieldPatchIterator',
    deps=[
        ':AttributePatchFileFinder', ':AttributePatchIterator',
        ':AttributePatchReader', ':AttributePatchReaderCreator',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/framework:Segment'
    ]
)
indexlib_cc_library(
    name='MultiFieldPatchIterator',
    deps=[
        ':AttributePatchIterator', ':SingleFieldPatchIterator',
        '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment'
    ]
)
indexlib_cc_library(
    name='PatchIteratorCreator',
    deps=[
        ':MultiFieldPatchIterator', ':PatchIterator', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment'
    ]
)
indexlib_cc_library(
    name='AttributePatchReaderCreator',
    deps=[
        ':MultiValueAttributePatchReader', ':SingleValueAttributePatchReader',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index/common:FieldTypeTraits'
    ]
)
