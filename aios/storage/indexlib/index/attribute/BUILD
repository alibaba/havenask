load('//aios/storage:defs.bzl', 'strict_cc_library')
load('//bazel:defs.bzl', 'gen_cpp_code')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':AttributeIndexFactory']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':AttributeReader']
)
strict_cc_library(
    name='indexer',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':AttributeDiskIndexer', ':AttributeMemIndexer',
        ':MultiSliceAttributeDiskIndexer'
    ]
)
strict_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/attribute/merger:AttributeMerger']
)
strict_cc_library(
    name='modifier',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':InplaceAttributeModifier', ':PatchAttributeModifier']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':RangeDescription']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/attribute/config']
)
strict_cc_library(
    name='patch',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':AttributeFieldValue', ':AttributePatchWriter',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchFileFinder',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchIterator',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchReader',
        '//aios/storage/indexlib/index/attribute/patch:AttributePatchReaderCreator',
        '//aios/storage/indexlib/index/attribute/patch:PatchIteratorCreator'
    ]
)
strict_cc_library(
    name='util',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/attribute/merger:DocumentMergeInfoHeap',
        '//aios/storage/indexlib/index/attribute/merger:SegmentOutputMapper'
    ]
)
strict_cc_library(
    name='attr_helper',
    srcs=['AttrHelper.cpp'],
    hdrs=['AttrHelper.h'],
    visibility=[
        '//aios/storage:indexlib_extensions',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=[
        ':AttributeIteratorBase', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base/proto:querier_proto_base',
        '//aios/storage/indexlib/index/common:AttrTypeTraits',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute'
    ]
)
strict_cc_library(
    name='AttributeIndexFactory',
    deps=[
        ':AttributeDiskIndexer', ':AttributeFactory', ':AttributeMemIndexer',
        ':AttributeMetrics', ':AttributeReader', '//aios/autil:log',
        '//aios/storage/indexlib/framework:MetricsManager',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute/config:AttributeConfig',
        '//aios/storage/indexlib/index/attribute/merger:AttributeMerger'
    ],
    alwayslink=True
)
strict_cc_library(
    name='AttributeDiskIndexerCreator',
    deps=[
        ':AttributeDiskIndexer', ':AttributeFactory',
        '//aios/storage/indexlib/framework:Segment'
    ]
)
strict_cc_library(
    name='AttributeMetrics',
    deps=[
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
simple_value_default_type_lite = [('int8_t', 'Int8'), ('uint8_t', 'UInt8'),
                                  ('int16_t', 'Int16'), ('uint16_t', 'UInt16'),
                                  ('int32_t', 'Int32'), ('uint32_t', 'UInt32'),
                                  ('int64_t', 'Int64'), ('uint64_t', 'UInt64'),
                                  ('float', 'Float'), ('double', 'Double')]
template_header = '\n#include "indexlib/index/attribute/AttributeReaderFactoryRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_header_disk_indexer = '\n#include "indexlib/index/attribute/AttributeDiskIndexerFactoryRegister.h"\nnamespace indexlibv2 { namespace index {\n'
template_tail = '\n}}\n'
template1 = '\nREGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template2 = '\nREGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1});\n'
template3 = '\nREGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
template4 = '\nREGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY({0}, {1}, {2});\n'
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
    name='gen_special_single_value_reader',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header,
    template_tail=template_tail
)
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
    name='gen_simple_single_value_disk_indexer',
    element_per_file=1,
    elements_list=[
        (simple_value_default_type_lite + [('autil::uint128_t', 'UInt128')])
    ],
    template=template2,
    template_header=template_header_disk_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_single_value_disk_indexer',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header_disk_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_simple_multi_value_disk_indexer',
    element_per_file=1,
    elements_list=[(
        simple_value_default_type_lite + [('autil::MultiChar', 'MultiString')]
    )],
    template=template1,
    template_header=template_header_disk_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_multi_value_disk_indexer',
    element_per_file=1,
    elements_list=[[('double', 'Line', 'ft_line'),
                    ('double', 'Polygon', 'ft_polygon'),
                    ('double', 'Location', 'ft_location')]],
    template=template3,
    template_header=template_header_disk_indexer,
    template_tail=template_tail
)
strict_cc_library(
    name='AttributeFactory',
    srcs=[],
    hdrs=['AttributeFactory.h', 'AttributeFactoryRegister.h'],
    deps=['//aios/autil:NoCopyable', '//aios/autil:lock', '//aios/autil:log']
)
strict_cc_library(
    name='MultiSliceAttributeDiskIndexer',
    deps=[
        ':AttributeDiskIndexer', ':AttributeDiskIndexerCreator', ':SliceInfo'
    ]
)
strict_cc_library(
    name='AttributeDiskIndexer',
    srcs=([
        'AttributeDiskIndexer.cpp', 'AttributeDiskIndexerFactoryRegister.cpp'
    ] + [
        ':gen_simple_single_value_disk_indexer',
        ':gen_simple_multi_value_disk_indexer',
        ':gen_special_single_value_disk_indexer',
        ':gen_special_multi_value_disk_indexer'
    ]),
    hdrs=[
        'AttributeDiskIndexer.h', 'AttributeDiskIndexerCreator.h',
        'AttributeDiskIndexerFactoryRegister.h',
        'MultiValueAttributeDiskIndexer.h', 'SingleValueAttributeDiskIndexer.h'
    ],
    deps=[
        ':AttributeDataInfo', ':AttributeFactory', ':AttributeMetrics',
        ':MultiValueAttributeDefragSliceArray',
        ':SingleValueAttributeCompressReader',
        ':SingleValueAttributeUnCompressReader', ':SliceInfo',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/framework:MetricsWrapper',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute/format:MultiValueAttributeOffsetUpdatableFormatter',
        '//aios/storage/indexlib/index/attribute/patch:DefaultValueAttributePatch',
        '//aios/storage/indexlib/index/attribute/patch:MultiValueAttributePatchReader',
        '//aios/storage/indexlib/index/attribute/patch:SingleValueAttributePatchReader',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common:numeric_compress',
        '//aios/storage/indexlib/index/common/field_format/attribute:AttributeFieldPrinter',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeFormatter',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:FloatCompressConvertor',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfo'
    ]
)
template_header_mem_indexer = '\n#include "indexlib/index/attribute/AttributeMemIndexerFactoryRegister.h"\nnamespace indexlibv2 { namespace index {\n'
gen_cpp_code(
    name='gen_simple_single_value_mem_indexer',
    element_per_file=1,
    elements_list=[
        (simple_value_default_type_lite + [('autil::uint128_t', 'UInt128')])
    ],
    template=template2,
    template_header=template_header_mem_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_single_value_mem_indexer',
    element_per_file=1,
    elements_list=[[('uint32_t', 'Time', 'ft_time'),
                    ('uint32_t', 'Date', 'ft_date'),
                    ('uint64_t', 'Timestamp', 'ft_timestamp')]],
    template=template4,
    template_header=template_header_mem_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_simple_multi_value_mem_indexer',
    element_per_file=1,
    elements_list=[(
        simple_value_default_type_lite + [('autil::MultiChar', 'MultiString')]
    )],
    template=template1,
    template_header=template_header_mem_indexer,
    template_tail=template_tail
)
gen_cpp_code(
    name='gen_special_multi_value_mem_indexer',
    element_per_file=1,
    elements_list=[[('double', 'Line', 'ft_line'),
                    ('double', 'Polygon', 'ft_polygon'),
                    ('double', 'Location', 'ft_location')]],
    template=template3,
    template_header=template_header_mem_indexer,
    template_tail=template_tail
)
strict_cc_library(
    name='AttributeMemIndexer',
    srcs=(['AttributeMemIndexer.cpp', 'AttributeMemIndexerFactoryRegister.cpp']
          + [
              ':gen_simple_single_value_mem_indexer',
              ':gen_simple_multi_value_mem_indexer',
              ':gen_special_single_value_mem_indexer',
              ':gen_special_multi_value_mem_indexer'
          ]),
    hdrs=[
        'AttributeMemIndexer.h', 'AttributeMemIndexerCreator.h',
        'AttributeMemIndexerFactoryRegister.h',
        'MultiValueAttributeMemIndexer.h', 'SingleValueAttributeMemIndexer.h'
    ],
    deps=[
        'AttributeDataInfo', ':AttributeFactory',
        ':MultiValueAttributeMemReader', ':SingleValueAttributeMemReader',
        '//aios/autil:NoCopyable', '//aios/autil:log', '//aios/future_lite',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/framework:MetricsWrapper',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index:MemIndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='AttributeReader',
    srcs=(['AttributeReaderFactoryRegister.cpp', 'AttributeReader.cpp'] + [
        ':gen_simple_single_value_reader', ':gen_special_single_value_reader',
        ':gen_simple_multi_value_reader', ':gen_special_multi_value_reader'
    ]),
    hdrs=[
        'AttributeReader.h', 'AttributeReaderCreator.h',
        'AttributeReaderFactoryRegister.h', 'MultiValueAttributeReader.h',
        'SingleValueAttributeReader.h'
    ],
    deps=[
        ':AttributeDiskIndexer', ':AttributeDiskIndexerCreator',
        ':AttributeFactory', ':AttributeIteratorTyped', ':AttributeMemIndexer',
        ':DefaultValueAttributeMemReader', ':MultiSliceAttributeDiskIndexer',
        ':RangeDescription', ':SequenceIterator',
        ':UniqEncodeSequenceIteratorTyped', '//aios/autil:NoCopyable',
        '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index:IndexReaderParameter',
        '//aios/storage/indexlib/index/common/field_format/attribute:AttributeFieldPrinter'
    ]
)
strict_cc_library(
    name='SingleValueAttributeReaderBase',
    srcs=[],
    deps=['//aios/autil:NoCopyable']
)
strict_cc_library(
    name='SingleValueAttributeCompressReader',
    srcs=[],
    deps=[
        ':AttributeMetrics', ':SingleValueAttributeReaderBase',
        '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:data_structure'
    ]
)
strict_cc_library(
    name='SingleValueAttributeUnCompressReader',
    srcs=[],
    deps=[
        ':SingleValueAttributeReaderBase', '//aios/autil:log',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributeReadOnlyFormatter',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributeUpdatableFormatter',
        '//aios/storage/indexlib/index/common:data_structure'
    ]
)
strict_cc_library(
    name='SingleValueAttributeMemReader',
    srcs=[],
    deps=[
        ':AttributeMemReader', '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute/format:SingleValueAttributeMemFormatter',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:FloatCompressConvertor'
    ]
)
strict_cc_library(name='AttributeMemReader', srcs=[])
strict_cc_library(
    name='MultiValueAttributeMemReader',
    srcs=[],
    deps=[
        ':AttributeMemReader', '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeFormatter',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:FloatCompressConvertor'
    ]
)
strict_cc_library(
    name='AttributeDataInfo',
    deps=['//aios/autil:json', '//aios/storage/indexlib/file_system']
)
strict_cc_library(
    name='MultiValueAttributeDefragSliceArray',
    deps=[
        ':AttributeMetrics', ':MultiValueAttributeOffsetReader',
        '//aios/autil:json',
        '//aios/storage/indexlib/framework/mem_reclaimer:IIndexMemoryReclaimer',
        '//aios/storage/indexlib/index/attribute/format:MultiValueAttributeDataFormatter',
        '//aios/storage/indexlib/index/attribute/format:MultiValueAttributeOffsetUpdatableFormatter'
    ]
)
strict_cc_library(
    name='DefaultValueAttributeMemReader',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/attribute/patch:DefaultValueAttributePatch',
        '//aios/storage/indexlib/index/common/field_format/attribute:AttributeConvertorFactory'
    ]
)
strict_cc_library(
    name='MultiValueAttributeCompressOffsetReader',
    deps=[
        ':AttributeMetrics', '//aios/autil:json',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common:numeric_compress',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='MultiValueAttributeUnCompressOffsetReader',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='MultiValueAttributeOffsetReader',
    deps=[
        ':AttributeMetrics', ':MultiValueAttributeCompressOffsetReader',
        ':MultiValueAttributeUnCompressOffsetReader', '//aios/autil:json',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common:numeric_compress',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='AttributeIteratorBase',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:error_code'
    ]
)
strict_cc_library(
    name='AttributeIteratorTyped',
    srcs=[],
    deps=[
        ':AttributeIteratorBase', ':AttributeReaderTraits',
        ':DefaultValueAttributeMemReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='UniqEncodeSequenceIteratorTyped',
    deps=[
        ':AttributeIteratorBase', ':AttributeReaderTraits', '//aios/autil:log'
    ]
)
strict_cc_library(
    name='SequenceIterator',
    srcs=[],
    deps=[':AttributeDiskIndexer', ':AttributeIteratorBase']
)
strict_cc_library(
    name='AttributeReaderTraits',
    srcs=[],
    deps=[
        ':AttributeDiskIndexer', ':MultiValueAttributeMemReader',
        ':SingleValueAttributeMemReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(name='Common', srcs=[])
strict_cc_library(
    name='InplaceAttributeModifier',
    deps=[
        ':AttributeDiskIndexer', ':AttributeIndexerOrganizerUtil',
        ':AttributeMemIndexer', ':AttributeModifier', ':UpdateFieldExtractor',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/index/common:IndexerOrganizerMeta',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
strict_cc_library(
    name='AttributeModifier',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='PatchAttributeModifier',
    deps=[':AttributeModifier', ':AttributePatchWriter']
)
strict_cc_library(
    name='AttributeUpdateBitmap',
    deps=[
        ':SegmentUpdateBitmap', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='SegmentUpdateBitmap',
    deps=[
        '//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/util:Bitmap'
    ]
)
strict_cc_library(
    name='AttributePatchWriter',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute/patch:AttributeUpdater',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchWriter'
    ]
)
strict_cc_library(
    name='AttributeFieldValue',
    deps=[
        ':Constant', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:MemBuffer'
    ]
)
strict_cc_library(
    name='UpdateFieldExtractor',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log', '//aios/autil:span',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal:AttributeDocument',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/index/primary_key/config:PrimaryKeyIndexConfig'
    ]
)
strict_cc_library(
    name='SliceInfo',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Constant'
    ]
)
strict_cc_library(name='Constant', srcs=[], deps=[':Types'])
strict_cc_library(name='Types', srcs=[])
strict_cc_library(name='RangeDescription', srcs=[])
strict_cc_library(
    name='AttributeIndexerOrganizerUtil',
    deps=[
        ':AttributeDiskIndexer', ':AttributeMemIndexer',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/attribute/config:AttributeConfig',
        '//aios/storage/indexlib/index/common:IndexerOrganizerMeta',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='SingleAttributeBuilder',
    srcs=[],
    deps=[
        ':AttributeIndexerOrganizerUtil', ':UpdateFieldExtractor', ':indexer',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='AttributeBuildWorkItem',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleAttributeBuilder',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
