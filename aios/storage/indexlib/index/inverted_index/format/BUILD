load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='ComplexDocid', srcs=[], deps=['//aios/storage/indexlib/base:Types']
)
strict_cc_library(name='PatchFormat', srcs=[])
strict_cc_library(
    name='BufferedByteSlice',
    deps=[
        ':FlushInfo', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/common:MultiValue',
        '//aios/storage/indexlib/index/common:ShortBuffer'
    ]
)
strict_cc_library(name='FlushInfo', srcs=[])
strict_cc_library(
    name='DocListFormat',
    deps=[
        ':DocListFormatOption', ':DocListSkipListFormat',
        '//aios/storage/indexlib/index/common:MultiValue',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='DocListSkipListFormat',
    deps=[
        ':DocListFormatOption',
        '//aios/storage/indexlib/index/common:AtomicValueTyped',
        '//aios/storage/indexlib/index/common:MultiValue',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider'
    ]
)
strict_cc_library(
    name='DocListFormatOption',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='PositionListFormatOption',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='PositionListFormat',
    deps=[
        ':PositionListFormatOption', ':PositionSkipListFormat',
        '//aios/storage/indexlib/index/common:MultiValue',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='PositionSkipListFormat',
    deps=[
        ':PositionListFormatOption',
        '//aios/storage/indexlib/index/common:AtomicValueTyped',
        '//aios/storage/indexlib/index/common:MultiValue',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider'
    ]
)
strict_cc_library(
    name='PostingFormat',
    deps=[':DocListFormat', ':PositionListFormat', ':PostingFormatOption']
)
strict_cc_library(
    name='PostingFormatOption',
    deps=[
        ':DocListFormatOption', ':PositionListFormatOption',
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='IndexFormatOption',
    deps=[
        ':PostingFormatOption', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexUtil',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
strict_cc_library(name='BufferedByteSliceReader', deps=[':BufferedByteSlice'])
strict_cc_library(
    name='ShortListOptimizeUtil', srcs=[], deps=[':IndexFormatOption']
)
strict_cc_library(
    name='NormalInDoc',
    srcs=[
        'InDocPositionIteratorState.cpp', 'NormalInDocPositionIterator.cpp',
        'NormalInDocState.cpp', 'PositionListSegmentDecoder.cpp'
    ],
    hdrs=[
        'InDocPositionIteratorState.h', 'NormalInDocPositionIterator.h',
        'NormalInDocState.h', 'PositionListSegmentDecoder.h'
    ],
    deps=[
        ':PositionBitmapReader', ':ShortListOptimizeUtil',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:MultiSectionMeta',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/index/inverted_index:InDocPositionState',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:PairValueSkipListReader',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeReaderImpl',
        '//aios/storage/indexlib/util:object_pool'
    ]
)
strict_cc_library(
    name='InMemPositionListDecoder',
    deps=[':BufferedByteSliceReader', ':NormalInDoc']
)
strict_cc_library(
    name='InMemPostingDecoder',
    deps=[':InMemDocListDecoder', ':InMemPositionListDecoder']
)
strict_cc_library(
    name='ShortListSegmentDecoder',
    deps=[
        ':BufferedSegmentIndexDecoder',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(
    name='SkipListSegmentDecoder',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder'
    ]
)
strict_cc_library(
    name='ShardingIndexHasher',
    deps=[
        '//aios/storage/indexlib/document/normal:Token',
        '//aios/storage/indexlib/index/common:DictHasher',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexUtil'
    ]
)
strict_cc_library(
    name='PostingDecoderImpl',
    deps=[
        ':DictInlineDecoder', ':DictInlineFormatter', ':PostingDecoder',
        '//aios/storage/indexlib/index/common/numeric_compress:EncoderProvider',
        '//aios/storage/indexlib/index/common/numeric_compress:NewPfordeltaIntEncoder'
    ]
)
strict_cc_library(name='PostingDecoder', srcs=[], deps=[':TermMeta'])
strict_cc_library(
    name='PositionListEncoder',
    deps=[
        ':BufferedByteSlice', ':InMemPositionListDecoder',
        ':PositionListFormat', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:BufferedSkipListWriter',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:InMemPairValueSkipListReader'
    ]
)
strict_cc_library(
    name='PositionBitmapWriter',
    deps=[
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/util:ExpandableBitmap',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='PositionBitmapReader',
    deps=[
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='InMemDocListDecoder',
    deps=[
        ':BufferedByteSlice', ':BufferedByteSliceReader',
        ':BufferedSegmentIndexDecoder',
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:SkipListReader'
    ]
)
strict_cc_library(
    name='InMemDictInlineDocListDecoder',
    deps=[':DictInlinePostingDecoder', ':InMemDocListDecoder']
)
strict_cc_library(
    name='BufferedSegmentIndexDecoder',
    srcs=[],
    deps=['//aios/storage/indexlib/file_system:byte_slice_rw']
)
strict_cc_library(
    name='DictInlineDecoder',
    srcs=[],
    deps=[
        ':ShortListOptimizeUtil',
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(
    name='DictInlineEncoder',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor'
    ]
)
strict_cc_library(
    name='DictInlineFormatter',
    deps=[':DictInlineDecoder', ':DictInlineEncoder', ':PostingFormatOption']
)
strict_cc_library(
    name='DictInlinePostingDecoder',
    deps=[':BufferedSegmentIndexDecoder', ':DictInlineFormatter']
)
strict_cc_library(
    name='DocListEncoder',
    deps=[
        ':BufferedByteSlice', ':DocListFormat', ':InMemDocListDecoder',
        ':PositionBitmapWriter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:BufferedSkipListWriter',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:InMemPairValueSkipListReader',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:InMemTriValueSkipListReader'
    ]
)
strict_cc_library(
    name='DocListEncoderImproved',
    deps=[
        ':BufferedByteSlice', ':DocListFormat', ':InMemDocListDecoder',
        ':PositionBitmapWriter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:BufferedSkipListWriter',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:InMemPairValueSkipListReader',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:InMemTriValueSkipListReader'
    ]
)
strict_cc_library(
    name='TermMeta',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='TermMetaDumper',
    deps=[
        ':PostingFormatOption', ':TermMeta',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor'
    ]
)
strict_cc_library(
    name='TermMetaLoader',
    deps=[
        ':PostingFormatOption', ':TermMeta',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/common/numeric_compress:VbyteCompressor',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
