load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='S16Compressor',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/autil:log', '//aios/storage/indexlib/base:Status']
)
strict_cc_library(
    name='EquivalentCompressDefine',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='EquivalentCompressTraits',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='EquivalentCompressReader',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':EquivalentCompressDefine', ':EquivalentCompressTraits',
        '//aios/autil:log', '//aios/autil:long_hash_value',
        '//aios/autil:multi_value', '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/stream:file_stream',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
strict_cc_library(
    name='EquivalentCompressSessionReader',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':EquivalentCompressReader', '//aios/autil:log',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/util:PoolUtil',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(
    name='EquivalentCompressWriter',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':EquivalentCompressReader', '//aios/autil:log',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='VbyteCompressor',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
    name='IntEncoder',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:byte_slice_rw'
    ]
)
strict_cc_library(
    name='ReferenceCompressIntEncoder',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':IntEncoder', '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
strict_cc_library(
    name='DecompressSse4',
    copts=['-Werror', '-mavx'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Unpack']
)
strict_cc_library(
    name='EncoderProvider',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':GroupVint32Encoder', ':IntEncoder', ':NewPfordeltaIntEncoder',
        ':NoCompressIntEncoder', ':ReferenceCompressIntEncoder',
        ':VbyteInt32Encoder'
    ]
)
strict_cc_library(
    name='GroupVarint',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':IntEncoder', ':VbyteCompressor']
)
strict_cc_library(
    name='GroupVint32Encoder',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':GroupVarint', ':IntEncoder']
)
strict_cc_library(
    name='NewPfordeltaCompressor',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DecompressSse4', ':Pack', ':S9Compressor', ':UnalignedUnpack']
)
strict_cc_library(
    name='NewPfordeltaIntEncoder',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':IntEncoder', ':NewPfordeltaCompressor', ':NosseNewPfordeltaCompressor'
    ]
)
strict_cc_library(
    name='NoCompressIntEncoder',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[]
)
strict_cc_library(
    name='NosseNewPfordeltaCompressor',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Pack', ':S9Compressor', ':UnalignedUnpack', ':Unpack']
)
strict_cc_library(
    name='Pack',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
strict_cc_library(
    name='S9Compressor',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(
    name='UnalignedUnpack',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[]
)
strict_cc_library(
    name='Unpack',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':UnalignedUnpack']
)
strict_cc_library(
    name='VbyteInt32Encoder',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':IntEncoder', ':VbyteCompressor']
)
