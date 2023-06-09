load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='hash_table',
    srcs=['HashTableBase.cpp', 'HashTableFileReaderBase.cpp'],
    hdrs=[
        'BucketCompressor.h', 'BucketOffsetCompressor.h', 'ChainHashTable.h',
        'ClosedHashTableBufferedFileIterator.h',
        'ClosedHashTableFileIterator.h', 'ClosedHashTableIterator.h',
        'ClosedHashTableTraits.h', 'CompactSpecialKeyBucket.h',
        'CuckooHashTable.h', 'CuckooHashTableFileReader.h',
        'CuckooHashTableTraits.h', 'DenseHashTable.h',
        'DenseHashTableFileReader.h', 'DenseHashTableTraits.h',
        'HashTableBase.h', 'HashTableDefine.h', 'HashTableFileReaderBase.h',
        'HashTableNode.h', 'HashTableOptions.h', 'HashTableReader.h',
        'HashTableWriter.h', 'SeparateChainHashTable.h', 'SpecialKeyBucket.h',
        'SpecialValue.h', 'SpecialValueBucket.h'
    ],
    visibility=['//aios/storage/indexlib/index:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/common:radix_tree',
        '//aios/storage/indexlib/index/common:typed_slice_list',
        '//aios/storage/indexlib/util:PoolUtil',
        '//aios/storage/indexlib/util:Status',
        '//aios/storage/indexlib/util:mmap_vector',
        '//aios/storage/indexlib/util:path_util'
    ]
)
