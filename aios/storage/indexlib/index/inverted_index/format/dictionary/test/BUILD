load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='indexlib_dictionary_unittest',
    srcs=[
        'CommonDiskTieredDictionaryReaderTest.cpp',
        'DefaultTermDictionaryReaderTest.cpp', 'HashDictionaryWriterTest.cpp',
        'InMemDictionaryReaderTest.cpp', 'LegacyBlockArrayReaderTest.cpp'
    ],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/inverted_index/config',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineDecoder',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DefaultTermDictionaryReader',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='TieredDictionaryTest',
    srcs=['TieredDictionaryReaderTest.cpp', 'TieredDictionaryWriterTest.cpp'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DefaultTermDictionaryReader',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
