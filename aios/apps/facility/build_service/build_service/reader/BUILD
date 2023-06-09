package(
    default_visibility=['//aios/apps/facility/build_service:__subpackages__']
)
cc_library(
    name='bs_reader',
    srcs=[
        'AESCipherFileReader.cpp', 'AESCipherGZipFileReader.cpp',
        'BinaryFileRawDocumentReader.cpp', 'CipherFormatFileDocumentReader.cpp',
        'CombinedRawDocumentParser.cpp', 'EmptyFilePattern.cpp',
        'FileDocumentReader.cpp', 'FileListCollector.cpp', 'FilePattern.cpp',
        'FilePatternBase.cpp', 'FilePatternParser.cpp',
        'FileRawDocumentReader.cpp', 'FileReader.cpp', 'FileReaderBase.cpp',
        'FixLenBinaryFileDocumentReader.cpp', 'FormatFileDocumentReader.cpp',
        'GZipFileReader.cpp', 'IdleDocumentParser.cpp', 'IndexDocReader.cpp',
        'IndexDocToFileReader.cpp', 'IndexQueryCondition.cpp',
        'MultiFileDocumentReader.cpp', 'MultiIndexDocReader.cpp',
        'MultiKVDocReader.cpp', 'ParserCreator.cpp', 'RawDocumentBuilder.cpp',
        'RawDocumentReader.cpp', 'RawDocumentReaderCreator.cpp',
        'ReaderManager.cpp', 'Separator.cpp', 'SingleIndexDocReader.cpp',
        'SourceFieldExtractorDocIterator.cpp', 'StandardRawDocumentParser.cpp',
        'SwiftFieldFilterRawDocumentParser.cpp', 'SwiftProcessedDocReader.cpp',
        'SwiftProcessedDocReaderV2.cpp', 'SwiftRawDocumentReader.cpp',
        'SwiftReaderWrapper.cpp', 'SwiftSchemaBasedRawDocumentParser.cpp',
        'SwiftTopicStreamRawDocumentReader.cpp',
        'VarLenBinaryFileDocumentReader.cpp'
    ],
    hdrs=[':bs_reader_headers'],
    include_prefix='build_service/reader',
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/apps/facility/build_service/build_service_tasks:__subpackages__',
        '//aios/plugins/basic_bs_plugins:__subpackages__',
        '//aios/plugins/pluginplatform/bs_plugins:__subpackages__',
        '//tf_plugin:__subpackages__'
    ],
    deps=([
        '//aios/apps/facility/build_service:bs_common',
        '//aios/apps/facility/build_service:bs_document',
        '//aios/apps/facility/build_service:bs_io',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/autil:dynamic_buf', '//aios/autil:span', '//aios/autil:cipher',
        '//aios/storage/indexlib:alter_table',
        '//aios/storage/indexlib/base:progress',
        '//aios/storage/indexlib/framework:tablet_doc_iterator',
        '//aios/storage/indexlib/index/inverted_index:executor',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage/indexlib/table/kkv_table:table',
        '//aios/storage/indexlib/table/normal_table:table'
    ] + []),
    alwayslink=True
)
filegroup(
    name='bs_reader_headers',
    srcs=[
        'AESCipherFileReader.h', 'AESCipherGZipFileReader.h',
        'BinaryFileRawDocumentReader.h', 'CipherFormatFileDocumentReader.h',
        'CollectResult.h', 'CombinedRawDocumentParser.h',
        'DocumentSeparators.h', 'DummyDocParser.h', 'EmptyFilePattern.h',
        'FileDocumentReader.h', 'FileListCollector.h', 'FilePattern.h',
        'FilePatternBase.h', 'FilePatternParser.h', 'FileRawDocumentReader.h',
        'FileReader.h', 'FileReaderBase.h', 'FixLenBinaryFileDocumentReader.h',
        'FormatFileDocumentReader.h', 'GZipFileReader.h',
        'IdleDocumentParser.h', 'IndexDocReader.h', 'IndexDocToFileReader.h',
        'IndexQueryCondition.h', 'MultiFileDocumentReader.h',
        'MultiIndexDocReader.h', 'MultiKVDocReader.h', 'ParserCreator.h',
        'RawDocumentBuilder.h', 'RawDocumentParser.h', 'RawDocumentReader.h',
        'RawDocumentReaderCreator.h', 'ReaderConfig.h', 'ReaderManager.h',
        'ReaderModuleFactory.h', 'Separator.h', 'SingleIndexDocReader.h',
        'SourceFieldExtractorDocIterator.h', 'StandardRawDocumentParser.h',
        'SwiftFieldFilterRawDocumentParser.h', 'SwiftProcessedDocReader.h',
        'SwiftProcessedDocReaderV2.h', 'SwiftRawDocumentReader.h',
        'SwiftReaderWrapper.h', 'SwiftSchemaBasedRawDocumentParser.h',
        'SwiftTopicStreamRawDocumentReader.h', 'VarLenBinaryDocumentEncoder.h',
        'VarLenBinaryFileDocumentReader.h'
    ]
)
