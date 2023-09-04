package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='swift_reader_creator',
    srcs=['SwiftReaderCreator.cpp'],
    hdrs=['ParamParser.h', 'SwiftReaderCreator.h'],
    include_prefix='suez/drc',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:bs_util',
        '//aios/apps/facility/swift:swift_client', '//aios/autil:log',
        '//aios/autil:string_helper'
    ]
)
cc_library(
    name='drc',
    srcs=[
        'BinaryRecord.cpp', 'BinaryRecordDecoder.cpp', 'Checkpoint.cpp',
        'DrcConfig.cpp', 'IgnoredLog.cpp', 'LogReader.cpp', 'LogRecord.cpp',
        'LogRecordBuilder.cpp', 'LogReplicationPipeline.cpp',
        'LogReplicator.cpp', 'LogReplicatorCreator.cpp', 'LogTracer.cpp',
        'LogWriter.cpp', 'SwiftSink.cpp', 'SwiftSource.cpp', 'Update2Add.cpp'
    ],
    hdrs=[
        'BinaryRecord.h', 'BinaryRecordDecoder.h', 'Checkpoint.h',
        'DrcConfig.h', 'IgnoredLog.h', 'LogReader.h', 'LogRecord.h',
        'LogRecordBuilder.h', 'LogReplicationPipeline.h', 'LogReplicator.h',
        'LogReplicatorCreator.h', 'LogRewriter.h', 'LogTracer.h', 'LogWriter.h',
        'Sink.h', 'Source.h', 'SwiftSink.h', 'SwiftSource.h', 'Update2Add.h'
    ],
    include_prefix='suez/drc',
    deps=[
        ':swift_reader_creator', '//aios/apps/facility/build_service:bs_config',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/apps/facility/swift:swift_client', '//aios/autil:hash_func',
        '//aios/autil:log', '//aios/autil:span', '//aios/autil:string_helper',
        '//aios/autil:time', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib:tablet',
        '//aios/storage/indexlib/table/kv_table:reader',
        '//aios/suez/sdk:utils', '//aios/worker_framework'
    ]
)
