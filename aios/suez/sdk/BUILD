package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='PartitionId',
    srcs=['PartitionId.cpp'],
    hdrs=['PartitionId.h'],
    include_prefix='suez/sdk',
    deps=['//aios/autil:json', '//aios/autil:range_util']
)
cc_library(name='Magic', hdrs=['Magic.h'], include_prefix='suez/sdk')
cc_library(
    name='SuezPartitionType',
    hdrs=['SuezPartitionType.h'],
    include_prefix='suez/sdk',
    deps=[':Magic']
)
cc_library(
    name='SuezError',
    srcs=['SuezError.cpp'],
    hdrs=['SuezError.h'],
    include_prefix='suez/sdk',
    deps=['//aios/autil:json']
)
cc_library(
    name='TableUserDefinedMeta',
    hdrs=['TableUserDefinedMeta.h'],
    include_prefix='suez/sdk',
    deps=[':PartitionId']
)
cc_library(
    name='hb_interface',
    srcs=['SchedulerInfo.cpp', 'ServiceInfo.cpp'],
    hdrs=['BizMeta.h', 'SchedulerInfo.h', 'ServiceInfo.h'],
    include_prefix='suez/sdk',
    deps=[
        ':SuezError', ':TableUserDefinedMeta', ':utils', '//aios/autil:json',
        '//aios/autil:string_helper'
    ]
)
cc_library(
    name='table_def_config',
    srcs=[],
    hdrs=['TableDefConfig.h'],
    include_prefix='suez/sdk',
    deps=[
        '//aios/apps/facility/build_service:bs_hashmode', '//aios/autil:json'
    ]
)
cc_library(
    name='RpcServer',
    srcs=['RpcServer.cpp'],
    hdrs=['RpcServer.h'],
    include_prefix='suez/sdk',
    deps=[
        ':utils', '//aios/network/arpc', '//aios/network/gig:multi_call',
        '//aios/network/http_arpc'
    ]
)
cc_library(
    name='utils',
    srcs=['IpUtil.cpp', 'JsonNodeRef.cpp', 'PathDefine.cpp'],
    hdrs=[
        'CmdLineDefine.h', 'IpUtil.h', 'JsonNodeRef.h', 'KMonitorMetaInfo.h',
        'MetricUtil.h', 'PathDefine.h'
    ],
    include_prefix='suez/sdk',
    deps=[
        ':PartitionId', '//aios/autil:lock', '//aios/autil:log',
        '//aios/autil:string_helper', '@com_google_protobuf//:protobuf'
    ]
)
cc_library(
    name='partition_data',
    srcs=[
        'SuezIndexPartitionData.cpp', 'SuezPartitionData.cpp',
        'SuezRawFilePartitionData.cpp', 'SuezTabletPartitionData.cpp'
    ],
    hdrs=[
        'SuezIndexPartitionData.h', 'SuezPartitionData.h',
        'SuezRawFilePartitionData.h', 'SuezTabletPartitionData.h'
    ],
    include_prefix='suez/sdk',
    deps=[
        ':PartitionId', ':SuezPartitionType',
        '//aios/apps/facility/build_service:bs_util',
        '//aios/storage/indexlib:interface',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition'
    ]
)
cc_library(
    name='index_provider',
    srcs=['IndexProvider.cpp', 'SourceReaderProvider.cpp', 'TableReader.cpp'],
    hdrs=['IndexProvider.h', 'SourceReaderProvider.h', 'TableReader.h'],
    include_prefix='suez/sdk',
    deps=[
        ':partition_data', ':table_def_config',
        '//aios/apps/facility/build_service:bs_workflow'
    ]
)
cc_library(
    name='index_provider_r',
    srcs=['IndexProviderR.cpp'],
    hdrs=['IndexProviderR.h'],
    include_prefix='suez/sdk',
    deps=[':index_provider', '//aios/navi']
)
cc_library(
    name='sdk',
    srcs=['SearchManager.cpp'],
    hdrs=['SearchInitParam.h', 'SearchManager.h'],
    include_prefix='suez/sdk',
    visibility=['//visibility:public'],
    deps=([':utils', ':hb_interface', ':RpcServer', ':index_provider'] +
          ['//aios/sap_easy:sap_easy_util']),
    alwayslink=1
)
cc_library(
    name='table_writer',
    srcs=[
        'RawDocument2SwiftFieldFilter.cpp', 'RawDocument2SwiftFieldFilter.h',
        'TableWriter.cpp'
    ],
    hdrs=['TableWriter.h', 'WriteResult.h'],
    implementation_deps=['//aios/apps/facility/build_service:bs_workflow'],
    include_prefix='suez/sdk',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:result', '//aios/storage/indexlib:interface',
        '//aios/suez/table/wal'
    ]
)
cc_library(
    name='remote_table_writer',
    srcs=[
        'RemoteTableWriter.cpp', 'RemoteTableWriterClosure.cpp',
        'RemoteTableWriterRequestGenerator.cpp'
    ],
    hdrs=[
        'RemoteTableWriter.h', 'RemoteTableWriterClosure.h',
        'RemoteTableWriterParam.h', 'RemoteTableWriterRequestGenerator.h'
    ],
    implementation_deps=[
        '//aios/network/gig:multi_call', '//aios/autil:range_util',
        '//aios/suez/service:table_service_cc_proto'
    ],
    include_prefix='suez/sdk',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:result', '//aios/suez/common'
    ]
)
