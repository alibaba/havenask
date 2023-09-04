package(default_visibility=['//aios/suez:__subpackages__'])
cc_library(
    name='common',
    srcs=[
        'DiskQuotaController.cpp', 'DiskUtil.cpp', 'GeneratorDef.cpp',
        'InnerDef.cpp', 'TableMeta.cpp', 'TablePathDefine.cpp',
        'TargetLayout.cpp', 'TargetRecorder.cpp', 'TimeRecorder.cpp'
    ],
    hdrs=[
        'DiskQuotaController.h', 'DiskUtil.h', 'GeneratorDef.h', 'InitParam.h',
        'InnerDef.h', 'ServerMetrics.h', 'TableMeta.h', 'TablePathDefine.h',
        'TargetLayout.h', 'TargetRecorder.h', 'TimeRecorder.h'
    ],
    include_prefix='suez/common',
    deps=[
        '//aios/apps/facility/build_service:bs_basic',
        '//aios/autil:file_recorder', '//aios/autil:time',
        '//aios/storage/indexlib/indexlib:indexlib_h', '//aios/suez/sdk:Magic',
        '//aios/suez/sdk:SuezError', '//aios/suez/sdk:SuezPartitionType',
        '//aios/suez/sdk:TableUserDefinedMeta',
        '//aios/suez/sdk:table_def_config', '//aios/suez/sdk:utils'
    ]
)
