package(default_visibility=['//navi:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto', 'copy_target_to', 'if_clang')
load(
    '//bazel:bundle.bzl', 'bundle_files', 'bundle_cc_hdrs', 'bundle_tar',
    'bundle_install'
)
cc_library(
    name='navi',
    deps=[
        ':navi_headers', ':navi_proto_cc_proto',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/network/gig:multi_call',
        '//aios/autil:plugin_base'
    ],
    implementation_deps=[':navi_inner_lib'],
    data=[':navi_python_home_scripts'],
    visibility=['//visibility:public']
)
cc_library(
    name='navi_headers',
    hdrs=glob([
        'common.h', 'config/*.h', 'engine/AsyncPipe.h', 'engine/Data.h',
        'engine/CreatorRegistry.h', 'engine/CreatorStats.h',
        'engine/GraphMetric.h', 'engine/Kernel.h',
        'engine/KernelComputeContext.h', 'engine/KernelConfigContext.h',
        'engine/KernelCreator.h', 'engine/KernelInitContext.h',
        'engine/KernelMetric.h', 'engine/ScheduleInfo.h', 'engine/MapKernel.h',
        'engine/N2OneKernel.h', 'engine/Navi.h', 'engine/NaviSnapshotStat.h',
        'engine/NaviConfigContext.h', 'engine/NaviResult.h',
        'engine/NaviUserResult.h', 'engine/PipeKernel.h',
        'engine/PortMergeKernel.h', 'engine/PortSplitKernel.h',
        'engine/Resource.h', 'engine/ResourceConfigContext.h',
        'engine/ResourceCreator.h', 'engine/ResourceInitContext.h',
        'engine/ResourceMap.h', 'engine/RunGraphParams.h',
        'engine/TimeoutChecker.h', 'engine/Type.h', 'engine/TypeContext.h',
        'engine/TypeInfo.h', 'engine/PartInfo.h', 'log/LoggingEvent.h',
        'log/NaviLogger.h', 'log/NaviLoggerProvider.h', 'log/TraceCollector.h',
        'perf/NaviPerfResult.h', 'resource/*.h', 'ops/DefaultMergeKernel.h',
        'ops/DefaultSplitKernel.h', 'util/NaviTestPool.h', 'util/NaviClosure.h',
        'util/ReadyBitMap.h', 'util/BitMap.h', 'test_cluster/NaviTestCluster.h',
        'test_cluster/NaviFuncClosure.h', 'tester/KernelTester.h',
        'tester/KernelTesterBuilder.h'
    ]),
    deps=([
        ':navi_proto_cc_proto_headers', ':navi_builder_headers',
        '//aios/autil:object_tracer', '//aios/network/anet:anet',
        '//aios/autil:json'
    ] + [])
)
cc_proto(
    name='navi_proto',
    srcs=glob([
        'proto/GraphDef.proto', 'proto/GraphVis.proto', 'proto/KernelDef.proto',
        'proto/NaviRpc.proto'
    ]),
    deps=['//aios/network/arpc:proto'],
    visibility=['//visibility:public']
)
cc_proto(
    name='navi_proto_inner',
    srcs=glob(['proto/NaviStream.proto']),
    deps=[':navi_proto']
)
cc_library(
    name='navi_builder_headers',
    hdrs=glob([
        'builder/GraphBuilder.h', 'builder/GraphDesc.h', 'builder/BorderId.h',
        'builder/KernelDefBuilder.h', 'builder/ResourceDefBuilder.h'
    ],
              exclude=[]),
    implementation_deps=[':navi_builder_inner'],
    deps=[
        ':navi_proto_inner_cc', ':navi_common', '//aios/autil:json',
        '//aios/network/gig:multi_call'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='navi_builder_inner',
    srcs=glob(['builder/*.cpp'], exclude=[]),
    hdrs=glob(['builder/*.h'], exclude=[]),
    deps=[
        ':navi_proto_inner_cc', ':navi_common', '//aios/autil:json',
        '//aios/network/gig:multi_call'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    alwayslink=True
)
cc_library(
    name='navi_common',
    srcs=glob(['common.cpp'], exclude=[]),
    hdrs=glob(['common.h', 'util/NaviTestPool.h'], exclude=[]),
    deps=['//aios/network/anet:anet', '//aios/autil:mem_pool_base'],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    alwayslink=True
)
cc_library(
    name='navi_inner_lib',
    srcs=glob([
        'log/*.cpp', 'util/*.cpp', 'config/*.cpp', 'engine/*.cpp',
        'resource/*.cpp', 'ops/*.cpp', 'config_loader/*.cpp',
        'distribute/*.cpp', 'perf/*.cpp', 'test_cluster/*.cpp', 'tester/*.cpp'
    ],
              exclude=['test_cluster/NaviGraphRunner.cpp']),
    hdrs=glob([
        'engine/*.h', 'log/*.h', 'ops/*.h', 'util/*.h', 'resource/*.h',
        'config/*.h', 'config_loader/*.h', 'distribute/*.h', 'perf/*.h',
        'test_cluster/*.h', 'test_cluster/*.h', 'tester/*.h'
    ],
              exclude=['test_cluster/NaviGraphRunner.h']),
    deps=([
        ':navi_headers', ':navi_common', ':navi_builder_headers',
        ':navi_proto_inner_cc', '//aios/autil:plugin_base',
        '//aios/network/gig:multi_call', '//aios/network/http_arpc:http_arpc',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//third_party/elfutils-libelf:elfutils-libelf'
    ] + []),
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    alwayslink=True
)
filegroup(
    name='navi_python_home_scripts', srcs=glob(['config_loader/python/*.py'])
)
