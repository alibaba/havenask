package(default_visibility=['//aios/navi:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto', 'copy_target_to', 'if_clang')
load('//bazel:bundle.bzl', 'bundle_files')
filegroup(
    name='testdata',
    srcs=glob(['testdata/**', 'leak_suppression']),
    testonly=True
)
filegroup(
    name='test_alog_conf', srcs=glob(['misc/test_alog.conf']), testonly=True
)
copy_target_to(
    name='copy_test_plugin',
    to_copy=':libtest_plugin.so',
    copy_name='libtest_plugin.so',
    dests=['testdata/config/modules/'],
    testonly=True
)
cc_library(
    name='navi',
    deps=[
        ':navi_headers', ':navi_proto_cc_proto', ':navi_inner_lib',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/network/gig:multi_call',
        '//aios/autil:plugin_base', '//aios/network/http_arpc:http_arpc'
    ],
    implementation_deps=[],
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
        'engine/N2OneKernel.h', 'engine/NamedData.h', 'engine/Navi.h',
        'engine/NaviSnapshotStat.h', 'engine/NaviConfigContext.h',
        'engine/NaviResult.h', 'engine/NaviUserResult.h', 'engine/PipeKernel.h',
        'engine/PortMergeKernel.h', 'engine/PortSplitKernel.h',
        'engine/ScopeTerminatorKernel.h', 'engine/Resource.h',
        'engine/ResourceConfigContext.h', 'engine/ResourceCreator.h',
        'engine/ResourceInitContext.h', 'engine/ResourceMap.h',
        'engine/RunGraphParams.h', 'engine/TimeoutChecker.h', 'engine/Type.h',
        'engine/TypeContext.h', 'engine/TypeInfo.h', 'engine/PartInfo.h',
        'log/LoggingEvent.h', 'log/NaviLogger.h', 'log/NaviLoggerProvider.h',
        'log/TraceCollector.h', 'log/LogBtFilter.h', 'perf/NaviPerfResult.h',
        'resource/*.h', 'ops/DefaultMergeKernel.h', 'ops/DefaultSplitKernel.h',
        'rpc_server/*.h', 'util/NaviTestPool.h', 'util/NaviClosure.h',
        'util/ReadyBitMap.h', 'util/BitMap.h', 'util/EnvParam.h',
        'test_cluster/NaviTestCluster.h', 'test_cluster/NaviFuncClosure.h'
    ]),
    include_prefix='navi',
    deps=([
        ':navi_proto_cc_proto_headers', ':navi_builder_headers',
        '//aios/autil:object_tracer', '//aios/network/anet:anet',
        '//aios/autil:json'
    ] + [])
)
cc_library(
    name='navi_graph_runner_testlib',
    hdrs=['test_cluster/NaviGraphRunner.h'],
    srcs=['test_cluster/NaviGraphRunner.cpp'],
    include_prefix='navi',
    deps=[':navi', ':navi_example_test_kernel_lib'],
    data=[':testdata', ':copy_test_plugin'],
    testonly=True,
    visibility=['//visibility:public']
)
cc_library(
    name='navi_engine_testlib',
    hdrs=['engine/test/MockAsyncPipe.h'],
    include_prefix='navi',
    deps=['//aios/unittest_framework:unittest_lib', ':navi'],
    testonly=True,
    visibility=['//visibility:public']
)
cc_library(
    name='navi_log_testlib',
    hdrs=['log/test/NaviLoggerProviderTestUtil.h'],
    srcs=['log/test/NaviLoggerProviderTestUtil.cpp'],
    include_prefix='navi',
    deps=['//aios/unittest_framework:unittest_lib', ':navi'],
    testonly=True,
    visibility=['//visibility:public']
)
cc_proto(
    name='navi_proto',
    srcs=glob([
        'proto/GraphDef.proto', 'proto/GraphVis.proto', 'proto/KernelDef.proto',
        'proto/NaviRpc.proto'
    ]),
    import_prefix='navi',
    deps=['//aios/network/arpc:proto'],
    visibility=['//visibility:public']
)
cc_proto(
    name='navi_proto_inner',
    srcs=glob(['proto/NaviStream.proto']),
    import_prefix='navi',
    deps=[':navi_proto']
)
cc_library(
    name='navi_builder_headers',
    hdrs=glob([
        'builder/GraphBuilder.h', 'builder/GraphDesc.h', 'builder/BorderId.h',
        'builder/KernelDefBuilder.h', 'builder/ResourceDefBuilder.h'
    ],
              exclude=[]),
    implementation_deps=[],
    include_prefix='navi',
    deps=[
        ':navi_proto_inner_cc', ':navi_common', ':navi_builder_inner',
        '//aios/autil:json', '//aios/network/gig:multi_call'
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
    include_prefix='navi',
    deps=[
        ':navi_proto_inner_cc_proto_headers', ':navi_proto_cc_proto_headers',
        ':navi_common', '//aios/autil:json', '//aios/network/gig:multi_call'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    alwayslink=True
)
cc_library(
    name='navi_common',
    srcs=['common.cpp'],
    hdrs=['common.h', 'util/NaviTestPool.h'],
    include_prefix='navi',
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
        'distribute/*.cpp', 'perf/*.cpp', 'rpc_server/*.cpp',
        'test_cluster/*.cpp'
    ],
              exclude=['test_cluster/NaviGraphRunner.cpp']),
    hdrs=glob([
        'engine/*.h', 'log/*.h', 'ops/*.h', 'util/*.h', 'resource/*.h',
        'config/*.h', 'config_loader/*.h', 'distribute/*.h', 'perf/*.h',
        'rpc_server/*.h', 'test_cluster/*.h', 'test_cluster/*.h'
    ],
              exclude=['test_cluster/NaviGraphRunner.h']),
    include_prefix='navi',
    deps=[
        ':navi_headers', ':navi_common', ':navi_builder_headers',
        ':navi_proto_inner_cc', '//aios/autil:plugin_base',
        '//aios/network/gig:multi_call', '//aios/network/http_arpc:http_arpc',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/autil:compression',
        '//aios/autil:diagnostic',
        '//third_party/elfutils-libelf:elfutils-libelf',
        '//third_party/libev:libev', '//aios/memory/lockless_allocator:ll_alloc'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    alwayslink=True
)
cc_library(
    name='navi_ops_testlib',
    srcs=glob(['tester/*.cpp']),
    hdrs=glob(['tester/*.h']),
    include_prefix='navi',
    deps=[
        ':navi_inner_lib', ':navi_engine_testlib',
        '//aios/kmonitor:metrics_report_navi_resource'
    ],
    testonly=True,
    alwayslink=True,
    visibility=['//visibility:public']
)
filegroup(
    name='navi_python_home_scripts', srcs=glob(['config_loader/python/*.py'])
)
cc_binary(
    name='libtest_plugin.so',
    srcs=glob(['example/TestBizResource.cpp', 'example/TestBizKernel.cpp']),
    copts=if_clang([], ['-fno-gnu-unique']),
    deps=[
        ':navi_example_test_kernel_lib', '//aios/network/http_arpc:http_arpc'
    ],
    linkshared=1,
    linkstatic=0,
    testonly=True
)
cc_library(
    name='navi_example_test_lib',
    srcs=glob([
        'example/TestData.cpp', 'example/TestResource.cpp',
        'example/TestType.cpp'
    ]),
    hdrs=glob([
        'example/TestData.h', 'example/TestResource.h', 'example/TestType.h'
    ]),
    include_prefix='navi',
    deps=([':navi_inner_lib'] + []),
    copts=['-Werror', '-Wno-aligned-new'],
    alwayslink=True,
    testonly=True,
    linkstatic=0
)
cc_library(
    name='navi_example_test_kernel_lib',
    srcs=glob(['example/TestKernel.cpp']),
    hdrs=['example/TestKernel.h'],
    include_prefix='navi',
    deps=[':navi_example_test_lib', ':navi_inner_lib'],
    copts=['-Werror', '-Wno-aligned-new'],
    alwayslink=True,
    testonly=True,
    linkstatic=0
)
cc_test(
    name='navi_engine_test',
    srcs=glob(['engine/test/*Test.cpp']),
    deps=[
        ':navi_example_test_lib', ':navi_example_test_kernel_lib',
        ':navi_inner_lib', ':navi_ops_testlib',
        '//aios/unittest_framework:unittest_framework'
    ],
    data=[
        ':testdata', ':test_alog_conf', ':copy_test_plugin',
        ':navi_python_home_scripts'
    ],
    copts=['-fno-access-control']
)
cc_test(
    name='navi_resource_test',
    srcs=glob(['resource/test/*Test.cpp']),
    deps=[':navi_inner_lib', '//aios/unittest_framework:unittest_framework'],
    data=[':testdata', ':test_alog_conf'],
    copts=['-fno-access-control']
)
cc_test(
    name='navi_example_test',
    srcs=glob([
        'example/ExampleEngine.cpp', 'example/DistributeGraphTest.cpp',
        'example/PartAccessGraphTest.cpp', 'example/LackPartGraphTest.cpp',
        'example/PartCancelGraphTest.cpp'
    ]),
    deps=[
        ':navi_example_test_lib',
        '//aios/unittest_framework:unittest_framework',
        '//aios/network/http_arpc:http_arpc', ':navi_example_test_kernel_lib'
    ],
    data=[
        ':testdata', ':test_alog_conf', ':copy_test_plugin',
        ':navi_python_home_scripts'
    ],
    linkstatic=False,
    tags=['exclusive'],
    copts=['-fno-access-control'],
    shard_count=5
)
cc_test(
    name='navi_run_graph_test',
    srcs=glob(['example/RunGraphTest.cpp']),
    deps=[
        ':navi_example_test_lib',
        '//aios/unittest_framework:unittest_framework',
        '//aios/network/http_arpc:http_arpc', ':navi_example_test_kernel_lib'
    ],
    data=[
        ':testdata', ':test_alog_conf', ':copy_test_plugin',
        ':navi_python_home_scripts'
    ],
    copts=['-fno-access-control']
)
cc_test(
    name='navi_tester_test',
    srcs=glob(['tester/test/*.cpp']),
    deps=[
        ':navi_example_test_lib',
        '//aios/unittest_framework:unittest_framework',
        ':navi_example_test_kernel_lib', ':navi_ops_testlib'
    ],
    data=[
        ':testdata', ':test_alog_conf', ':copy_test_plugin',
        ':navi_python_home_scripts'
    ],
    copts=['-fno-access-control']
)
cc_test(
    name='navi_test',
    srcs=glob(['test/*.cpp']),
    deps=[
        ':navi_example_test_lib',
        '//aios/unittest_framework:unittest_framework',
        ':navi_example_test_kernel_lib', ':navi_graph_runner_testlib',
        ':navi_ops_testlib'
    ],
    data=[
        ':testdata', ':test_alog_conf', ':copy_test_plugin',
        ':navi_python_home_scripts'
    ],
    copts=['-fno-access-control'],
    shard_count=2
)
cc_test(
    name='navi_util_test',
    srcs=glob(['util/test/*Test.cpp']),
    deps=[':navi_inner_lib', '//aios/unittest_framework:unittest_framework'],
    copts=['-fno-access-control']
)
cc_test(
    name='navi_builder_test',
    srcs=glob(['builder/test/GraphBuilderTest.cpp']),
    deps=[
        ':navi_builder_headers', '//aios/unittest_framework:unittest_framework',
        '//aios/network/gig:multi_call'
    ],
    copts=['-fno-access-control']
)
bundle_files(
    name='navi_tools_package',
    prefix='usr/local/lib/python/site-packages/navi',
    srcs=glob(['config_loader/python/*.py']),
    deps=[],
    strip_prefix='/aios/navi/config_loader/python',
    visibility=['//visibility:public']
)
bundle_files(
    name='navi_dep_libs',
    prefix='usr/local/lib64',
    srcs=['//third_party/libev:libev_shared']
)
bundle_files(
    name='navi_package',
    prefix='usr/local/bin',
    srcs=[':navi_tools_package'],
    deps=[':navi_dep_libs'],
    visibility=['//visibility:public']
)
