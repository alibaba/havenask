package(default_visibility=['//visibility:public'])
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:defs.bzl', 'gen_conf_lib')
copts = [
    '-Wno-unused-variable', '-Wno-format', '-Wno-deprecated-declarations',
    '-Wno-nonnull-compare'
]
cc_proto(
    name='proto',
    visibility=['//visibility:public'],
    srcs=glob(['hippo/proto/*.proto', 'hippo/proto/yarn/*.proto'],
              exclude=['hippo/proto/HeartBeat.proto']),
    import_prefix='hippo',
    strip_import_prefix='hippo',
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='hippo_sdk_header',
    hdrs=glob(['include/hippo/*.h']),
    strip_include_prefix='include',
    linkstatic=True
)
cc_library(
    name='hippo_havenask_schedule_header',
    hdrs=glob([
        'include/hippo/DriverCommon.h', 'include/hippo/HippoUtil.h',
        'include/hippo/LeaderSerializer.h',
        'include/hippo/LeaderSerializerCreator.h',
        'include/hippo/MasterDriver.h', 'include/hippo/MasterEvent.h',
        'include/hippo/ProtoWrapper.h', 'include/hippo/ScheduleNode.h',
        'include/hippo/ScheduleUnit.h', 'include/hippo/ResourceManager.h',
        'include/hippo/ResourceAllocator.h'
    ]),
    strip_include_prefix='include',
    linkstatic=True
)
cc_library(
    name='hippo_sdk',
    srcs=glob([
        'src/proto_helper/ResourceHelper.h',
        'src/proto_helper/ResourceRequirement.h', 'src/util/PackageUtil.h',
        'src/common/common.h', 'src/util/Log.h', 'src/util/PathUtil.h',
        'src/util/PathUtil.cpp', 'src/proto_helper/ResourceHelper.cpp',
        'src/proto_helper/ResourceRequirement.cpp', 'src/util/PackageUtil.cpp',
        'src/sdk/HippoUtil.cpp', 'src/sdk/ProtoWrapper.cpp'
    ]),
    deps=[
        '//aios/autil:json', '//aios/autil:system_util', ':hippo_sdk_header',
        ':hippo_proto', '//aios/filesystem/fslib:fslib-framework'
    ],
    includes=['src']
)
cc_library(
    name='hippo_proto',
    hdrs=glob(['hippo/proto/*.h']),
    srcs=glob(['hippo/proto/*.cpp']),
    deps=[':proto_cc_proto'],
    strip_include_prefix='hippo',
    include_prefix='hippo'
)
cc_library(
    name='hippo_havenask_schedule',
    visibility=['//visibility:public'],
    srcs=glob([
        'src/sdk/AmLeaderChecker.cpp', 'src/sdk/BasicMasterDriver.cpp',
        'src/sdk/LeaderSerializerCreator.cpp',
        'src/sdk/ProcessLauncherBase.cpp', 'src/sdk/SlotAllocatorBase.cpp',
        'src/sdk/LeaderSerializerImpl.cpp', 'src/sdk/C2ProxyClient.cpp',
        'src/sdk/ProtoWrapper.cpp', 'src/sdk/HippoUtil.cpp',
        'src/sdk/MasterDriver.cpp', 'src/sdk/default/*.cpp',
        'src/sdk/kubernetes/*.cpp', 'src/http_client/HttpClient.cpp',
        'src/http_client/PBJson.cpp', 'src/sdk/AmLeaderChecker.h',
        'src/sdk/BasicMasterDriver.h', 'src/sdk/LeaderSerializerCreator.h',
        'src/sdk/ProcessLauncherBase.h', 'src/sdk/SlotAllocatorBase.h',
        'src/sdk/LeaderSerializerImpl.h', 'src/sdk/C2ProxyClient.h',
        'src/sdk/default/*.h', 'src/sdk/kubernetes/*.h',
        'src/util/PathUtil.cpp', 'src/util/SignatureUtil.cpp',
        'src/util/PackageUtil.cpp', 'src/util/JsonUtil.cpp',
        'src/util/CommonUtil.cpp', 'src/common/LeaderLocator.cpp',
        'src/common/AppState.cpp', 'src/common/LocalState.cpp',
        'src/common/ZkState.cpp', 'src/common/LeaderState.cpp',
        'src/proto_helper/ResourceHelper.cpp',
        'src/proto_helper/ResourceRequirement.cpp', 'src/util/PathUtil.h',
        'src/util/Log.h', 'src/util/SignatureUtil.h', 'src/util/PackageUtil.h',
        'src/util/JsonUtil.h', 'src/util/CommonUtil.h',
        'src/util/TerminateNotifier.h', 'src/http_client/HttpClient.h',
        'src/http_client/PBJson.h', 'src/common/common.h',
        'src/common/Serializeable.h', 'src/common/LeaderLocator.h',
        'src/common/AppState.h', 'src/common/LocalState.h',
        'src/common/ZkState.h', 'src/common/LeaderState.h',
        'src/proto_helper/ResourceHelper.h',
        'src/proto_helper/ResourceRequirement.h'
    ]),
    deps=[
        ':proto_cc_proto', ':hippo_havenask_schedule_header',
        '//aios/autil:system_util', '//aios/alog:alog', '//aios/autil:base64',
        '//aios/worker_framework:worker_framework', '@rapidjson//:rapidjson'
    ],
    tags=['manual', 'only_for_opensource'],
    copts=copts,
    includes=['src']
)
cc_library(
    name='hippo',
    visibility=['//visibility:public'],
    srcs=glob([]),
    deps=[':hippo_havenask_schedule'],
    copts=copts
)
test_h = gen_conf_lib(
    name='test_h',
    srcs=['src/test/test.h.in'],
    outs=['src/test/test.h'],
    args={
        '@TOP_SRCDIR@':
            'aios/hippo',
        '@TMP_TOP_SRCDIR@':
            '/tmp/aios/hippo',
        '@DOTEST_LOGGER_CONF@':
            '/home/xijie.xuxj/alibaba/alibaba_master/aios/hippo/src/test/hippo_alog.conf'
    },
    strip_include_prefix='src'
)
test_data = glob(['src/testdata/*', 'src/testdata/**/*'])
