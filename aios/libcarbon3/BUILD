package(default_visibility=['//visibility:private'])
load('//bazel:defs.bzl', 'cc_proto', 'gen_conf_lib')
cc_proto(
    name='master_framework_proto',
    srcs=glob(['master_framework/proto/*.proto']),
    import_prefix='master_framework',
    strip_import_prefix='master_framework',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_proto(
    name='carbon3_proto',
    srcs=glob(['carbon/proto/*.proto']),
    import_prefix='carbon',
    strip_import_prefix='carbon',
    visibility=['//visibility:public'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_library(
    name='master_framework',
    srcs=(
        glob([
            'src/master_framework/master_base/*.h',
            'src/master_framework/simple_master/*.h',
            'src/master_framework/common/*.h'
        ]) + [
            'src/master_framework/master_base/MasterBase.cpp',
            'src/master_framework/master_base/ScheduleUnitManager.cpp',
            'src/master_framework/master_base/SlotSorter.cpp',
            'src/master_framework/master_base/PartitionScheduleUnit.cpp',
            'src/master_framework/master_base/PartitionScheduleUnitFactory.cpp',
            'src/master_framework/master_base/ComboScheduleUnit.cpp',
            'src/master_framework/master_base/BasicScheduleUnit.cpp',
            'src/master_framework/master_base/ReplicaScheduleUnitFactory.cpp',
            'src/master_framework/master_base/ReplicaScheduleUnit.cpp',
            'src/master_framework/master_base/ReplicaSchedulePlan.cpp',
            'src/master_framework/master_base/ScheduleUnit.cpp',
            'src/master_framework/master_base/ScheduleHelper.cpp',
            'src/master_framework/master_base/SlotClassifier.cpp',
            'src/master_framework/master_base/SlotChecker.cpp',
            'src/master_framework/simple_master/SimpleMasterSchedulerOrigin.cpp',
            'src/master_framework/simple_master/Router.cpp',
            'src/master_framework/simple_master/TransferAppPlanUtil.cpp',
            'src/master_framework/simple_master/SimpleMaster.cpp',
            'src/master_framework/simple_master/AppPlan.cpp',
            'src/master_framework/simple_master/SimpleMasterServiceImpl.cpp',
            'src/master_framework/simple_master/SimpleMasterSchedulerAdapterImpl.cpp',
            'src/master_framework/simple_master/SimpleMasterScheduler.cpp'
        ]
    ),
    hdrs=glob(['include/master_framework/*.h', 'include/carbon/Log.h']),
    includes=['src/master_framework'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=[':carbon3', ':master_framework_proto_cc_proto', '//aios/hippo']
)
cc_library(
    name='carbon3',
    srcs=glob([
        'src/master/*.h', 'src/common/*.h', 'src/compatible/*.h',
        'src/service/*.h', 'src/monitor/*.h', 'src/master/*.cpp',
        'src/common/*.cpp', 'src/compatible/*.cpp', 'src/service/*.cpp',
        'src/monitor/*.cpp'
    ]),
    hdrs=glob(['include/carbon/*.h', 'include/carbon/legacy/*.h']),
    includes=['src'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=[
        ':carbon3_proto_cc_proto', '//aios/autil:closure_guard', '//aios/hippo',
        '//third_party/lzma'
    ]
)
cc_library(
    name='c2',
    srcs=glob([
        'src/master/*.h', 'src/common/*.h', 'src/compatible/*.h',
        'src/service/*.h', 'src/monitor/*.h', 'src/client/*.h',
        'src/master/*.cpp', 'src/common/*.cpp', 'src/compatible/*.cpp',
        'src/service/*.cpp', 'src/monitor/*.cpp', 'src/client/*.cpp'
    ]),
    hdrs=glob([
        'include/c2/*.h', 'include/c2/legacy/*.h', 'include/carbon/*.h',
        'include/carbon/legacy/*.h'
    ]),
    includes=['src'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=[
        ':carbon3_proto_cc_proto', '//aios/autil:closure_guard', '//aios/hippo',
        '//aios/hippo:hippo_sdk', '//aios/network/http_arpc',
        '//third_party/lzma'
    ]
)
gen_conf_lib(
    name='test_header',
    srcs=['src/test/test.h.in'],
    outs=['src/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/libcarbon3',
        '@TMP_TOP_SRCDIR@': '/tmp/aios/libcarbon3',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@DOTEST_LOGGER_CONF@': '.',
        '@INSTALLROOT@': '.'
    },
    strip_include_prefix='src'
)
