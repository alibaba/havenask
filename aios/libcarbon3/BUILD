package(default_visibility=['//visibility:private'])
load('//bazel:defs.bzl', 'cc_proto', 'gen_conf_lib')
cc_proto(
    name='master_framework_proto',
    visibility=['//visibility:public'],
    srcs=glob(['master_framework/proto/*.proto']),
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto'],
    import_prefix='master_framework',
    strip_import_prefix='master_framework'
)
cc_proto(
    name='carbon3_proto',
    visibility=['//visibility:public'],
    srcs=glob(['carbon/proto/*.proto']),
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto'],
    import_prefix='carbon',
    strip_import_prefix='carbon'
)
cc_library(
    name='master_framework',
    visibility=['//visibility:public'],
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
    deps=[':master_framework_proto_cc_proto', ':carbon3', '//aios/hippo:hippo'],
    includes=['src/master_framework'],
    strip_include_prefix='include'
)
cc_library(
    name='carbon3',
    visibility=['//visibility:public'],
    srcs=glob([
        'src/master/*.h', 'src/common/*.h', 'src/compatible/*.h',
        'src/service/*.h', 'src/monitor/*.h', 'src/master/*.cpp',
        'src/common/*.cpp', 'src/compatible/*.cpp', 'src/service/*.cpp',
        'src/monitor/*.cpp'
    ]),
    hdrs=glob(['include/carbon/*.h', 'include/carbon/legacy/*.h']),
    deps=[
        ':carbon3_proto_cc_proto', '//aios/hippo:hippo',
        '//aios/autil:closure_guard', '//third_party/lzma:lzma'
    ],
    includes=['src'],
    strip_include_prefix='include'
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
