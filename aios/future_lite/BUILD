load('//bazel:defs.bzl', 'gen_conf_lib', 'if_arm')
config_setting(name='use_coro', values={'define': 'use_coro=yes'})
genrule(
    name='future_lite_coro_config_h',
    srcs=['future_lite/Config.h.in'],
    outs=['future_lite/Config.h'],
    cmd=select({
        ':use_coro':
            'sed s/FUTURE_LITE_CORO_SWITCH/FUTURE_LITE_USE_COROUTINES/ $(SRCS) > $(OUTS)',
        '//conditions:default':
            'sed s/FUTURE_LITE_CORO_SWITCH/FUTURE_LITE_NOT_USE_COROUTINES/ $(SRCS) > $(OUTS)'
    })
)
cc_library(
    name='future_lite_base',
    srcs=(
        glob([
            'future_lite/*.cpp', 'future_lite/util/*.cpp',
            'future_lite/uthread/*.cpp', 'future_lite/uthread/internal/*.cc',
            'future_lite/coro/*.cpp'
        ]) + if_arm(
            glob(['future_lite/uthread/internal/*arm64_aapcs_elf*']),
            glob(['future_lite/uthread/internal/*x86_64_sysv_elf*'])
        )
    ),
    hdrs=(
        glob([
            'future_lite/*.h', 'future_lite/util/*.h',
            'future_lite/uthread/*.h', 'future_lite/uthread/internal/*.h',
            'future_lite/experimental/coroutine.h', 'future_lite/coro/*.h'
        ]) + [':future_lite_coro_config_h']
    ),
    deps=[
        '//aios/alog:alog', '//aios/autil:thread', '//aios/autil:mem_pool_base',
        '//aios/autil:string_type', '@rt_libs//:librt',
        '@com_google_absl//absl/base', '@com_google_absl//absl/synchronization'
    ],
    visibility=['//visibility:public'],
    include_prefix='future_lite',
    strip_include_prefix='future_lite'
)
cc_library(
    name='future_lite',
    deps=[
        ':future_lite_base',
        '//aios/future_lite/future_lite/executors:simple_executor'
    ],
    visibility=['//visibility:public']
)
gen_conf_lib(
    name='test_h',
    srcs=['future_lite/test/test.h.in'],
    outs=['future_lite/test/test.h'],
    args={
        '@DOTEST_LOGGER_CONF@': 'aios/future_lite/future_lite/test/logger.conf',
        '@TOP_SRCDIR@': 'aios/future_lite'
    },
    include_prefix='future_lite'
)
