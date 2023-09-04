cc_library(
    name='navi_coroutine_kernel',
    srcs=glob(['CoroutineKernel.cpp']),
    hdrs=['CoroutineKernel.h', 'NaviAsyncPipeExecutor.h'],
    include_prefix='navi_ops/coroutine',
    deps=[
        '//aios/future_lite:future_lite', '//aios/navi:navi',
        '//aios/autil:plugin_base'
    ],
    visibility=['//visibility:public']
)
