cc_library(
    name='navi_coroutine_kernel',
    srcs=glob(['CoroutineKernel.cpp']),
    hdrs=['CoroutineKernel.h', 'NaviAsyncPipeExecutor.h'],
    deps=[
        '//aios/future_lite:future_lite', '//navi:navi',
        '//aios/autil:plugin_base'
    ],
    visibility=['//visibility:public']
)
