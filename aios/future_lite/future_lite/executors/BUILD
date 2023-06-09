cc_library(
    name='simple_executor',
    srcs=['SimpleExecutor.cpp'],
    hdrs=['SimpleIOExecutor.h', 'SimpleExecutor.h'],
    deps=[
        '//aios/alog:alog', '//aios/autil:thread', '//aios/autil:mem_pool_base',
        '//aios/autil:string_type', '//aios/future_lite:future_lite_base'
    ],
    visibility=['//visibility:public'],
    include_prefix='future_lite/executors',
    alwayslink=True
)
cc_library(
    name='simple_async_io_executor',
    srcs=['SimpleAsyncIOExecutor.cpp'],
    hdrs=[],
    deps=[':simple_executor'],
    visibility=['//visibility:public'],
    alwayslink=True
)
