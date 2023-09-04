cc_library(
    name='trace',
    srcs=['turing_ops_util/variant/Tracer.cpp'],
    hdrs=['turing_ops_util/variant/Tracer.h'],
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    visibility=['//visibility:public'],
    deps=([
        '//aios/autil:common_macros', '//aios/autil:data_buffer',
        '//aios/autil:env_util', '//aios/matchdoc:matchdoc', '//aios/autil:log'
    ] + [])
)
cc_library(
    name='sort_expr_meta',
    hdrs=['turing_ops_util/variant/SortExprMeta.h'],
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    visibility=['//visibility:public'],
    deps=[],
    alwayslink=1
)
cc_library(
    name='op_doc_tracer',
    srcs=glob(['turing_ops_util/util/DocTracer.cpp']),
    hdrs=glob(['turing_ops_util/util/DocTracer.h']),
    visibility=['//visibility:public'],
    include_prefix='turing_ops_util',
    strip_include_prefix='turing_ops_util',
    deps=[':trace', '//aios/matchdoc'],
    alwayslink=1
)
