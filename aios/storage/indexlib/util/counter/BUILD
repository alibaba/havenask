load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='counter',
    srcs=[
        'AccumulativeCounter.cpp', 'Counter.cpp', 'CounterBase.cpp',
        'CounterCreator.cpp', 'CounterMap.cpp', 'MultiCounter.cpp',
        'StateCounter.cpp', 'StringCounter.cpp'
    ],
    hdrs=[
        'AccumulativeCounter.h', 'Counter.h', 'CounterBase.h',
        'CounterCreator.h', 'CounterMap.h', 'MultiCounter.h', 'StateCounter.h',
        'StringCounter.h'
    ],
    visibility=[
        '//aios/plugins/havenask_plugins/bs_reader_plugins:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=[
        '//aios/autil:json', '//aios/autil:lock', '//aios/autil:thread',
        '//aios/storage/indexlib/util:Exception'
    ]
)
