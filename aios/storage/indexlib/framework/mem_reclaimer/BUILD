load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(name='IIndexMemoryReclaimer', srcs=[])
strict_cc_library(
    name='EpochBasedMemReclaimer',
    deps=[':IIndexMemoryReclaimer', ':MemReclaimerMetrics', '//aios/autil:log']
)
strict_cc_library(
    name='MemReclaimerMetrics',
    deps=[
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
