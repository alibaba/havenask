load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(name='IIndexMemoryReclaimer', srcs=[])
indexlib_cc_library(
    name='EpochBasedMemReclaimer',
    deps=[':IIndexMemoryReclaimer', ':MemReclaimerMetrics', '//aios/autil:log']
)
indexlib_cc_library(
    name='MemReclaimerMetrics',
    deps=[
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
