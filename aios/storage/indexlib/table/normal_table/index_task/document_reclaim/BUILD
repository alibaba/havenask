load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='IndexReclaimerParam', deps=['//aios/autil:json', '//aios/autil:log']
)
strict_cc_library(
    name='IndexReclaimerCreator',
    deps=[
        ':AndIndexReclaimer', ':IndexFieldReclaimer', ':IndexReclaimerParam',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/inverted_index:Common',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexFactory'
    ]
)
strict_cc_library(
    name='IndexReclaimer',
    srcs=[],
    deps=['//aios/storage/indexlib/base:Status']
)
strict_cc_library(
    name='AndIndexReclaimer',
    deps=[
        ':IndexReclaimer', ':IndexReclaimerParam',
        '//aios/storage/indexlib/index/deletionmap:modifier',
        '//aios/storage/indexlib/index/inverted_index:AndPostingExecutor',
        '//aios/storage/indexlib/index/inverted_index:MultiFieldIndexReader',
        '//aios/storage/indexlib/index/inverted_index:TermPostingExecutor'
    ]
)
strict_cc_library(
    name='IndexFieldReclaimer',
    deps=[
        ':IndexReclaimer', ':IndexReclaimerParam',
        '//aios/storage/indexlib/index/deletionmap:modifier',
        '//aios/storage/indexlib/index/inverted_index:MultiFieldIndexReader',
        '//aios/storage/indexlib/index/inverted_index:TermPostingExecutor'
    ]
)
