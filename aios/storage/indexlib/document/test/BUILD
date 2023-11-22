load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='KVDocumentBatchMaker',
    testonly=True,
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=([
        '//aios/storage/indexlib/document/kkv:KKVDocumentFactory',
        '//aios/storage/indexlib/document/kv:KVDocumentFactory',
        '//aios/storage/indexlib/document/raw_document/test:RawDocumentMaker'
    ] + [])
)
strict_cc_library(
    name='NormalDocumentMaker',
    testonly=True,
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=([
        '//aios/storage/indexlib/document:PlainDocument',
        '//aios/storage/indexlib/document/normal:NormalDocumentFactory',
        '//aios/storage/indexlib/document/raw_document/test:RawDocumentMaker',
        '//aios/storage/indexlib/framework:tablet_schema_loader'
    ] + [])
)
