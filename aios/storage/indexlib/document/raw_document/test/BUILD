load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='RawDocumentMaker',
    testonly=True,
    export_name='raw_document_maker',
    export_visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage:indexlib_extensions',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=['//aios/storage/indexlib/document/raw_document:DefaultRawDocument']
)
