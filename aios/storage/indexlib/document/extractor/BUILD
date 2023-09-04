load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='IDocumentInfoExtractorFactory',
    srcs=[],
    hdrs=['DocumentInfoExtractorType.h', 'IDocumentInfoExtractorFactory.h'],
    deps=[
        ':IDocumentInfoExtractor', '//aios/storage/indexlib/config:IIndexConfig'
    ]
)
strict_cc_library(
    name='IDocumentInfoExtractor',
    srcs=[],
    hdrs=['IDocumentInfoExtractor.h'],
    deps=['//aios/autil:NoCopyable']
)
