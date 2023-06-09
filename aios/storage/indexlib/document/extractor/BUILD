load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='IDocumentInfoExtractorFactory',
    srcs=[],
    hdrs=['DocumentInfoExtractorType.h', 'IDocumentInfoExtractorFactory.h'],
    deps=[
        ':IDocumentInfoExtractor', '//aios/storage/indexlib/config:IIndexConfig'
    ]
)
indexlib_cc_library(
    name='IDocumentInfoExtractor',
    srcs=[],
    hdrs=['IDocumentInfoExtractor.h'],
    deps=['//aios/autil:NoCopyable']
)
