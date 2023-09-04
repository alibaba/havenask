package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='mock_swift_client',
    srcs=['MockSwiftReader.cpp'],
    hdrs=[
        'MockSwiftAdminAdapter.h', 'MockSwiftClient.h', 'MockSwiftReader.h',
        'MockSwiftWriter.h', 'MockSwiftWriterAsyncHelper.h'
    ],
    include_prefix='swift/testlib',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/swift/client:swift_client_no_transport',
        '//aios/autil:json', '//aios/autil:thread',
        '//aios/unittest_framework:unittest_lib',
        '@com_google_protobuf//:protobuf_headers'
    ]
)
