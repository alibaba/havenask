cc_test(
    name='tools_test',
    srcs=glob(['*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        'testdata/normal_schema.json', 'testdata/orc_schema.json',
        '//aios/catalog:testdata'
    ],
    deps=['//aios/catalog/tools', '//aios/unittest_framework']
)
