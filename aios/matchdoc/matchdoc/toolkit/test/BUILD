test_data = ['testdata/schema.json']
cc_test(
    name='matchdoc_toolkit_test',
    srcs=[
        'FieldDefaultValueSetterTest.cpp', 'MatchDocDefaultValueSetterTest.cpp',
        'MatchDocHelperTest.cpp', 'MatchDocJoinerTest.cpp'
    ],
    deps=[
        '//aios/matchdoc:matchdoc_toolkit',
        '//aios/unittest_framework:unittest_framework'
    ],
    data=test_data
)
