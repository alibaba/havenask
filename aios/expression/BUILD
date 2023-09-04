load('//bazel:defs.bzl', 'genlex', 'genyacc')
genlex(name='scanner', src='expression/syntax/Scanner.ll')
genyacc(name='parser', src='expression/syntax/BisonParser.yy')
cc_library(
    name='expression',
    srcs=(
        glob([
            'expression/common/*.cpp', 'expression/framework/*.cpp',
            'expression/function/*.cpp', 'expression/plugin/*.cpp',
            'expression/syntax/*.cpp', 'expression/util/*.cpp'
        ]) + [':scanner', ':parser_srcs']
    ),
    hdrs=glob([
        'expression/*.h', 'expression/common/*.h', 'expression/framework/*.h',
        'expression/function/*.h', 'expression/plugin/*.h',
        'expression/syntax/*.h', 'expression/util/*.h'
    ]),
    copts=['-Wno-register', '-Werror'],
    include_prefix='expression',
    strip_include_prefix='expression',
    visibility=['//visibility:public'],
    deps=[
        ':parser_hdrs', '//aios/autil:NoCopyable', '//aios/matchdoc',
        '//aios/resource_reader', '@rapidjson'
    ]
)
