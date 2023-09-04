package(default_visibility=['//aios/suez_turing:__subpackages__'])
load('//bazel:defs.bzl', 'genyacc', 'genlex')
genlex(name='scanner', src='Scanner.ll')
genyacc(
    name='parser',
    src='BisonParser.yy',
    path='suez/turing/expression/syntax/parser/'
)
