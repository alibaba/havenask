load('//bazel:defs.bzl', 'genyacc', 'genlex', 'test_bc_code')
cc_library(
    name='cava_adapter',
    deps=[':havenask_cava_mock'],
    visibility=['//visibility:public']
)
genlex(name='scanner', src='cava/parse/Scanner.ll')
genyacc(name='parser', src='cava/parse/Parser.yy')
cc_library(
    name='havenask_cava_mock',
    hdrs=glob(['havenask_mock/**/*.h', 'havenask_mock/**/*.hh']),
    srcs=glob(['havenask_mock/**/*.cpp']),
    deps=[
        '@llvm//:orc_jit', '@llvm//:bit_writer', '@llvm//:ir_reader',
        '@llvm//:linker', '@llvm//:ipo', '@llvm//:x86_info',
        '@llvm//:x86_code_gen', '//aios/autil:mem_pool_base',
        '//aios/autil:json', '//aios/autil:string_type', '//aios/autil:thread',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    visibility=['//visibility:public'],
    include_prefix='cava',
    strip_include_prefix='havenask_mock',
    alwayslink=1
)
test_bc_code(
    name='test_jit_test',
    srcs='testdata/testcode/foo.cpp',
    deps=[':cava_inner_lib', ':foo_includes'],
    copy_name='jit_test.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_foo_bc',
    srcs='testdata/testcode/foo.cpp',
    deps=[':cava_inner_lib', ':foo_includes'],
    copy_name='foo.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_global_bc',
    srcs='testdata/testcode/global.cpp',
    deps=[],
    copy_name='global.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_bar_bc',
    srcs='cava/codegen/test/testcode/Bar.cpp',
    deps=[':cava_inner_lib', ':bar_headers'],
    copy_name='bar.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_inner_bc',
    srcs='testdata/testcode/inner.cpp',
    deps=[':cava_inner_lib', ':inner_headers'],
    copy_name='inner.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_wrapper_bc',
    srcs='testdata/testcode/wrapper.cpp',
    deps=[':cava_inner_lib', ':inner_headers'],
    copy_name='wrapper.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_system_bc',
    srcs='cava/lang/System.cpp',
    deps=[':cava_inner_lib'],
    copy_name='system.bc',
    dests=['testdata/bitcode/']
)
test_bc_code(
    name='test_cstring_bc',
    srcs='cava/runtime/CString.cpp',
    deps=[':cava_inner_lib'],
    copy_name='cstring.bc',
    dests=['testdata/bitcode/']
)
