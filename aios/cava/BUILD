load('//bazel:defs.bzl', 'genlex', 'genyacc', 'test_bc_code')
cc_library(
    name='cava_adapter',
    visibility=['//visibility:public'],
    deps=[':havenask_cava_mock']
)
genlex(name='scanner', src='cava/parse/Scanner.ll')
genyacc(name='parser', src='cava/parse/Parser.yy')
cc_library(
    name='havenask_cava_mock',
    srcs=glob(['havenask_mock/**/*.cpp']),
    hdrs=glob(['havenask_mock/**/*.h', 'havenask_mock/**/*.hh']),
    include_prefix='cava',
    strip_include_prefix='havenask_mock',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:json', '//aios/autil:mem_pool_base',
        '//aios/autil:string_type', '//aios/autil:thread',
        '//aios/filesystem/fslib:fslib-framework', '@llvm//:bit_writer',
        '@llvm//:ipo', '@llvm//:ir_reader', '@llvm//:linker', '@llvm//:orc_jit',
        '@llvm//:x86_code_gen', '@llvm//:x86_info'
    ],
    alwayslink=1
)
test_bc_code(
    name='test_jit_test',
    srcs='testdata/testcode/foo.cpp',
    copy_name='jit_test.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib', ':foo_includes']
)
test_bc_code(
    name='test_foo_bc',
    srcs='testdata/testcode/foo.cpp',
    copy_name='foo.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib', ':foo_includes']
)
test_bc_code(
    name='test_global_bc',
    srcs='testdata/testcode/global.cpp',
    copy_name='global.bc',
    dests=['testdata/bitcode/'],
    deps=[]
)
test_bc_code(
    name='test_bar_bc',
    srcs='cava/codegen/test/testcode/Bar.cpp',
    copy_name='bar.bc',
    dests=['testdata/bitcode/'],
    deps=[':bar_headers', ':cava_inner_lib']
)
test_bc_code(
    name='test_inner_bc',
    srcs='testdata/testcode/inner.cpp',
    copy_name='inner.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib', ':inner_headers']
)
test_bc_code(
    name='test_wrapper_bc',
    srcs='testdata/testcode/wrapper.cpp',
    copy_name='wrapper.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib', ':inner_headers']
)
test_bc_code(
    name='test_system_bc',
    srcs='cava/lang/System.cpp',
    copy_name='system.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib']
)
test_bc_code(
    name='test_cstring_bc',
    srcs='cava/runtime/CString.cpp',
    copy_name='cstring.bc',
    dests=['testdata/bitcode/'],
    deps=[':cava_inner_lib']
)
