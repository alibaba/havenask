load('//bazel:defs.bzl', 'rpm_library')
rpm_library(
    name='readline',
    rpms=['@readline_file//file:file', '@readline-devel_file//file:file'],
    hdrs=[
        'readline/chardefs.h', 'readline/history.h', 'readline/keymaps.h',
        'readline/readline.h', 'readline/rlconf.h', 'readline/rlstdc.h',
        'readline/rltypedefs.h', 'readline/tilde.h'
    ],
    shared_lib='libreadline.so.6',
    include_path='usr/include',
    lib_path='lib64'
)
