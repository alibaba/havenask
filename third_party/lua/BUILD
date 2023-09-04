load('//bazel:defs.bzl', 'rpm_library')
hdrs = ['lauxlib.h', 'lualib.h', 'lua.h', 'lua.hpp', 'luaconf.h']
static_lib = None
shared_lib = 'liblua.so'
rpm_library(
    name='lua',
    rpms=['@lua//file:file', '@lua-devel//file:file'],
    hdrs=hdrs,
    static_lib=static_lib,
    shared_lib=shared_lib,
    include_path='usr/include/',
    lib_path='usr/lib64/'
)
