cc_library(
    name='minizip',
    srcs=[
        'mz_crypt.c', 'mz_os.c', 'mz_os_posix.c', 'mz_strm.c', 'mz_strm_buf.c',
        'mz_strm_mem.c', 'mz_strm_os_posix.c', 'mz_strm_split.c',
        'mz_strm_zlib.c', 'mz_zip.c', 'mz_zip_rw.c'
    ],
    hdrs=glob(['*.h']),
    copts=[
        '-DHAVE_STDINT_H', '-DHAVE_INTTYPES_H', '-DHAVE_ZLIB',
        '-DMZ_ZIP_NO_ENCRYPTION', '-D_POSIX_C_SOURCE=200112L'
    ],
    include_prefix='',
    strip_include_prefix='',
    visibility=['//visibility:public'],
    deps=['@zlib_archive//:zlib']
)
