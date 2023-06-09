/* mz_strm_libcomp.h -- Stream for apple compression
   Version 2.8.9, July 4, 2019
   part of the MiniZip project

   Copyright (C) 2010-2019 Nathan Moinvaziri
      https://github.com/nmoinvaz/minizip

   This program is distributed under the terms of the same license as zlib.
   See the accompanying LICENSE file for the full text of the license.
*/

#ifndef MZ_STREAM_LIBCOMP_H
#define MZ_STREAM_LIBCOMP_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/***************************************************************************/

int32_t mz_stream_libcomp_open(void* stream, const char* filename, int32_t mode);
int32_t mz_stream_libcomp_is_open(void* stream);
int32_t mz_stream_libcomp_read(void* stream, void* buf, int32_t size);
int32_t mz_stream_libcomp_write(void* stream, const void* buf, int32_t size);
int64_t mz_stream_libcomp_tell(void* stream);
int32_t mz_stream_libcomp_seek(void* stream, int64_t offset, int32_t origin);
int32_t mz_stream_libcomp_close(void* stream);
int32_t mz_stream_libcomp_error(void* stream);

int32_t mz_stream_libcomp_get_prop_int64(void* stream, int32_t prop, int64_t* value);
int32_t mz_stream_libcomp_set_prop_int64(void* stream, int32_t prop, int64_t value);

void* mz_stream_libcomp_create(void** stream);
void mz_stream_libcomp_delete(void** stream);

void* mz_stream_libcomp_get_interface(void);
void* mz_stream_libcomp_get_crc32_update(void);

/***************************************************************************/

int32_t mz_stream_zlib_open(void* stream, const char* filename, int32_t mode);
int32_t mz_stream_zlib_is_open(void* stream);
int32_t mz_stream_zlib_read(void* stream, void* buf, int32_t size);
int32_t mz_stream_zlib_write(void* stream, const void* buf, int32_t size);
int64_t mz_stream_zlib_tell(void* stream);
int32_t mz_stream_zlib_seek(void* stream, int64_t offset, int32_t origin);
int32_t mz_stream_zlib_close(void* stream);
int32_t mz_stream_zlib_error(void* stream);

int32_t mz_stream_zlib_get_prop_int64(void* stream, int32_t prop, int64_t* value);
int32_t mz_stream_zlib_set_prop_int64(void* stream, int32_t prop, int64_t value);

void* mz_stream_zlib_create(void** stream);
void mz_stream_zlib_delete(void** stream);

void* mz_stream_zlib_get_interface(void);
void* mz_stream_zlib_get_crc32_update(void);

/***************************************************************************/

#ifdef __cplusplus
}
#endif

#endif
