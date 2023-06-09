/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @file: common.h
 * @author:
 * @date:
 * $Id$
 *
 * @desc:  *** add description here ***
 */

#ifndef __COMMON_H_
#define __COMMON_H_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#include <stdint.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>
#include <sys/syscall.h>

namespace sap {

/* define BOOLEAN. */
#define BOOL int
#define TRUE 1
#define FALSE 0


/* define common ret_t type and value. */
enum ret_t
{
    r_succeed = 0,
    r_failed = 1,
    r_nullpoint,
    r_enomem,
    r_outofbounds,
    r_einval,
    r_epthread,
    r_uninitialized,
    r_eintr,
    r_eio,
    r_eaccess,
    r_eagain,
    r_eof,
    r_timeout,
    r_yield_thread,
    r_ignore
};

enum class coroutine_t {
    c_none = 0,
    c_easy = 1,
    c_lazy = 2,
    c_holo = 3,
    c_uthr = 4,

    // for unittest
    c_test = 128
};

enum class thread_pool_t {
    t_easy = 0,
    t_holo = 1
};

enum class stream_comparator_t {
    sc_none = 0,
    sc_string = 1,
    sc_data_buffer_int_t = 2
};

enum class config_generator_t {
    cg_none = 0,
    cg_coroutine_type = 1
};

#ifndef SAP_LIKELY
#if __GNUC__ > 2 || __GNUC_MINOR__ >= 96
    #define SAP_LIKELY(x)       __builtin_expect(!!(x),1)
    #define SAP_UNLIKELY(x)     __builtin_expect(!!(x),0)
    #define SAP_EXPECTED(x,y)   __builtin_expect((x),(y))
#else //__GNUC__ > 2 || __GNUC_MINOR__ >= 96
    #define SAP_LIKELY(x)       (x)
    #define SAP_UNLIKELY(x)     (x)
    #define SAP_EXPECTED(x,y)   (x)
#endif //__GNUC__ > 2 || __GNUC_MINOR__ >= 96

# if __WORDSIZE == 64
#  define __INT64_C(c)  c ## L
#  define __UINT64_C(c) c ## UL
# else
#  define __INT64_C(c)  c ## LL
#  define __UINT64_C(c) c ## ULL
# endif
#endif

/* Limits of integral types.  */

/* Minimum of signed integral types.  */
# define INT8_MIN               (-128)
# define INT16_MIN              (-32767-1)
# define INT32_MIN              (-2147483647-1)
# define INT64_MIN              (-__INT64_C(9223372036854775807)-1)
/* Maximum of signed integral types.  */
# define INT8_MAX               (127)
# define INT16_MAX              (32767)
# define INT32_MAX              (2147483647)
# define INT64_MAX              (__INT64_C(9223372036854775807))

/* Maximum of unsigned integral types.  */
# define UINT8_MAX              (255)
# define UINT16_MAX             (65535)
# define UINT32_MAX             (4294967295U)
# define UINT64_MAX             (__UINT64_C(18446744073709551615))

/* define EXTERN_C_BEGIN/EXTERN_C_END. */
#ifdef __cplusplus
#  define EXTERN_C_BEGIN extern "C" {
#  define EXTERN_C_END }
#else  //__cplusplus
#  define EXTERN_C_BEGIN
#  define EXTERN_C_END
#endif //__cplusplus

/* define for close assign operator and copy constructor. */
#define COPY_CONSTRUCTOR(T) \
    T(const T &); \
T & operator=(const T &);

/* define STR. */
#define _STR(x) # x
#define STR(x) _STR(x)

/* define SAFE_STRING */
#define SAFE_STRING(x) (x ? x : "(null)")
#define SAFE_STRING_LENGTH(x) (x ? strlen(x) : 0)
#define SAFE_STRING_EQUAL(x,y) (x && y ? (strcmp(x, y) == 0) : (x == y))

/* common define. */
#ifndef NULL
#define NULL 0
#endif //NULL

#define COMMON_FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)


#ifndef IEXIT
#define IEXIT(x) exit(x)
#endif //IEXIT

#ifndef IEXIT_SUC
//#define IEXIT_SUC kill(getpid(), SIGTERM)
#define IEXIT_SUC exit(0)
#endif //IEXIT_SUC

#ifndef IEXIT_ERR
#define IEXIT_ERR kill(getpid(), SIGTERM)
//#define IEXIT_ERR kill(getpid(), SIGUSR2)
//#define IEXIT_ERR exit(-1)
#endif //IEXIT_ERR

#define gettid() syscall(__NR_gettid)
}

#endif //_COMMON_H_
