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
 * =====================================================================================
 * 
 *       Filename:  common.h
 * 
 *    Description:  cm_basic common define
 * 
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 * 
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 * 
 * =====================================================================================
 */

#ifndef __CM_BASIC_COMMON_H_
#define __CM_BASIC_COMMON_H_

#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <unistd.h>
#include <memory>
#include <sys/syscall.h>
#if __GNUC_PREREQ(4,3)
#else
#include <byteswap.h>
#endif

namespace cm_basic {

/* define BOOLEAN. */

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

/* define for close assign operator and copy constructor. */
#define COPY_CONSTRUCTOR(T) \
    T(const T &); \
T & operator=(const T &);

/* define STR. */
#define _STR(x) # x
#define STR(x) _STR(x)

/* define deletePtr */
#define deletePtr(ptr) do{\
    if (ptr) { \
        delete ptr; ptr = NULL; \
    } \
}while(0)

/* define SAFE_STRING */
#define SAFE_STRING(x) (x ? x : "(null)")
#define SAFE_STRING_LENGTH(x) (x ? strlen(x) : 0)
#define SAFE_STRING_EQUAL(x,y) (x && y ? (strcmp(x, y) == 0) : (x == y))

/* common define. */
#ifndef NULL
#define NULL 0
#endif //NULL

#define BOOL int
#define TRUE 1
#define FALSE 0

#define IP_LEN 256
#define DEFAULT_RWBUFFER_SIZE 4096
#define DEFAULT_SOCK_BACKLOG 256

#define   INVALID_POINT            0xFFFFFFFFFFFFFFFE      // 不可用指针


/** 对前一个值按照后一个值取对齐， 后一个值必须是  1 << n 得到的 */
#define  align_ptr(p, a) \
    (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))

/** 向上取整对齐, 后一个值必须是  1 << n 得到 */
#define  align_int(d, a)  \
    (((d) + (a - 1)) & ~(a - 1))


/** 向上取整对齐, 按任意数对齐 */
#define  align_int2(d, a)  \
    ( (0 != d%a)?( ((d/a) + 1)*a ):d )



/** 64位数字的大小端转换 */
#if __GNUC_PREREQ(4,3)
    #define uint64_bswap( u )    __builtin_bswap64(u)
#else
    #define uint64_bswap( u )    __bswap_64(u)
#endif



#ifndef CACHE_LINE_SIZE
/**  计算形成 64字节 cache line 对齐  */
#define  ALC_CACHE_LINE_ALIGN_SIZE(SZ) \
    (((((SZ) - 1) / CACHE_LINE_SIZE) + 1) * CACHE_LINE_SIZE)
#define  CACHE_LINE_SIZE (64)
#define  CACHE_LINE_MASK (CACHE_LINE_SIZE - 1)
#endif

/** 释放内存， 避免野指针 */
#define SAFE_FREE(p)    if (p != NULL) {free (p); p = NULL;}
#define SAFE_DELETE(p)  if (p != NULL) {delete (p); p = NULL;}
#define SAFE_DEL_ARR(p) if (p != NULL) {delete[] (p); p = NULL;}

#define DEFINE_SHARED_PTR(x) typedef std::shared_ptr<x> x##Ptr
#define DEFINE_SHARED_CONST_PTR(x) typedef std::shared_ptr<const x> x##Const##Ptr
#define DYNAMIC_POINTER_CAST(x, y) std::dynamic_pointer_cast<x>(y)

const char enum_protocol_type[5][8] = {"tcp", "udp", "http", "any", "rdma"};   // 协议类型对应的字符串
const char enum_online_status[3][8] = {"online", "offline", "initing" };
const char enum_node_status[5][16] = {"normal", "abnormal", "timeout", "invalid", "uninit" };
const char enum_topo_type[2][32] = {"one_map_one", "cluster_map_table" };
const char enum_check_type[6][16] = {"heartbeat", "7level_check", "4level_check", "keep_online", "keep_offline", "cascade" };
}

#endif
