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
 *       Filename:  hash_func.h
 *
 *    Description:  字符串解析库
 *
 *        Version:  0.1
 *        Created:  2013-05-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __HASH_FUNC_H_
#define __HASH_FUNC_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief BKDR hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t bkdr_hash_func(const void* str);

/**
 * @brief BKDR hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t bkdr_hash_func_ex(const void* str, const int len);

/**
 * @brief ELF hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t elf_hash_func(const void* str);

/**
 * @brief ELF hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t elf_hash_func_ex(const void* str, const int len);

/**
 * @brief RS hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t rs_hash_func(const void* str);

/**
 * @brief RS hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t rs_hash_func_ex(const void* str, const int len);

/**
 * @brief JS hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t js_hash_func(const void* str);

/**
 * @brief JS hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t js_hash_func_ex(const void* str, const int len);

/**
 * @brief SDBM hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t sdbm_hash_func(const void* str);

/**
 * @brief SDMB hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t sdbm_hash_func_ex(const void* str, const int len);

/**
 * @brief TIME33 hash function for char* ending with '\\0'
 * @param str the string
 * @return uint64_t hash code
 */
uint64_t time33_hash_func(const void* str);

/**
 * @brief TIME33 hash function for const char* with given length
 * @param str the string
 * @param len the length of input string
 * @return uint64_t hash code
 */
uint64_t time33_hash_func_ex(const void* str, const int len);

#ifdef __cplusplus
}
#endif

#endif
