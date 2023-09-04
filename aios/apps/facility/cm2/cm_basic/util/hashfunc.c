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
#include "aios/apps/facility/cm2/cm_basic/util/hashfunc.h"

uint64_t bkdr_hash_func(const void *str)
{
    const uint64_t seed = 131; // 31 131 1313 13131 131313 etc..
    uint64_t hash = 0;
    const unsigned char *p;
    p = (const unsigned char *)str;
    while (*p != '\0') {
        hash = hash * seed + (*p++);
    }

    return hash;
}

uint64_t bkdr_hash_func_ex(const void *str, const int len)
{
    const uint64_t seed = 131; // 31 131 1313 13131 131313 etc..
    uint64_t hash = 0;
    const unsigned char *p;
    const unsigned char *end_ptr;

    end_ptr = (const unsigned char *)str + len;
    for (p=(const unsigned char *)str; p<end_ptr; p++) {
        hash = hash * seed + (*p);
    }
    return hash;
}

uint64_t elf_hash_func(const void *str)
{
    uint64_t hash = 0 ;
    uint64_t x = 0 ;
    const unsigned char *p;
    p = (const unsigned char *)str;
    while (*p) {
        hash = (hash << 4) + (*p++);
        if ((x = hash & __UINT64_C(0xF000000000000000)) != 0) {
            hash ^= (x >> 56);
            hash &= ~x;
        }
    }
    return hash;
}

uint64_t elf_hash_func_ex(const void *str, const int len)
{
    uint64_t hash = 0 ;
    uint64_t x = 0 ;
    const unsigned char *p;
    const unsigned char *end_ptr;

    end_ptr = (const unsigned char *)str + len;
    for (p=(const unsigned char *)str; p<end_ptr; p++) {
        hash = (hash << 4) + (*p);
        if ((x = hash & __UINT64_C(0xF000000000000000)) != 0) {
            hash ^= (x >> 56);
            hash &= ~x;
        }
    }
    return hash;
}

uint64_t rs_hash_func(const void *str)
{
    const uint64_t b = 378551;
    uint64_t a = 63689;
    uint64_t hash = 0;
    const unsigned char *p;

    p = (const unsigned char *)(str);
    while (*p) {
        hash = hash * a + (*p++);
        a *= b;
    } 
    return hash;
} 

uint64_t rs_hash_func_ex(const void *str, const int len)
{
    const uint64_t b = 378551;
    uint64_t a = 63689;
    uint64_t hash = 0;
    const unsigned char *p;
    const unsigned char *end_ptr;

    end_ptr = (const unsigned char *)str + len;
    for (p=(const unsigned char *)str; p<end_ptr; p++) {
        hash = hash * a + (*p);
        a *= b;
    }
    return hash;
}

uint64_t js_hash_func(const void *str)
{
    uint64_t hash = 1315423911;
    const unsigned char *p;
    p = (const unsigned char *)str;
    while (*p) {
        hash ^= ((hash << 5) + (*p++) + (hash >> 2));
    } 
    return hash;
} 

uint64_t js_hash_func_ex(const void *str, const int len)
{
    uint64_t hash = 1315423911;
    const unsigned char *p;
    const unsigned char *end_ptr;
    end_ptr = (const unsigned char *)str + len;
    for (p=(const unsigned char *)str; p<end_ptr; p++) {
        hash ^= ((hash << 5) + (*p) + (hash >> 2));
    } 
    return hash;
}

uint64_t sdbm_hash_func(const void *str)
{
    uint64_t hash = 0;
    const unsigned char *p;
    p = (const unsigned char *)str;
    while (*p) {
        hash = (*p++) + (hash << 6) + (hash << 16) - hash;
    } 
    return hash;
}

uint64_t sdbm_hash_func_ex(const void *str, const int len)
{
    uint64_t hash = 0;
    const unsigned char *p;
    const unsigned char *end_ptr;
    end_ptr = (const unsigned char *)str + len;
    for (p=(const unsigned char *)str; p<end_ptr; p++) {
        hash = (*p) + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
} 

uint64_t time33_hash_func(const void *str)
{
    uint64_t hash = 0;
    const unsigned char *p;
    p = (const unsigned char *)str;
    while (*p) {
		  hash += (hash << 5) + (*p++);
    } 
    return hash;
}

uint64_t time33_hash_func_ex(const void *str, const int len)
{
	uint64_t hash = 0;
	const unsigned char *p;
	const unsigned char *end_ptr;

	end_ptr = (const unsigned char *)str + len;
	for (p = (const unsigned char *)str; p < end_ptr; p++) {
		hash += (hash << 5) + (*p);
	}
	return hash;
}

