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
#pragma once
#include <stddef.h>
#include <stdint.h>


namespace isearch {
namespace util {

typedef uint64_t u_n64_t;
typedef uint32_t u_n32_t;
typedef uint16_t u_n16_t;
typedef uint8_t  u_n8_t;
typedef int64_t n64_t;
typedef int32_t n32_t;
typedef int16_t n16_t;
typedef int8_t  n8_t;

template <typename Key>
struct KeyCompare {
};
template<>
struct KeyCompare<unsigned int> {
    inline bool operator()(const unsigned int nKey1, const unsigned int nKey2) const {
        return nKey1 < nKey2;
    }
};
template < typename Type, typename Compare = KeyCompare<Type> >
struct BinarySearch {
    Compare comp;
    inline n32_t operator()(Type *szElements, n32_t nBegin, n32_t nEnd, Type _Value) {
        n32_t nPos;
        while (nBegin <= nEnd) {
            nPos = (nBegin + nEnd) / 2;
            if (!comp(szElements[nPos], _Value)) {
                nEnd = nPos - 1;
            }
            else {
                nBegin = nPos + 1;
            }
        }
        return nBegin;
    }
};

struct CacheItem {
    CacheItem(): key(0), hash_next(NULL) {}
    u_n64_t   key;
    CacheItem  *hash_next;
};

} // namespace util
} // namespace isearch
