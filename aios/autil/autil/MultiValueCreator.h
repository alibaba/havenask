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

#include <assert.h>
#include <stddef.h>
#include <string>
#include <type_traits>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/MultiValueFormatter.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/Span.h"

namespace autil {

class MultiValueCreator
{
public:
    MultiValueCreator() {};
    ~MultiValueCreator() {};
private:
    MultiValueCreator(const MultiValueCreator &);
    MultiValueCreator& operator = (const MultiValueCreator &);
public:
    static char* createMultiStringBuffer(const std::vector<std::string>& strVec,
                                         mem_pool::PoolBase* pool = NULL);

    static char* createMultiStringBuffer(const std::vector<StringView>& strVec,
                                         mem_pool::PoolBase* pool = NULL);

    template <typename T>
    static typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char*>::type
    createMultiValueBuffer(const std::vector<T>& values,
            mem_pool::PoolBase* pool = NULL);
    static char *createMultiValueBuffer(const std::vector<std::string> &data,
            mem_pool::PoolBase* pool = NULL);
    static char *createMultiValueBuffer(const std::vector<StringView> &data,
            mem_pool::PoolBase* pool = NULL);

    template <typename T>
    static typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char*>::type
    createMultiValueBuffer(const T* data, size_t size,
                           mem_pool::PoolBase* pool = NULL);
    template <typename T>
    static typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char *>::type
    createMultiValueBufferSeg(size_t segNum, const T *data[], size_t size[], mem_pool::PoolBase *pool = NULL);
    static char *createMultiValueBuffer(const std::string* data, size_t size,
            mem_pool::PoolBase* pool = NULL);
    static char *createMultiValueBuffer(const MultiChar* data, size_t size,
            mem_pool::PoolBase* pool = NULL);
    static char *createMultiValueBuffer(const StringView *data, size_t size,
            mem_pool::PoolBase* pool = NULL);

    static char* createNullMultiValueBuffer(mem_pool::PoolBase* pool = NULL);
};

template <typename T>
inline typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char*>::type
MultiValueCreator::createMultiValueBuffer(
        const std::vector<T>& values,
        mem_pool::PoolBase* pool)
{
    return createMultiValueBuffer(values.data(), values.size(), pool);
}

inline char* MultiValueCreator::createMultiValueBuffer(
        const std::vector<std::string>& values,
        mem_pool::PoolBase* pool)
{
    return createMultiValueBuffer(values.data(), values.size(), pool);
}

inline char* MultiValueCreator::createMultiValueBuffer(
        const std::vector<StringView>& values,
        mem_pool::PoolBase* pool)
{
    return createMultiValueBuffer(values.data(), values.size(), pool);
}

template <typename T>
inline typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char*>::type
MultiValueCreator::createMultiValueBuffer(const T* data, size_t size,
        mem_pool::PoolBase* pool)
{
    const T* dataIn[1] = {data};
    size_t sizeIn[1] = {size};
    return createMultiValueBufferSeg<T>(1, dataIn, sizeIn, pool);
}

template <typename T>
inline typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), char *>::type
MultiValueCreator::createMultiValueBufferSeg(size_t segNum, const T *data[], size_t size[], mem_pool::PoolBase *pool) {
    size_t totalSize = std::accumulate(size, size + segNum, 0);
    size_t bufLen = MultiValueFormatter::calculateBufferLen(totalSize, sizeof(T));
    char* buffer = POOL_COMPATIBLE_NEW_VECTOR(pool, char, bufLen);
    assert(buffer);
    MultiValueFormatter::formatToBufferSeg<T>(segNum, data, size, buffer, bufLen);
    return buffer;
}
}
