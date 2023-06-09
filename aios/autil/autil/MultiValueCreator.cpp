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
#include "autil/MultiValueCreator.h"

#include <iosfwd>

using namespace std;
using namespace autil::mem_pool;

namespace autil {


template<typename StringType>
char* createMultiStringBufferT(
        const StringType *data, size_t tokenNum, PoolBase* pool){
    size_t offsetLen = 0;
    size_t bufLen = MultiValueFormatter::calculateMultiStringBufferLen(
            data, tokenNum, offsetLen);
    char* buffer = POOL_COMPATIBLE_NEW_VECTOR(pool, char, bufLen);
    MultiValueFormatter::formatMultiStringToBuffer(
            buffer, bufLen, offsetLen,
            data, tokenNum);
    return buffer;
}

char* MultiValueCreator::createMultiStringBuffer(
        const std::vector<std::string>& strVec,
        mem_pool::PoolBase* pool)
{
    return createMultiStringBufferT(strVec.data(), strVec.size(), pool);
}

char* MultiValueCreator::createMultiStringBuffer(
        const std::vector<StringView>& strVec,
        mem_pool::PoolBase* pool)
{
    return createMultiStringBufferT(strVec.data(), strVec.size(), pool);
}

char *MultiValueCreator::createMultiValueBuffer(
        const std::string* data, size_t size,
        mem_pool::PoolBase* pool)
{
    return createMultiStringBufferT(data, size, pool);
}

char *MultiValueCreator::createMultiValueBuffer(
        const MultiChar* data, size_t size,
        mem_pool::PoolBase* pool)
{
    return createMultiStringBufferT(data, size, pool);
}

char *MultiValueCreator::createMultiValueBuffer(
        const StringView* data, size_t size,
        mem_pool::PoolBase* pool)
{
    return createMultiStringBufferT(data, size, pool);
}

char* MultiValueCreator::createNullMultiValueBuffer(
        mem_pool::PoolBase* pool)
{
    size_t bufLen = MultiValueFormatter::getEncodedCountLength(
            MultiValueFormatter::VAR_NUM_NULL_FIELD_VALUE_COUNT);
    char* buffer = POOL_COMPATIBLE_NEW_VECTOR(pool, char, bufLen);
    char* writeBuf = buffer;
    assert(buffer);
    MultiValueFormatter::encodeCount(
            MultiValueFormatter::VAR_NUM_NULL_FIELD_VALUE_COUNT,
            writeBuf, bufLen);
    return buffer;
}


}
