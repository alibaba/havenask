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

#include <cmath>
#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"

namespace indexlib { namespace util {

class BlockFpEncoder
{
private:
    BlockFpEncoder();
    ~BlockFpEncoder();

public:
    static int32_t Encode(const float* input, size_t count, char* output, size_t outBufferSize);

    static autil::StringView Encode(const float* input, size_t count, autil::mem_pool::Pool* pool);

    static int32_t Decode(const autil::StringView& data, char* output, size_t outputLen)
    {
        if (data.empty()) {
            return -1;
        }
        size_t valueCount = GetDecodeCount(data.size());
        if (valueCount * sizeof(float) > outputLen) {
            return -1;
        }
        const char* buffer = data.data();
        char maxExponent = buffer[0] - 127;
        buffer++;
        int16_t* fractionBuffer = (int16_t*)buffer;
        for (size_t i = 0; i < valueCount; i++) {
            ((float*)output)[i] = ((float)(fractionBuffer[i]) / FLOAT_BITS_VALUE) * std::pow(2, maxExponent);
        }
        return valueCount;
    }

    static size_t GetEncodeBytesLen(size_t count) { return (count << 1) + 1; }

    static size_t GetDecodeCount(size_t dataLen) { return dataLen == 0 ? 0 : (dataLen - 1) >> 1; }

private:
    typedef struct FpSingle_ {
        uint32_t nFraction : 23;
        uint32_t nExponent : 8;
        uint32_t nSign     : 1;
    } FpSingle;

    typedef struct Int16_ {
        uint16_t nFraction : 15;
        uint16_t nSign     : 1;
    } Int16;

private:
    static const int FLOAT_BITS = 15;
    static const int FLOAT_BITS_VALUE;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::util
