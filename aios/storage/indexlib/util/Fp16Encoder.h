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

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/Half.h"

namespace indexlib { namespace util {

class Fp16Encoder
{
public:
    Fp16Encoder() {}
    ~Fp16Encoder() {}

public:
    static autil::StringView Encode(const float* input, size_t count, autil::mem_pool::Pool* pool)
    {
        if (!input || !pool || count == 0) {
            return autil::StringView::empty_instance();
        }
        size_t bufferSize = GetEncodeBytesLen(count);
        char* buffer = (char*)pool->allocate(bufferSize);
        int32_t compLen = Encode(input, count, buffer, bufferSize);
        assert(compLen == (int32_t)bufferSize);
        return autil::StringView(buffer, compLen);
    }

    static void Encode(const float input, char* output)
    {
        half_float::half temp = (half_float::half)(input);
        memcpy(output, &temp, sizeof(half_float::half));
    }

    static int32_t Encode(const float* input, size_t count, char* output, size_t outBufferSize)
    {
        size_t bufferSize = GetEncodeBytesLen(count);
        if (outBufferSize < bufferSize) {
            return -1;
        }

        char* buffer = output;
        half_float::half* cursor = (half_float::half*)buffer;
        for (size_t i = 0; i < count; i++) {
            cursor[i] = (half_float::half)(input[i]);
        }
        return (int32_t)bufferSize;
    }

    static int32_t Decode(const autil::StringView& data, char* output)
    {
        if (data.empty() || data.size() != sizeof(half_float::half)) {
            return -1;
        }
        float temp = ((half_float::half*)data.data())[0];
        memcpy(output, &temp, sizeof(float));
        return 1;
    }

    static int32_t Decode(const autil::StringView& data, char* output, size_t outputLen)
    {
        if (data.empty()) {
            return -1;
        }
        size_t valueCount = GetDecodeCount(data.size());
        if (valueCount * sizeof(float) > outputLen) {
            return -1;
        }
        half_float::half* cursor = (half_float::half*)data.data();
        float* outputCursor = (float*)output;
        for (size_t i = 0; i < valueCount; i++) {
            outputCursor[i] = (float)cursor[i];
        }
        return valueCount;
    }

    static size_t GetEncodeBytesLen(size_t count) { return count << 1; }

    static size_t GetDecodeCount(size_t dataLen) { return dataLen >> 1; }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Fp16Encoder> Fp16EncoderPtr;
}} // namespace indexlib::util
