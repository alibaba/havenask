#ifndef __INDEXLIB_FLOAT_INT8_ENCODER_H
#define __INDEXLIB_FLOAT_INT8_ENCODER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <math.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class FloatInt8Encoder
{
public:
    FloatInt8Encoder() {}
    ~FloatInt8Encoder() {}
    
public:
    static autil::ConstString Encode(float maxAbs,
            const float* input, size_t count, autil::mem_pool::Pool* pool)
    {
        if (!input || !pool || count == 0) {
            return autil::ConstString::EMPTY_STRING;
        }
        size_t bufferSize = GetEncodeBytesLen(count);
        char* buffer = (char*)pool->allocate(bufferSize);
        int32_t compLen = Encode(maxAbs, input, count, buffer, bufferSize);
        assert(compLen == (int32_t)bufferSize);
        return autil::ConstString(buffer, compLen);
    }

    static void Encode(float maxAbs, const float input, char* output)
    {
        const float scale_factor = (float)127 / maxAbs;
        int8_t temp = (int8_t)roundf(std::max(-maxAbs, std::min(input, maxAbs)) * scale_factor);
        memcpy(output, &temp, sizeof(int8_t));
    }

    static int32_t Encode(float maxAbs, const float* input, size_t count,
                          char* output, size_t outBufferSize)
    {
        size_t bufferSize = GetEncodeBytesLen(count);
        if (outBufferSize < bufferSize)
        {
            return -1;
        }

        const float scale_factor = (float)127 / maxAbs;
        int8_t* cursor = (int8_t*)output;
        for (size_t i = 0; i < count; i++) {
            cursor[i] = (int8_t)roundf(std::max(-maxAbs, std::min(input[i], maxAbs)) * scale_factor);
        }
        return (int32_t)bufferSize;
    }

    static int32_t Decode(float maxAbs, const autil::ConstString& data,
                          char* output)
    {
        if (data.empty() || data.size() != sizeof(int8_t)) {
            return -1;
        }
        const float scale = maxAbs / 127;
        float temp = scale * ((int8_t*)data.data())[0];
        memcpy(output, &temp, sizeof(float));
        return 1;
    }

    static int32_t Decode(float maxAbs, const autil::ConstString& data,
                          char* output, size_t outputLen)
    {
        if (data.empty()) {
            return -1;
        }
        size_t valueCount = GetDecodeCount(data.size());
        if (valueCount * sizeof(float) > outputLen) {
            return -1;
        }

        int8_t* cursor = (int8_t*)data.data();
        const float scale = maxAbs / 127;
        float* outputCursor = (float*)output;
        for (size_t i = 0; i < valueCount; i++) {
            outputCursor[i] = scale * (float)cursor[i];
        }
        return valueCount;
    }

    static size_t GetEncodeBytesLen(size_t count) {
        return count;
    }
    
    static size_t GetDecodeCount(size_t dataLen) {
        return dataLen;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FloatInt8Encoder);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_FLOAT_INT8_ENCODER_H
