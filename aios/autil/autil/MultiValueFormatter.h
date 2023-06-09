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

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <limits>
#include <numeric>
#include <stdint.h>
#include <string.h>
#include <string>
#include <type_traits>
#include <vector>

#include "autil/LongHashValue.h"
#include "autil/Span.h"

namespace autil {

template <typename T>
class MultiValueType;

template <typename T>
class Span;

#define MULTI_VALUE_FORMAT_SUPPORTED(T)                   \
    std::is_same<T, bool>::value ||                       \
    std::is_same<T, int8_t>::value ||                     \
    std::is_same<T, uint8_t>::value ||                    \
    std::is_same<T, int16_t>::value ||                    \
    std::is_same<T, uint16_t>::value ||                   \
    std::is_same<T, int32_t>::value ||                    \
    std::is_same<T, uint32_t>::value ||                   \
    std::is_same<T, long>::value ||                       \
    std::is_same<T, unsigned long>::value ||              \
    std::is_same<T, long long>::value ||                  \
    std::is_same<T, unsigned long long>::value ||         \
    std::is_same<T, int64_t>::value ||                    \
    std::is_same<T, uint64_t>::value ||                   \
    std::is_same<T, autil::uint128_t>::value ||           \
    std::is_same<T, float>::value ||                      \
    std::is_same<T, double>::value ||                     \
    std::is_same<T, char>::value                          \

#define STRING_TYPE_VALUE_FORMAT_SUPPORTED(T)                           \
    std::is_same<const T, const std::string>::value ||                  \
    std::is_same<const T, const autil::StringView>::value ||            \
    std::is_same<const T, const autil::MultiValueType<char> >::value

class MultiValueFormatter
{
public:
    static const uint32_t VAR_NUM_NULL_FIELD_VALUE_COUNT = 0x3FFFFFFF;
    static const uint32_t UNDECODED_COUNT = VAR_NUM_NULL_FIELD_VALUE_COUNT + 1;
    static const uint32_t VALUE_COUNT_MAX_BYTES = 4;

public:
    MultiValueFormatter() {};
    ~MultiValueFormatter() {};
private:
    MultiValueFormatter(const MultiValueFormatter &);
    MultiValueFormatter& operator = (const MultiValueFormatter &);

public:
    template <typename T>
    static void formatToBuffer(const std::vector<T>& dataVec, char* buffer, size_t bufLen) {
        return formatToBuffer(dataVec.data(), dataVec.size(), buffer, bufLen);
    }
    template <typename T>
    static typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), void>::type
    formatToBuffer(const T *data, size_t size, char* buffer, size_t bufLen) {
        const T *dataIn[1] = {data};
        size_t sizeIn[1] = {size};
        formatToBufferSeg<T>(1, dataIn, sizeIn, buffer, bufLen);
    }

    template <typename T>
    static typename std::enable_if<MULTI_VALUE_FORMAT_SUPPORTED(T), void>::type
    formatToBufferSeg(size_t segNum, const T *data[], size_t size[], char *buffer, size_t bufLen) {
        size_t totalSize = std::accumulate(size, size + segNum, 0);
        size_t countLen = encodeCount(totalSize, buffer, bufLen);
        if (countLen == 0 || totalSize == 0)
        {
            return;
        }
        assert(countLen + sizeof(T) * totalSize == bufLen);
        buffer += countLen;
        for (size_t seg = 0; seg < segNum; ++seg) {
            std::copy_n(data[seg], size[seg], (T *)(buffer));
            buffer += sizeof(T) * size[seg];
        }
    }

    static inline size_t calculateBufferLen(size_t count, size_t valueSize)
    {
        size_t totalSize = getEncodedCountLength(count);
        if (count < VAR_NUM_NULL_FIELD_VALUE_COUNT)
        {
            totalSize += valueSize * count;
        }
        return totalSize;
    }

    static inline size_t getEncodedCountLength(uint32_t count)
    {
        assert(count <= VAR_NUM_NULL_FIELD_VALUE_COUNT);
        if (count <= 0x0000003F)
        {
            return 1;
        }
        if (count <= 0x00003FFF)
        {
            return 2;
        }
        if (count <= 0x003FFFFF)
        {
            return 3;
        }
        return 4;
    }

    static inline size_t getEncodedCountFromFirstByte(uint8_t firstByte)
    {
        return (firstByte >> 6) + 1;
    }

    static inline size_t encodeCount(
            uint32_t count, char* buffer, size_t bufferSize)
    {
        size_t len = getEncodedCountLength(count);
        if (len > bufferSize)
        {
            return 0;
        }

        switch (len)
        {
        case 1:
        {
            ((uint8_t*)buffer)[0] = 0x00 | (uint8_t)count;
            break;
        }
        case 2:
        {
            ((uint8_t*)buffer)[0] = 0x40 | (uint8_t)((count >> 8) & 0xFF);
            ((uint8_t*)buffer)[1] = (uint8_t)(count & 0xFF);
            break;
        }
        case 3:
        {
            ((uint8_t*)buffer)[0] = 0x80 | (uint8_t)((count >> 16) & 0xFF);
            ((uint8_t*)buffer)[1] = (uint8_t)((count >> 8) & 0xFF);
            ((uint8_t*)buffer)[2] = (uint8_t)(count & 0xFF);
            break;
        }
        case 4:
        {
            ((uint8_t*)buffer)[0] = 0xC0 | (uint8_t)((count >> 24) & 0xFF);
            ((uint8_t*)buffer)[1] = (uint8_t)((count >> 16) & 0xFF);
            ((uint8_t*)buffer)[2] = (uint8_t)((count >> 8) & 0xFF);
            ((uint8_t*)buffer)[3] = (uint8_t)(count & 0xFF);
            break;
        }
        default:
            assert(false);
        }
        return len;
    }

    static inline uint32_t decodeCount(const char* start, size_t &countLen)
    {
        uint8_t *data = (uint8_t *)start;
        uint8_t b = *(data++);
        uint8_t mask = b >> 6;
        uint32_t value = b & 0x3F; if (mask == 0) goto done;
        value = (value << 8) | *(data++); if (mask == 1) goto done;
        value = (value << 8) | *(data++); if (mask == 2) goto done;
        value = (value << 8) | *(data++); goto done;
    done:
        countLen = mask + 1;
        return value;
    }

    static inline size_t getOffsetItemLength(uint32_t offset)
    {
        if (offset <= std::numeric_limits<uint8_t>::max())
        {
            return sizeof(uint8_t);
        }

        if (offset <= std::numeric_limits<uint16_t>::max())
        {
            return sizeof(uint16_t);
        }
        return sizeof(uint32_t);
    }

    static void setOffset(char *offsetBuf, size_t offsetLen, size_t offset) {
        switch (offsetLen)
        {
        case 1:
            *((uint8_t*)offsetBuf) = (uint8_t)offset;
            break;
        case 2:
            *((uint16_t*)offsetBuf) = (uint16_t)offset;
            break;
        case 4:
            *((uint32_t*)offsetBuf) = (uint32_t)offset;
            break;
        default:
            assert(false);
        }
    }

    static inline uint32_t getOffset(
            const char* offsetBaseAddr,
            size_t offsetItemLen, size_t idx)
    {
        switch (offsetItemLen)
        {
        case 1:
            return ((const uint8_t*)offsetBaseAddr)[idx];
        case 2:
            return ((const uint16_t*)offsetBaseAddr)[idx];
        case 4:
            return ((const uint32_t*)offsetBaseAddr)[idx];
        default:
            assert(false);
        }
        return std::numeric_limits<uint32_t>::max();
    }

    static size_t calculateMultiStringBufferLen(const std::vector<std::string> &data) {
        size_t offsetLen = 0;
        return calculateMultiStringBufferLen(data.data(), data.size(), offsetLen);
    }

    template <typename StringType>
    static typename std::enable_if<STRING_TYPE_VALUE_FORMAT_SUPPORTED(StringType), size_t>::type
    calculateMultiStringBufferLen(
            StringType data[],
            size_t dataCount,
            size_t &offsetLen) {
        size_t latestOffset = 0;
        size_t length = 0;

        for (size_t i = 0; i < dataCount; ++i) {
            size_t encodeHeaderLen =
                MultiValueFormatter::getEncodedCountLength(data[i].size());

            latestOffset = length;
            length += (encodeHeaderLen + data[i].size());
        }
        offsetLen = MultiValueFormatter::getOffsetItemLength(latestOffset);
        size_t bufLen = MultiValueFormatter::getEncodedCountLength(dataCount) +
            + sizeof(uint8_t) + offsetLen * dataCount + length;
        return bufLen;
    }

    template<typename StringType>
    static typename std::enable_if<STRING_TYPE_VALUE_FORMAT_SUPPORTED(StringType), void>::type
    formatMultiStringToBuffer(
            char *buffer, size_t bufLen, size_t offsetLen,
            const StringType data[], size_t tokenNum)
    {
        char* writeBuf = buffer;
        assert(buffer);

        // append recordNum
        size_t recordNumLen = MultiValueFormatter::encodeCount(tokenNum, writeBuf, bufLen);
        writeBuf += recordNumLen;
        // append offset length
        *(uint8_t*)writeBuf = offsetLen;
        writeBuf++;

        char* offsetBuf = writeBuf;
        char* dataBuf = (char*)(offsetBuf + offsetLen * tokenNum);
        uint32_t offset = 0;
        for (size_t i = 0; i < tokenNum; ++i) {
            size_t encodeLen = MultiValueFormatter::encodeCount(
                    data[i].size(), dataBuf, buffer + bufLen - dataBuf);
            dataBuf += encodeLen;
            memcpy(dataBuf, data[i].data(), data[i].size());
            dataBuf += data[i].size();

            MultiValueFormatter::setOffset(offsetBuf, offsetLen, offset);
            offsetBuf += offsetLen;
            offset += (encodeLen + data[i].size());
        }
        assert(size_t(dataBuf - buffer) == bufLen);
    }
};

}
