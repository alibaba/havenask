#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

const size_t VAR_NUM_ATTRIBUTE_SLICE_LEN = 64 * 1024 * 1024;
const size_t VAR_NUM_ATTRIBUTE_MAX_COUNT = 65535;
const size_t MULTI_VALUE_ATTRIBUTE_MAX_COUNT = 0x3FFFFFFFL;

class VarNumAttributeFormatter
{
public:
    VarNumAttributeFormatter() {}
    ~VarNumAttributeFormatter() {}

public:
    static inline size_t GetEncodedCountLength(uint32_t count)
    {
        assert(count <= 0x3FFFFFFF);
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

    static inline size_t GetEncodedCountFromFirstByte(uint8_t firstByte)
    {
        return (firstByte >> 6) + 1;
    }

    static inline size_t EncodeCount(
            uint32_t count, char* buffer, size_t bufferSize)
    {
        size_t len = GetEncodedCountLength(count);
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
    
    static inline uint32_t DecodeCount(const char* buffer, size_t &countLen)
    {
        uint32_t value = 0;
        uint8_t firstByte = ((uint8_t*)buffer)[0];
        uint8_t lenMask = firstByte >> 6;
        uint8_t firstByteValue = firstByte & 0x3F;
        switch (lenMask)
        {
        case 0:
        {
            value = firstByteValue;
            countLen = 1;
            break;
        }
        case 1:
        {
            uint32_t va1 = firstByteValue;
            uint32_t va2 = ((uint8_t*)buffer)[1];
            value = (va1 << 8) | va2;
            countLen = 2;
            break;
        }
        case 2:
        {
            uint32_t va1 = firstByteValue;
            uint32_t va2 = ((uint8_t*)buffer)[1];
            uint32_t va3 = ((uint8_t*)buffer)[2];
            value = (va1 << 16) | (va2 << 8) | va3;
            countLen = 3;
            break;
        }
        case 3:
        {
            uint32_t va1 = firstByteValue;
            uint32_t va2 = ((uint8_t*)buffer)[1];
            uint32_t va3 = ((uint8_t*)buffer)[2];
            uint32_t va4 = ((uint8_t*)buffer)[3];
            value = (va1 << 24) | (va2 << 16) | (va3 << 8) | va4;
            countLen = 4;
            break;
        }
        default:
            assert(false);
        }
        return value;
    }

    static inline size_t GetOffsetItemLength(uint32_t offset)
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

    static inline uint32_t GetOffset(
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

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeFormatter);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_FORMATTER_H
