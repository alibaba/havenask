#ifndef __INDEXLIB_DICT_INLINE_ENCODER_H
#define __INDEXLIB_DICT_INLINE_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineEncoder
{
public:
    DictInlineEncoder() {}
    ~DictInlineEncoder() {}

public:
    static bool Encode(const std::vector<uint32_t>& data, uint64_t& result);

private:
    static uint32_t CalculateCompressedSize(const std::vector<uint32_t>& data);

    static void CompressData(uint8_t*& cursor, uint32_t& remainBufferLen, uint32_t value);

private:
    friend class DictInlineEncoderTest;
};

DEFINE_SHARED_PTR(DictInlineEncoder);

////////////////////////////////////////////////////
inline bool DictInlineEncoder::Encode(
        const std::vector<uint32_t>& data, uint64_t& result)
{
    uint32_t calculateSize = 0;
    calculateSize = CalculateCompressedSize(data);
    
    if (calculateSize > MAX_DICT_INLINE_AVAILABLE_SIZE)
    {
        return false;
    }

    uint64_t buffer = 0;
    uint8_t *cursor = (uint8_t*)&buffer;
    uint32_t leftBufferLen = sizeof(uint64_t);

    for (size_t i = 0; i < data.size(); i++)
    {
        CompressData(cursor, leftBufferLen, data[i]);
    }

    result = buffer;

#if __BYTE_ORDER == __BIG_ENDIAN
    result = result >> BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM;
#endif
    return true;
}

inline void DictInlineEncoder::CompressData(uint8_t*& cursor,
        uint32_t& remainBufferLen, uint32_t value)
{
    uint32_t compressSize = common::VByteCompressor::EncodeVInt32(
            cursor, remainBufferLen, value);
    cursor += compressSize;
    remainBufferLen -= compressSize;
}

inline uint32_t DictInlineEncoder::CalculateCompressedSize(
        const std::vector<uint32_t>& data)
{
    uint32_t compressSize = 0;
    for (size_t i = 0; i < data.size(); i++)
    {
        compressSize += common::VByteCompressor::GetVInt32Length(data[i]);
    }
    return compressSize;
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICT_INLINE_ENCODER_H
