#ifndef __INDEXLIB_DICT_INLINE_DECODER_H
#define __INDEXLIB_DICT_INLINE_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

IE_NAMESPACE_BEGIN(index);

class DictInlineDecoder
{
public:
    DictInlineDecoder()
    {}

    ~DictInlineDecoder() {}

public:
    static void Decode(uint64_t compressValue, uint32_t count, uint32_t* dataBuf);
};

DEFINE_SHARED_PTR(DictInlineDecoder);

///////////////////////////////////////////////////////////////
inline void DictInlineDecoder::Decode(
        uint64_t compressValue, uint32_t count, uint32_t* dataBuf)
{
    uint64_t buffer = compressValue;
#if __BYTE_ORDER == __BIG_ENDIAN
    buffer = compressValue << BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM;
#endif
    uint8_t *cursor = (uint8_t*)&buffer;
    uint32_t leftBufferLen = MAX_DICT_INLINE_AVAILABLE_SIZE;
    for (uint32_t i = 0; i < count; i++)
    {
        dataBuf[i] = (uint32_t)common::VByteCompressor::DecodeVInt32(cursor, leftBufferLen);
    }
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICT_INLINE_DECODER_H
