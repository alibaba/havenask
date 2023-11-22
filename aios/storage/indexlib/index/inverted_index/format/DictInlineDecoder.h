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

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
// dict inline value = | compress mode (4) | inline posting value |
// InlinePostingValueWithOrder = | dfFirst | inline posting value |
using InlinePostingValueWithOrder = std::pair<bool, uint64_t>;
class DictInlineDecoder
{
public:
    DictInlineDecoder() = default;
    ~DictInlineDecoder() = default;

    static void Decode(uint64_t compressValue, uint32_t count, uint32_t* dataBuf);
    static void DecodeContinuousDocId(InlinePostingValueWithOrder inlinePostingValueWithOrder, docid32_t& startDocid,
                                      df_t& df);
    static void FastMemSet(docid32_t* dest, size_t destLen, docid32_t value);
};

///////////////////////////////////////////////////////////////
inline void DictInlineDecoder::Decode(uint64_t compressValue, uint32_t count, uint32_t* dataBuf)
{
    uint64_t buffer = compressValue;
#if __BYTE_ORDER == __BIG_ENDIAN
    buffer = compressValue << BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM;
#endif
    uint8_t* cursor = (uint8_t*)&buffer;
    uint32_t leftBufferLen = MAX_DICT_INLINE_AVAILABLE_SIZE;
    for (uint32_t i = 0; i < count; i++) {
        auto [status, val] = VByteCompressor::DecodeVInt32(cursor, leftBufferLen);
        THROW_IF_STATUS_ERROR(status);
        dataBuf[i] = (uint32_t)val;
    }
}

inline void DictInlineDecoder::DecodeContinuousDocId(InlinePostingValueWithOrder inlinePostingValueWithOrder,
                                                     docid32_t& startDocid, df_t& df)
{
    auto& [dfFirst, inlinePostingValue] = inlinePostingValueWithOrder;
    uint64_t value = ShortListOptimizeUtil::GetDictInlineValue(inlinePostingValue);
    df = (df_t)(value >> 32);
    value = value << 32;
    startDocid = (docid32_t)(value >> 32);
    if (!dfFirst) {
        std::swap(startDocid, df);
    }
}

inline void DictInlineDecoder::FastMemSet(docid32_t* dest, size_t destLen, docid32_t value)
{
    uint32_t maxBlocks = destLen >> 4;
    uint32_t rest = destLen % 16;
    for (uint32_t blk = 0; blk < maxBlocks; blk++, dest += 16) {
        dest[0] = value;
        dest[1] = value;
        dest[2] = value;
        dest[3] = value;
        dest[4] = value;
        dest[5] = value;
        dest[6] = value;
        dest[7] = value;
        dest[8] = value;
        dest[9] = value;
        dest[10] = value;
        dest[11] = value;
        dest[12] = value;
        dest[13] = value;
        dest[14] = value;
        dest[15] = value;
    }
    for (uint32_t i = 0; i < rest; ++i) {
        dest[i] = value;
    }
}

} // namespace indexlib::index
