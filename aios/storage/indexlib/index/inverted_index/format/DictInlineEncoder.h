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

#include "indexlib/base/Types.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {

// dict inline value = | compress mode (4) | inline posting value |
// InlinePostingValueWithOrder = | dfFirst | inline posting value |
using InlinePostingValueWithOrder = std::pair<bool, uint64_t>;

class DictInlineEncoder
{
public:
    DictInlineEncoder() = default;
    ~DictInlineEncoder() = default;

    static bool Encode(const std::vector<uint32_t>& data, uint64_t& result);
    static bool EncodeContinuousDocId(docid_t startDocid, df_t df, bool enableDictInlineLongDF,
                                      InlinePostingValueWithOrder& inlinePostingValueWithOrder);

private:
    static uint32_t CalculateCompressedSize(const std::vector<uint32_t>& data);

    static void CompressData(uint8_t*& cursor, uint32_t& remainBufferLen, uint32_t value);

    friend class DictInlineEncoderTest;
};

////////////////////////////////////////////////////
inline bool DictInlineEncoder::Encode(const std::vector<uint32_t>& data, uint64_t& result)
{
    uint32_t calculateSize = 0;
    calculateSize = CalculateCompressedSize(data);

    if (calculateSize > MAX_DICT_INLINE_AVAILABLE_SIZE) {
        return false;
    }

    uint64_t buffer = 0;
    uint8_t* cursor = (uint8_t*)&buffer;
    uint32_t leftBufferLen = sizeof(uint64_t);

    for (size_t i = 0; i < data.size(); i++) {
        CompressData(cursor, leftBufferLen, data[i]);
    }

    result = buffer;

#if __BYTE_ORDER == __BIG_ENDIAN
    result = result >> BIG_ENDIAN_DICT_INLINE_SHIFT_BITNUM;
#endif
    return true;
}

inline bool DictInlineEncoder::EncodeContinuousDocId(docid_t startDocid, df_t df, bool enableDictInlineLongDF,
                                                     InlinePostingValueWithOrder& inlinePostingValueWithOrder)
{
    const int32_t THRESHOLD = 0xFFFFFFF; // 2 ^ 28 - 1
    bool dfFirst = true;
    if (df > THRESHOLD) {
        if (!enableDictInlineLongDF || startDocid > THRESHOLD) {
            return false;
        }
        dfFirst = false;
    }
    uint64_t v1 = (uint64_t)startDocid;
    uint64_t v2 = (uint64_t)df;
    if (!dfFirst) {
        std::swap(v1, v2);
    }
    v2 = v2 << 32;
    inlinePostingValueWithOrder = {dfFirst, (v1 | v2)};
    return true;
}

inline void DictInlineEncoder::CompressData(uint8_t*& cursor, uint32_t& remainBufferLen, uint32_t value)
{
    auto [status, compressSize] = VByteCompressor::EncodeVInt32(cursor, remainBufferLen, value);
    THROW_IF_STATUS_ERROR(status);
    cursor += compressSize;
    remainBufferLen -= compressSize;
}

inline uint32_t DictInlineEncoder::CalculateCompressedSize(const std::vector<uint32_t>& data)
{
    uint32_t compressSize = 0;
    for (size_t i = 0; i < data.size(); i++) {
        compressSize += VByteCompressor::GetVInt32Length(data[i]);
    }
    return compressSize;
}

} // namespace indexlib::index
