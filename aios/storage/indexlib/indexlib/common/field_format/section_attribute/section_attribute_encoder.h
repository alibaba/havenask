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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib::common {

class SectionAttributeEncoder
{
public:
    static uint32_t Encode(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                           uint32_t sectionCount, uint8_t* buf, uint32_t bufLen);

    static void Decode(const uint8_t* srcBuf, uint32_t srcBufLen, uint8_t* destBuf, uint32_t destBufLen);

    static void EncodeSectionWeights(const section_weight_t* srcWeights, uint32_t sectionCount, uint8_t*& cursor,
                                     uint8_t* bufEnd);

    static void EncodeFieldIds(const section_fid_t* srcFids, uint32_t sectionCount, uint8_t*& cursor, uint8_t* bufEnd);
    static void EncodeSectionLens(const section_len_t* srcLengths, uint32_t sectionCount, uint8_t*& cursor,
                                  uint8_t* bufEnd);
    static void EncodeWeightUnit(const section_weight_t* weightUnit, uint8_t*& cursor, uint8_t* bufEnd);

    static void DecodeSectionLens(const uint8_t*& srcCursor, uint8_t*& destCursor, uint8_t* destEnd);

    // decode process:
    // current fieldId = last fieldId + delta
    static void DecodeSectionFids(const uint8_t*& srcCursor, uint32_t sectionCount, uint8_t*& destCursor,
                                  uint8_t* destEnd);

    static void DecodeSectionWeights(const uint8_t*& srcCursor, const uint8_t* srcEnd, uint32_t sectionCount,
                                     uint8_t*& destCursor, uint8_t* destEnd);

private:
    template <typename T>
    static void CheckBufferLen(T* dest, T* destEnd, uint32_t k);

    static section_weight_t GetWeight(const uint8_t* srcCursor, uint8_t weightLen);

private:
    static const uint8_t itemNumPerUnit[16];

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline void SectionAttributeEncoder::CheckBufferLen(T* dest, T* destEnd, uint32_t k)
{
    if (dest + k > destEnd) {
        INDEXLIB_FATAL_ERROR(BufferOverflow, "decode buffer is too short");
    }
}

inline section_weight_t SectionAttributeEncoder::GetWeight(const uint8_t* srcCursor, uint8_t weightLen)
{
    if (weightLen == 0) {
        return 0;
    }
    if (weightLen == 1) {
        return *srcCursor;
    }
    assert(weightLen == 2);
    return *((uint16_t*)srcCursor);
}
} // namespace indexlib::common
