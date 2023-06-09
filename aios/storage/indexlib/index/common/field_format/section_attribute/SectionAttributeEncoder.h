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

#include <cassert>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/field_format/section_attribute/SectionMeta.h"

namespace indexlib::index {

class SectionAttributeEncoder : private autil::NoCopyable
{
public:
    SectionAttributeEncoder() = default;
    ~SectionAttributeEncoder() = default;

public:
    static std::pair<Status, uint32_t> Encode(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                                              uint32_t sectionCount, uint8_t* buf, uint32_t bufLen);

    static Status Decode(const uint8_t* srcBuf, uint32_t srcBufLen, uint8_t* destBuf, uint32_t destBufLen);

    static Status EncodeSectionWeights(const section_weight_t* srcWeights, uint32_t sectionCount, uint8_t*& cursor,
                                       uint8_t* bufEnd);

    static Status EncodeFieldIds(const section_fid_t* srcFids, uint32_t sectionCount, uint8_t*& cursor,
                                 uint8_t* bufEnd);
    static Status EncodeSectionLens(const section_len_t* srcLengths, uint32_t sectionCount, uint8_t*& cursor,
                                    uint8_t* bufEnd);
    static Status EncodeWeightUnit(const section_weight_t* weightUnit, uint8_t*& cursor, uint8_t* bufEnd);

    static Status DecodeSectionLens(const uint8_t*& srcCursor, uint8_t*& destCursor, uint8_t* destEnd);

    // decode process:
    // current fieldId = last fieldId + delta
    static Status DecodeSectionFids(const uint8_t*& srcCursor, uint32_t sectionCount, uint8_t*& destCursor,
                                    uint8_t* destEnd);

    static Status DecodeSectionWeights(const uint8_t*& srcCursor, const uint8_t* srcEnd, uint32_t sectionCount,
                                       uint8_t*& destCursor, uint8_t* destEnd);

private:
    template <typename T>
    static Status CheckBufferLen(T* dest, T* destEnd, uint32_t k);

    static section_weight_t GetWeight(const uint8_t* srcCursor, uint8_t weightLen);

private:
    static const uint8_t _itemNumPerUnit[16];

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
inline Status SectionAttributeEncoder::CheckBufferLen(T* dest, T* destEnd, uint32_t k)
{
    if (dest + k > destEnd) {
        AUTIL_LOG(ERROR, "buffer over flow, decode buffer is too short");
        return Status::Corruption("buffer over flow, decode buffer is too short");
    }
    return Status::OK();
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
} // namespace indexlib::index
