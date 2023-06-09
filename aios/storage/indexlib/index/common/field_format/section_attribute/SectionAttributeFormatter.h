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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/common/field_format/section_attribute/SectionAttributeEncoder.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"

namespace indexlib::index {

class SectionAttributeFormatter : private autil::NoCopyable
{
public:
    SectionAttributeFormatter(const std::shared_ptr<indexlibv2::config::SectionAttributeConfig>& sectionAttrConfig);
    ~SectionAttributeFormatter();

public:
    std::pair<Status, std::string> Encode(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                                          uint32_t sectionCount);

    std::pair<Status, uint32_t> EncodeToBuffer(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                                               uint32_t sectionCount, uint8_t* buf);

    Status Decode(const char* dataBuf, uint32_t dataLen, uint8_t* buffer, uint32_t bufLength);
    Status Decode(const std::string& value, uint8_t* buffer, uint32_t bufLength);

    static uint32_t UnpackBuffer(const uint8_t* buffer, bool hasFieldId, bool hasSectionWeight,
                                 section_len_t*& lengthBuf, section_fid_t*& fidBuf, section_weight_t*& weightBuf);

public:
    static const uint32_t DATA_SLICE_LEN = 16 * 1024;

private:
    std::shared_ptr<indexlibv2::config::SectionAttributeConfig> _sectionAttrConfig;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////

inline Status SectionAttributeFormatter::Decode(const char* dataBuf, uint32_t dataLen, uint8_t* buffer,
                                                uint32_t bufLength)
{
    const uint8_t* srcCursor = (const uint8_t*)dataBuf;
    uint8_t* destCursor = buffer;
    uint8_t* destEnd = buffer + bufLength;
    auto status = SectionAttributeEncoder::DecodeSectionLens(srcCursor, destCursor, destEnd);
    RETURN_IF_STATUS_ERROR(status, "decode section length fail. error [%s].", status.ToString().c_str());

    uint32_t sectionCount = *((section_len_t*)buffer);
    if (_sectionAttrConfig->HasFieldId()) {
        status = SectionAttributeEncoder::DecodeSectionFids(srcCursor, sectionCount, destCursor, destEnd);
        RETURN_IF_STATUS_ERROR(status, "decode section fids fail. error [%s].", status.ToString().c_str());
    }

    if (_sectionAttrConfig->HasSectionWeight()) {
        const uint8_t* srcEnd = (const uint8_t*)dataBuf + dataLen;
        auto status =
            SectionAttributeEncoder::DecodeSectionWeights(srcCursor, srcEnd, sectionCount, destCursor, destEnd);
        RETURN_IF_STATUS_ERROR(status, "decode section weights fail. error [%s].", status.ToString().c_str());
    }
    return Status::OK();
}

inline Status SectionAttributeFormatter::Decode(const std::string& value, uint8_t* buffer, uint32_t bufLength)
{
    return Decode((const char*)value.data(), (uint32_t)value.size(), buffer, bufLength);
}
} // namespace indexlib::index
