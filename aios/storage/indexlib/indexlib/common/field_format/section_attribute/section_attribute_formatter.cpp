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
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"

#include "indexlib/common/field_format/section_attribute/section_attribute_encoder.h"
#include "indexlib/util/NumericUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, SectionAttributeFormatter);

SectionAttributeFormatter::SectionAttributeFormatter(const SectionAttributeConfigPtr& sectionAttrConfig)
    : mSectionAttrConfig(sectionAttrConfig)
{
}

SectionAttributeFormatter::~SectionAttributeFormatter() {}

string SectionAttributeFormatter::Encode(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                                         uint32_t sectionCount)
{
    uint8_t buf[DATA_SLICE_LEN];
    uint32_t encodedSize = EncodeToBuffer(lengths, fids, weights, sectionCount, buf);
    return string((char*)buf, encodedSize);
}

uint32_t SectionAttributeFormatter::EncodeToBuffer(section_len_t* lengths, section_fid_t* fids,
                                                   section_weight_t* weights, uint32_t sectionCount, uint8_t* buf)
{
    uint8_t* cursor = buf;
    uint8_t* bufEnd = buf + DATA_SLICE_LEN;

    SectionAttributeEncoder::EncodeSectionLens(lengths, sectionCount, cursor, bufEnd);
    if (mSectionAttrConfig->HasFieldId()) {
        SectionAttributeEncoder::EncodeFieldIds(fids, sectionCount, cursor, bufEnd);
    }

    if (mSectionAttrConfig->HasSectionWeight()) {
        SectionAttributeEncoder::EncodeSectionWeights(weights, sectionCount, cursor, bufEnd);
    }

    uint32_t encodedLen = cursor - buf;
    uint32_t packedLen = NumericUtil::UpperPack(encodedLen, 4);
    memset(buf + encodedLen, 0, packedLen - encodedLen);
    return packedLen;
}

uint32_t SectionAttributeFormatter::UnpackBuffer(const uint8_t* buffer, bool hasFieldId, bool hasSectionWeight,
                                                 section_len_t*& lengthBuf, section_fid_t*& fidBuf,
                                                 section_weight_t*& weightBuf)
{
    const uint8_t* cursor = buffer;
    uint32_t sectionCount = *((section_len_t*)cursor);
    cursor += sizeof(section_len_t);
    lengthBuf = (section_len_t*)cursor;
    cursor += sizeof(section_len_t) * sectionCount;

    if (hasFieldId) {
        fidBuf = (section_fid_t*)cursor;
        cursor += sizeof(section_fid_t) * sectionCount;
    } else {
        fidBuf = NULL;
    }

    if (hasSectionWeight) {
        weightBuf = (section_weight_t*)cursor;
    } else {
        weightBuf = NULL;
    }
    return sectionCount;
}
}} // namespace indexlib::common
