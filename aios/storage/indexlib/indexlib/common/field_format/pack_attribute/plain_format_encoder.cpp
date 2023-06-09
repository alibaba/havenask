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
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"

#include "indexlib/util/ByteSimilarityHasher.h"

namespace indexlib::common {
AUTIL_LOG_SETUP(indexlib.common, PlainFormatEncoder);

PlainFormatEncoder::PlainFormatEncoder(uint32_t fixAttrSize, const std::vector<AttributeReferencePtr>& fixLenAttributes,
                                       const std::vector<AttributeReferencePtr>& varLenAttributes)
    : mFixAttrSize(fixAttrSize)
    , mFixLenAttributes(fixLenAttributes)
    , mVarLenAttributes(varLenAttributes)
{
    assert(!mVarLenAttributes.empty());
}

PlainFormatEncoder::~PlainFormatEncoder() {}

bool PlainFormatEncoder::Encode(const autil::StringView& encodePackValue, char* buffer, size_t bufferSize,
                                autil::StringView& plainDataValue)
{
    plainDataValue = InnerEncode(encodePackValue, buffer, bufferSize);
    return !plainDataValue.empty();
}

/*
autil::StringView PlainFormatEncoder::Encode(const autil::StringView& encodePackValue,
                                             autil::mem_pool::Pool* pool) const
{
    uint32_t varLenOffsetLen = mVarLenAttributes.size() * sizeof(uint64_t);
    size_t bufferSize = encodePackValue.size() - varLenOffsetLen;
    char* buffer = (char*)pool->allocate(bufferSize);
    return InnerEncode(encodePackValue, buffer, bufferSize);
}
*/

// pack data with encoded count
autil::StringView PlainFormatEncoder::InnerEncode(const autil::StringView& encodePackValue, char* buffer,
                                                  size_t bufferSize) const
{
    size_t countLen = 0;
    uint32_t size = autil::MultiValueFormatter::decodeCount(encodePackValue.data(), countLen);
    const char* data = encodePackValue.data() + countLen;

    uint32_t varLenOffsetLen = mVarLenAttributes.size() * sizeof(uint64_t);
    uint32_t tightSize = size - varLenOffsetLen;
    uint32_t totalSize = tightSize + autil::MultiValueFormatter::getEncodedCountLength(tightSize);

    if (bufferSize < totalSize) {
        AUTIL_LOG(ERROR, "buffer size [%lu] is not big enough, totalSize [%u]", bufferSize, totalSize);
        return autil::StringView::empty_instance();
    }

    char* cursor = buffer;
    countLen = autil::MultiValueFormatter::encodeCount(tightSize, cursor, bufferSize);
    cursor += countLen;

    memcpy(cursor, data, mFixAttrSize);
    cursor += mFixAttrSize;
    const char* varLenData = data + mFixAttrSize + varLenOffsetLen;
    memcpy(cursor, varLenData, size - mFixAttrSize - varLenOffsetLen);
    return autil::StringView(buffer, totalSize);
}

bool PlainFormatEncoder::Decode(const autil::StringView& plainValue, autil::mem_pool::Pool* pool,
                                autil::StringView& packValue) const
{
    if (!pool) {
        AUTIL_LOG(WARN, "no pool for decode interface.");
        return false;
    }

    assert(!mVarLenAttributes.empty());
    size_t offsetDataLen = mVarLenAttributes.size() * sizeof(uint64_t);
    size_t targetLen = plainValue.size() + offsetDataLen;
    char* buffer = (char*)pool->allocate(targetLen);
    const char* baseAddr = plainValue.data();
    memcpy(buffer, baseAddr, mFixAttrSize);
    memcpy(buffer + mFixAttrSize + offsetDataLen, baseAddr + mFixAttrSize, plainValue.size() - mFixAttrSize);

    const char* data = baseAddr + mFixAttrSize;
    size_t offset = mFixAttrSize + offsetDataLen;
    for (size_t i = 0; i < mVarLenAttributes.size(); i++) {
        autil::PackDataFormatter::setVarLenOffset(mVarLenAttributes[i]->GetOffset(), buffer, offset);
        size_t fieldLen = mVarLenAttributes[i]->CalculateDataLength(data);
        offset += fieldLen;
        data += fieldLen;
    }

    if (targetLen != offset) {
        AUTIL_LOG(ERROR, "decode plain format encoder fail, wrong target length [%lu], expected [%lu]", targetLen,
                  offset);
        return false;
    }
    packValue = autil::StringView(buffer, targetLen);
    return true;
}

bool PlainFormatEncoder::DecodeDataValues(const autil::StringView& plainValue,
                                          std::vector<AttributeFieldData>& dataValues)
{
    dataValues.reserve(mFixLenAttributes.size() + mVarLenAttributes.size());
    for (auto& fixLenRef : mFixLenAttributes) {
        auto dataValue = fixLenRef->GetDataValue(plainValue.data());
        dataValues.emplace_back(std::make_pair(dataValue.valueStr, -1));
    }

    const char* data = plainValue.data() + mFixAttrSize;
    for (size_t i = 0; i < mVarLenAttributes.size(); i++) {
        size_t fieldLen = mVarLenAttributes[i]->CalculateDataLength(data);
        dataValues.emplace_back(std::make_pair(autil::StringView(data, fieldLen), -1));
        data += fieldLen;
    }
    return (data - plainValue.data()) == plainValue.size();
}

autil::StringView PlainFormatEncoder::EncodeDataValues(const std::vector<AttributeFieldData>& dataValues,
                                                       autil::mem_pool::Pool* pool)
{
    if (!pool) {
        return autil::StringView::empty_instance();
    }

    if (dataValues.size() != (mFixLenAttributes.size() + mVarLenAttributes.size())) {
        AUTIL_LOG(ERROR, "value number mismatch [%lu], expect [%lu]", dataValues.size(),
                  mFixLenAttributes.size() + mVarLenAttributes.size());
        return autil::StringView::empty_instance();
    }

    size_t bufferSize = 0;
    for (auto& value : dataValues) {
        bufferSize += value.first.size();
    }

    char* buffer = (char*)pool->allocate(bufferSize);
    char* cursor = buffer;
    for (auto& value : dataValues) {
        memcpy(cursor, value.first.data(), value.first.size());
        cursor += value.first.size();
    }
    return autil::StringView(buffer, cursor);
}

uint32_t PlainFormatEncoder::GetEncodeSimHash(const autil::StringView& encodePackValue) const
{
    uint32_t varLenOffsetLen = mVarLenAttributes.size() * sizeof(uint64_t);
    size_t bufferSize = encodePackValue.size() - varLenOffsetLen;

    char buffer[bufferSize];
    autil::StringView ret = InnerEncode(encodePackValue, buffer, bufferSize);
    return util::ByteSimilarityHasher::GetSimHash(ret.data(), ret.size());
}

} // namespace indexlib::common
