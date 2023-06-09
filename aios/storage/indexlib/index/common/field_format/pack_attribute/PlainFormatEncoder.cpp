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
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"

#include "indexlib/util/ByteSimilarityHasher.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PlainFormatEncoder);

PlainFormatEncoder::PlainFormatEncoder(uint32_t fixAttrSize,
                                       const std::vector<std::shared_ptr<AttributeReference>>& fixLenAttributes,
                                       const std::vector<std::shared_ptr<AttributeReference>>& varLenAttributes)
    : _fixAttrSize(fixAttrSize)
    , _fixLenAttributes(fixLenAttributes)
    , _varLenAttributes(varLenAttributes)
{
    assert(!_varLenAttributes.empty());
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
    uint32_t varLenOffsetLen = _varLenAttributes.size() * sizeof(uint64_t);
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

    uint32_t varLenOffsetLen = _varLenAttributes.size() * sizeof(uint64_t);
    uint32_t tightSize = size - varLenOffsetLen;
    uint32_t totalSize = tightSize + autil::MultiValueFormatter::getEncodedCountLength(tightSize);

    if (bufferSize < totalSize) {
        AUTIL_LOG(ERROR, "buffer size [%lu] is not big enough, totalSize [%u]", bufferSize, totalSize);
        return autil::StringView::empty_instance();
    }

    char* cursor = buffer;
    countLen = autil::MultiValueFormatter::encodeCount(tightSize, cursor, bufferSize);
    cursor += countLen;

    memcpy(cursor, data, _fixAttrSize);
    cursor += _fixAttrSize;
    const char* varLenData = data + _fixAttrSize + varLenOffsetLen;
    memcpy(cursor, varLenData, size - _fixAttrSize - varLenOffsetLen);
    return autil::StringView(buffer, totalSize);
}

bool PlainFormatEncoder::Decode(const autil::StringView& plainValue, autil::mem_pool::Pool* pool,
                                autil::StringView& packValue) const
{
    if (!pool) {
        AUTIL_LOG(WARN, "no pool for decode interface.");
        return false;
    }

    assert(!_varLenAttributes.empty());
    size_t offsetDataLen = _varLenAttributes.size() * sizeof(uint64_t);
    size_t targetLen = plainValue.size() + offsetDataLen;
    char* buffer = (char*)pool->allocate(targetLen);
    const char* baseAddr = plainValue.data();
    memcpy(buffer, baseAddr, _fixAttrSize);
    memcpy(buffer + _fixAttrSize + offsetDataLen, baseAddr + _fixAttrSize, plainValue.size() - _fixAttrSize);

    const char* data = baseAddr + _fixAttrSize;
    size_t offset = _fixAttrSize + offsetDataLen;
    for (size_t i = 0; i < _varLenAttributes.size(); i++) {
        autil::PackDataFormatter::setVarLenOffset(_varLenAttributes[i]->GetOffset(), buffer, offset);
        size_t fieldLen = _varLenAttributes[i]->CalculateDataLength(data);
        offset += fieldLen;
        data += fieldLen;
    }

    if (targetLen != offset) {
        AUTIL_LOG(ERROR, "decode plain format encoder fail, wrong target length [%lu], expected [%lu]", targetLen,
                  offset + _fixAttrSize);
        return false;
    }
    packValue = autil::StringView(buffer, targetLen);
    return true;
}

bool PlainFormatEncoder::DecodeDataValues(const autil::StringView& plainValue,
                                          std::vector<AttributeFieldData>& dataValues)
{
    dataValues.reserve(_fixLenAttributes.size() + _varLenAttributes.size());
    for (auto& fixLenRef : _fixLenAttributes) {
        auto dataValue = fixLenRef->GetDataValue(plainValue.data());
        dataValues.emplace_back(std::make_pair(dataValue.valueStr, -1));
    }

    const char* data = plainValue.data() + _fixAttrSize;
    for (size_t i = 0; i < _varLenAttributes.size(); i++) {
        size_t fieldLen = _varLenAttributes[i]->CalculateDataLength(data);
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

    if (dataValues.size() != (_fixLenAttributes.size() + _varLenAttributes.size())) {
        AUTIL_LOG(ERROR, "value number mismatch [%lu], expect [%lu]", dataValues.size(),
                  _fixLenAttributes.size() + _varLenAttributes.size());
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
    uint32_t varLenOffsetLen = _varLenAttributes.size() * sizeof(uint64_t);
    size_t bufferSize = encodePackValue.size() - varLenOffsetLen;

    char buffer[bufferSize];
    autil::StringView ret = InnerEncode(encodePackValue, buffer, bufferSize);
    return indexlib::util::ByteSimilarityHasher::GetSimHash(ret.data(), ret.size());
}

} // namespace indexlibv2::index
