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

#include "autil/DataBuffer.h"
#include "autil/StringTokenizer.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class MultiStringAttributeConvertor : public VarNumAttributeConvertor<char>
{
public:
    MultiStringAttributeConvertor(bool needHash = false, const std::string& fieldName = "", bool isBinary = false,
                                  const std::string& separator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
        : VarNumAttributeConvertor<char>(needHash, fieldName, -1, separator)
        , mIsBinary(isBinary)
    {
    }
    ~MultiStringAttributeConvertor() {}

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

    bool DecodeLiteralField(const autil::StringView& str, std::string& value) override;

private:
    void SetOffset(char* offsetBuf, size_t offsetLen, size_t offset);

    void AppendOffsetAndData(const std::vector<autil::StringView>& vec, uint8_t offsetLen, char*& writeBuf);

    void AdjustStringVector(std::vector<autil::StringView>& vec, uint32_t& dataLen, uint32_t& latestOffset);

private:
    bool mIsBinary;
};

//////////////////////////////////////////////////////////////////

inline autil::StringView MultiStringAttributeConvertor::InnerEncode(const autil::StringView& attrData,
                                                                    autil::mem_pool::Pool* memPool,
                                                                    std::string& resultStr, char* outBuffer,
                                                                    EncodeStatus& status)
{
    if (attrData.empty()) {
        uint32_t tokenNum = 0;
        size_t bufSize = sizeof(uint64_t) + VarNumAttributeFormatter::GetEncodedCountLength(0);
        char* begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);

        char* buffer = begin;
        AppendHashAndCount(attrData, tokenNum, buffer);
        return autil::StringView(begin, bufSize);
    }

    std::vector<autil::StringView> vec;
    if (mIsBinary) {
        autil::DataBuffer dataBuffer(const_cast<void*>((const void*)attrData.data()), attrData.size());
        uint32_t len = 0;
        dataBuffer.read(len);
        vec.reserve(len);
        for (uint32_t i = 0; i < len; ++i) {
            uint32_t sLen = 0;
            dataBuffer.read(sLen);
            const void* data = dataBuffer.readNoCopy(sLen);
            vec.push_back(autil::StringView((const char*)data, sLen));
        }
    } else {
        vec = autil::StringTokenizer::constTokenize(attrData, mSeparator, autil::StringTokenizer::TOKEN_LEAVE_AS);
    }

    uint32_t length = 0;
    uint32_t latestOffset = 0;
    AdjustStringVector(vec, length, latestOffset);

    size_t offsetLen = VarNumAttributeFormatter::GetOffsetItemLength(latestOffset);
    size_t bufSize = sizeof(uint64_t) + VarNumAttributeFormatter::GetEncodedCountLength(vec.size()) + +sizeof(uint8_t) +
                     offsetLen * vec.size() + length;

    char* begin = (char*)allocate(memPool, resultStr, outBuffer, bufSize);
    char* writeBuf = begin;
    AppendHashAndCount(attrData, vec.size(), writeBuf);
    AppendOffsetAndData(vec, offsetLen, writeBuf);
    assert((size_t)(writeBuf - begin) == bufSize);
    return autil::StringView(begin, writeBuf - begin);
}

inline void MultiStringAttributeConvertor::SetOffset(char* offsetBuf, size_t offsetLen, size_t offset)
{
    switch (offsetLen) {
    case 1:
        *((uint8_t*)offsetBuf) = (uint8_t)offset;
        break;
    case 2:
        *((uint16_t*)offsetBuf) = (uint16_t)offset;
        break;
    case 4:
        *((uint32_t*)offsetBuf) = (uint32_t)offset;
        break;
    default:
        assert(false);
    }
}

inline void MultiStringAttributeConvertor::AdjustStringVector(std::vector<autil::StringView>& vec, uint32_t& dataLen,
                                                              uint32_t& latestOffset)
{
    uint32_t tokenNums = AdjustCount(vec.size());
    assert(vec.size() >= tokenNums);
    vec.resize(tokenNums);

    uint32_t maxHeaderLen =
        VarNumAttributeFormatter::GetEncodedCountLength(vec.size()) + sizeof(uint8_t) + sizeof(uint32_t) * vec.size();
    uint32_t MAX_OFFSET = VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_MAX_COUNT - maxHeaderLen;

    dataLen = 0;
    latestOffset = 0;
    // cut off logic
    for (size_t i = 0; i < vec.size(); ++i) {
        uint32_t encodeHeaderLen = VarNumAttributeFormatter::GetEncodedCountLength(vec[i].size());
        if (dataLen + encodeHeaderLen + vec[i].size() >= MAX_OFFSET) {
            if (MAX_OFFSET - dataLen <= encodeHeaderLen) {
                vec.resize(i);
                break;
            }

            size_t newCutLen = MAX_OFFSET - dataLen - encodeHeaderLen;
            uint32_t newHeaderLen = VarNumAttributeFormatter::GetEncodedCountLength(newCutLen);
            vec[i] = vec[i].first(newCutLen);
            vec.resize(i + 1);
            latestOffset = dataLen;
            dataLen += (newHeaderLen + newCutLen);
            break;
        }
        latestOffset = dataLen;
        dataLen += (encodeHeaderLen + vec[i].size());
    }
}

inline void MultiStringAttributeConvertor::AppendOffsetAndData(const std::vector<autil::StringView>& vec,
                                                               uint8_t offsetLen, char*& writeBuf)
{
    // append offset length
    *(uint8_t*)writeBuf = offsetLen;
    ++writeBuf;

    char* offsetBuf = (char*)writeBuf;
    char* dataBuf = (char*)(offsetBuf + offsetLen * vec.size());
    uint32_t offset = 0;
    // append offset and data
    for (size_t i = 0; i < vec.size(); ++i) {
        size_t encodeLen = VarNumAttributeFormatter::EncodeCount(vec[i].size(), dataBuf, 4);
        dataBuf += encodeLen;
        memcpy(dataBuf, vec[i].data(), vec[i].size());
        dataBuf += vec[i].size();

        SetOffset(offsetBuf, offsetLen, offset);
        offsetBuf += offsetLen;
        offset += (encodeLen + vec[i].size());
    }
    writeBuf = dataBuf;
}

inline bool MultiStringAttributeConvertor::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    uint32_t count = 0;
    autil::StringView data = Decode(str).data;
    size_t countLen = 0;
    bool isNull = false;
    count = VarNumAttributeFormatter::DecodeCount(data.data(), countLen, isNull);
    if (isNull) {
        value = std::string("");
        return true;
    }

    autil::MultiValueType<autil::MultiValueType<char>> multiStr(data.data());
    std::vector<std::string> tmp;
    tmp.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        tmp.push_back(std::string(multiStr[i].data(), multiStr[i].size()));
    }
    value = autil::StringUtil::toString(tmp, "");
    return true;
}

DEFINE_SHARED_PTR(MultiStringAttributeConvertor);
}} // namespace indexlib::common
