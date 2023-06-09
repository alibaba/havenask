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

#include "indexlib/base/Constant.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"

namespace indexlibv2::index {

class StringAttributeConvertor : public MultiValueAttributeConvertor<char>
{
public:
    StringAttributeConvertor(bool needHash)
        : MultiValueAttributeConvertor<char>(needHash, /*fieldName*/ "", /*fixedValueCount*/ -1,
                                             INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
    }

    StringAttributeConvertor(bool needHash, const std::string& fieldName)
        : MultiValueAttributeConvertor<char>(needHash, fieldName, /*fixedValueCount*/ -1,
                                             INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
    }

    StringAttributeConvertor(bool needHash, const std::string& fieldName, int32_t fixedValueCount)
        : MultiValueAttributeConvertor<char>(needHash, fieldName, fixedValueCount, INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
    }
    ~StringAttributeConvertor() = default;

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

    bool DecodeLiteralField(const autil::StringView& str, std::string& value) override;
};

//////////////////////////////////////////////////////
inline autil::StringView StringAttributeConvertor::InnerEncode(const autil::StringView& attrData,
                                                               autil::mem_pool::Pool* memPool, std::string& resultStr,
                                                               char* outBuffer, EncodeStatus& status)
{
    size_t originalDataSize = attrData.length();
    size_t count = originalDataSize;
    if (_fixedValueCount != -1 && (count != (size_t)_fixedValueCount)) {
        AUTIL_LOG(WARN, "fixed string_attribute count[%lu] mismatch fixedValueCount[%d]", originalDataSize,
                  _fixedValueCount);
        status = EncodeStatus::ES_VALUE_ERROR;
        count = _fixedValueCount;
    }
    count = AdjustCount(count);
    size_t bufSize = GetBufferSize(count);
    char* begin = (char*)Allocate(memPool, resultStr, outBuffer, bufSize);
    memset(begin, ' ', bufSize);
    char* buffer = begin;
    AppendHashAndCount(attrData, (uint32_t)count, buffer);
    memcpy(buffer, attrData.data(), std::min(originalDataSize, count));
    return autil::StringView(begin, bufSize);
}

inline bool StringAttributeConvertor::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    uint32_t count = 0;
    const char* addr = nullptr;
    autil::StringView data = Decode(str).data;
    if (_fixedValueCount != -1) {
        if (data.size() != _fixedValueCount) {
            return false;
        }
        addr = (const char*)data.data();
        count = _fixedValueCount;
    } else {
        size_t countLen = 0;
        bool isNull = false;
        count = MultiValueAttributeFormatter::DecodeCount(data.data(), countLen, isNull);
        if (isNull) {
            value = std::string("");
            return true;
        }
        addr = (const char*)(data.data() + countLen);
    }
    value = std::string(addr, count);
    return true;
}
} // namespace indexlibv2::index
