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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/HashString.h"

namespace indexlibv2::index {
template <typename T>
class MultiValueAttributeConvertor : public AttributeConvertor
{
public:
    MultiValueAttributeConvertor()
        : AttributeConvertor(/*needHash*/ false, /*fieldName*/ "")
        , _fixedValueCount(/*fixedValueCount*/ -1)
        , _separator(INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
    {
        _encodeEmpty = true;
    }

    MultiValueAttributeConvertor(bool needHash, const std::string& fieldName, int32_t fixedValueCount,
                                 const std::string& seperator)
        : AttributeConvertor(needHash, fieldName)
        , _fixedValueCount(fixedValueCount)
        , _separator(seperator)
    {
        _encodeEmpty = true;
    }

    ~MultiValueAttributeConvertor() = default;

public:
    AttrValueMeta Decode(const autil::StringView& str) override;

    bool DecodeLiteralField(const autil::StringView& str, std::string& value) override;

    std::pair<Status, std::string> EncodeNullValue() override;

    autil::StringView EncodeFromAttrValueMeta(const AttrValueMeta& attrValueMeta,
                                              autil::mem_pool::Pool* memPool) override
    {
        size_t dataSize = attrValueMeta.data.size();
        size_t encodeSize = sizeof(uint64_t) + dataSize;
        char* begin = (char*)memPool->allocate(encodeSize);
        *(uint64_t*)begin = attrValueMeta.hashKey;
        memcpy(begin + sizeof(uint64_t), attrValueMeta.data.data(), dataSize);
        return autil::StringView(begin, encodeSize);
    }

    autil::StringView EncodeFromRawIndexValue(const autil::StringView& rawValue,
                                              autil::mem_pool::Pool* memPool) override
    {
        size_t encodeSize = sizeof(uint64_t) + rawValue.size();
        char* begin = (char*)memPool->allocate(encodeSize);
        uint64_t hashKey = (uint64_t)-1;
        if (_needHash) {
            hashKey = indexlib::util::HashString::Hash(rawValue.data(), rawValue.size());
        }
        *(uint64_t*)begin = hashKey;
        memcpy(begin + sizeof(uint64_t), rawValue.data(), rawValue.size());
        return autil::StringView(begin, encodeSize);
    }

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

protected:
    size_t AdjustCount(size_t count);
    void AppendHashAndCount(const autil::StringView& str, uint32_t count, char*& resultBuf);
    size_t GetBufferSize(size_t count)
    {
        if (_fixedValueCount == -1) {
            return sizeof(uint64_t) +                                           //  hash value
                   MultiValueAttributeFormatter::GetEncodedCountLength(count) + // count value
                   sizeof(T) * count;                                           //  actual value
        }
        return sizeof(uint64_t) +             //  hash value
               +sizeof(T) * _fixedValueCount; // actual value
    }
    template <typename Type>
    void write(char*& resultBuf, Type value)
    {
        *(Type*)resultBuf = value;
        resultBuf += sizeof(value);
    }
    virtual autil::StringView InnerEncode(const autil::StringView& originalData,
                                          const std::vector<autil::StringView>& attrDatas,
                                          autil::mem_pool::Pool* memPool, std::string& resultStr, char* outBuffer,
                                          EncodeStatus& status);

protected:
    int32_t _fixedValueCount;
    std::string _separator;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeConvertor, T);

template <typename T>
inline void MultiValueAttributeConvertor<T>::AppendHashAndCount(const autil::StringView& str, uint32_t count,
                                                                char*& resultBuf)
{
    uint64_t hashKey = (uint64_t)-1;
    if (_needHash) {
        hashKey = indexlib::util::HashString::Hash(str.data(), str.size());
    }

    write(resultBuf, hashKey);
    size_t encodeLen = 0;
    if (_fixedValueCount == -1) {
        encodeLen = MultiValueAttributeFormatter::EncodeCount(count, resultBuf, 4);
    }
    resultBuf += encodeLen;
}

template <typename T>
inline size_t MultiValueAttributeConvertor<T>::AdjustCount(size_t count)
{
    if (count > MultiValueAttributeFormatter::MULTI_VALUE_ATTRIBUTE_MAX_COUNT) {
        AUTIL_LOG(WARN, "var_num_attribute count overflow, %lu", count);
        return MultiValueAttributeFormatter::MULTI_VALUE_ATTRIBUTE_MAX_COUNT;
    }

    if (_fixedValueCount != -1 && count > (size_t)_fixedValueCount) {
        AUTIL_LOG(WARN, "fixed var_num_attribute count[%lu] overflow fixedValueCount[%d]", count, _fixedValueCount);
        return _fixedValueCount;
    }
    return count;
}

template <typename T>
inline autil::StringView
MultiValueAttributeConvertor<T>::InnerEncode(const autil::StringView& originalValue,
                                             const std::vector<autil::StringView>& vec, autil::mem_pool::Pool* memPool,
                                             std::string& resultStr, char* outBuffer, EncodeStatus& status)
{
    size_t tokenNum = vec.size();
    tokenNum = AdjustCount(tokenNum);
    size_t bufSize = GetBufferSize(tokenNum);
    char* begin = (char*)Allocate(memPool, resultStr, outBuffer, bufSize);

    char* buffer = begin;
    AppendHashAndCount(originalValue, (uint32_t)tokenNum, buffer);
    T* resultBuf = (T*)buffer;
    for (size_t i = 0; i < tokenNum; ++i) {
        T& value = *(resultBuf++);
        if (!StrToT(vec[i], value)) {
            status = EncodeStatus::ES_TYPE_ERROR;
            value = T();
            AUTIL_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", _fieldName.c_str(), originalValue.data());
            ERROR_COLLECTOR_LOG(DEBUG, "convert attribute[%s] error multi_value:[%s]", _fieldName.c_str(),
                                originalValue.data());
        }
    }
    assert(size_t(((char*)resultBuf) - begin) == bufSize);
    return autil::StringView(begin, bufSize);
}

template <typename T>
inline autil::StringView
MultiValueAttributeConvertor<T>::InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                             std::string& resultStr, char* outBuffer, EncodeStatus& status)
{
    std::vector<autil::StringView> vec = autil::StringTokenizer::constTokenize(
        attrData, _separator, autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (_fixedValueCount != -1 && vec.size() != (size_t)_fixedValueCount) {
        AUTIL_LOG(WARN,
                  "fixed var_num_attribute count[%lu] "
                  "mismatch fixedValueCount[%d] in attribute[%s]",
                  vec.size(), _fixedValueCount, _fieldName.c_str());
        status = EncodeStatus::ES_VALUE_ERROR;
        vec.resize(_fixedValueCount, autil::StringView::empty_instance());
    }
    return InnerEncode(attrData, vec, memPool, resultStr, outBuffer, status);
}

template <typename T>
inline bool MultiValueAttributeConvertor<T>::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    uint32_t count = 0;
    T* addr = nullptr;
    autil::StringView data = Decode(str).data;
    if (_fixedValueCount != -1) {
        if (data.size() != sizeof(T) * _fixedValueCount) {
            return false;
        }
        addr = (T*)data.data();
        count = _fixedValueCount;
    } else {
        size_t countLen = 0;
        bool isNull = false;
        count = MultiValueAttributeFormatter::DecodeCount(data.data(), countLen, isNull);
        if (isNull) {
            value = std::string("__NULL__");
            return true;
        }
        if (data.size() != (countLen + count * sizeof(T))) {
            return false;
        }
        addr = (T*)(data.data() + countLen);
    }
    std::vector<T> tmp(addr, addr + count);
    value = autil::StringUtil::toString(tmp, "");
    return true;
}

template <typename T>
inline AttrValueMeta MultiValueAttributeConvertor<T>::Decode(const autil::StringView& str)
{
    autil::StringView targetStr = str;
    std::string emptyAttrValue;
    if (targetStr.empty()) {
        const static std::string emptyAttrValue = Encode(std::string());
        targetStr = autil::StringView(emptyAttrValue);
    }

    assert(targetStr.length() >= sizeof(uint64_t));
    uint64_t key;
    key = *(uint64_t*)targetStr.data();
    autil::StringView data(targetStr.data() + sizeof(key), targetStr.length() - sizeof(key));
    return AttrValueMeta(key, data);
}

template <typename T>
inline std::pair<Status, std::string> MultiValueAttributeConvertor<T>::EncodeNullValue()
{
    char buf[20];
    char* cur = buf + sizeof(uint64_t);
    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(
        MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT, cur, 4);
    uint64_t hashKey = indexlib::util::HashString::Hash(cur, encodeLen);
    cur = buf;
    write(cur, hashKey);
    return std::make_pair(Status::OK(), std::string(buf, sizeof(uint64_t) + encodeLen));
}

} // namespace indexlibv2::index
