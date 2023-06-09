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
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TypeInfo.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeConvertor : public AttributeConvertor
{
public:
    SingleValueAttributeConvertor() : AttributeConvertor(/*needHash*/ false, /*fieldName*/ "") {}
    SingleValueAttributeConvertor(bool needHash, const std::string& fieldName) : AttributeConvertor(needHash, fieldName)
    {
    }
    virtual ~SingleValueAttributeConvertor() = default;

public:
    std::pair<Status, std::string> EncodeNullValue() override;
    AttrValueMeta Decode(const autil::StringView& str) override;
    bool DecodeLiteralField(const autil::StringView& str, std::string& value) override;

    static autil::StringView EncodeValue(T value, autil::mem_pool::Pool* memPool)
    {
        T* dst = (T*)memPool->allocate(sizeof(T));
        *dst = value;
        return autil::StringView((char*)dst, sizeof(T));
    }

    autil::StringView EncodeFromAttrValueMeta(const AttrValueMeta& attrValueMeta,
                                              autil::mem_pool::Pool* memPool) override
    {
        return autil::MakeCString(attrValueMeta.data.data(), attrValueMeta.data.size(), memPool);
    }

    autil::StringView EncodeFromRawIndexValue(const autil::StringView& rawValue,
                                              autil::mem_pool::Pool* memPool) override
    {
        return autil::MakeCString(rawValue.data(), rawValue.size(), memPool);
    }

private:
    autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                  std::string& resultStr, char* outBuffer, EncodeStatus& status) override;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeConvertor, T);

template <typename T>
inline autil::StringView
SingleValueAttributeConvertor<T>::InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                              std::string& resultStr, char* outBuffer, EncodeStatus& status)
{
    assert(_needHash == false);
    T* value = (T*)Allocate(memPool, resultStr, outBuffer, sizeof(T));

    if (!StrToT(attrData, *value)) {
        status = EncodeStatus::ES_TYPE_ERROR;
        *value = T();
        AUTIL_LOG(DEBUG, "convert attribute[%s] error value:[%s]", _fieldName.c_str(), attrData.data());
        ERROR_COLLECTOR_LOG(ERROR, "convert attribute[%s] error value:[%s]", _fieldName.c_str(), attrData.data());
    }
    return autil::StringView((char*)value, sizeof(T));
}

template <typename T>
inline AttrValueMeta SingleValueAttributeConvertor<T>::Decode(const autil::StringView& str)
{
    assert(str.length() == sizeof(T));
    return AttrValueMeta(uint64_t(-1), str);
}

template <typename T>
inline bool SingleValueAttributeConvertor<T>::DecodeLiteralField(const autil::StringView& str, std::string& value)
{
    if (str.length() != sizeof(T)) {
        return false;
    }

    T* data = (T*)str.data();
    value = autil::StringUtil::toString(*data);
    return true;
}

template <typename T>
inline std::pair<Status, std::string> SingleValueAttributeConvertor<T>::EncodeNullValue()
{
    AUTIL_LOG(ERROR, "single value not support EncodeNullValue.");
    return std::make_pair(Status::Unimplement("single value not support EncodeNullValue"), std::string(""));
}
} // namespace indexlibv2::index
