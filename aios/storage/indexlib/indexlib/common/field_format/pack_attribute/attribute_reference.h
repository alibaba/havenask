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

#include "autil/ConstString.h"
#include "autil/PackDataFormatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class AttributeReference
{
public:
    typedef autil::PackOffset ReferenceOffset;
    struct DataValue {
    public:
        DataValue()
            : valueStr(autil::StringView::empty_instance())
            , valueCount(0)
            , hasCountInValueStr(false)
            , isFixedValueLen(false)
        {
        }

        DataValue(const autil::StringView& _valueStr, uint32_t _valueCount, bool _hasCountInValueStr,
                  bool _isFixedValueLen)
            : valueStr(_valueStr)
            , valueCount(_valueCount)
            , hasCountInValueStr(_hasCountInValueStr)
            , isFixedValueLen(_isFixedValueLen)
        {
        }
        autil::StringView valueStr;
        uint32_t valueCount;
        bool hasCountInValueStr;
        bool isFixedValueLen;
    };

public:
    AttributeReference(ReferenceOffset offset, const std::string& attrName, config::CompressTypeOption compressType,
                       int32_t fixedMultiValueCount, const std::string& seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR)
        : mOffset(offset)
        , mAttrName(attrName)
        , mCompressType(compressType)
        , mFixedMultiValueCount(fixedMultiValueCount)
        , mSeparator(seperator)
        , mFieldType(ft_unknown)
        , mIsMultiValue(false)
    {
    }

    virtual ~AttributeReference() {}

public:
    ReferenceOffset GetOffset() const { return mOffset; }

    // value from attribute document, with count header
    virtual size_t SetValue(char* baseAddr, size_t dataCursor, const autil::StringView& value) = 0;

    // value without count header
    virtual size_t SetDataValue(char* baseAddr, size_t dataCursor, const autil::StringView& value, uint32_t count) = 0;

    // return value
    // first:  binary data from pack attribute
    // second: be trimmed var len count which should store in binary header, -1 means stored in binary header
    virtual DataValue GetDataValue(const char* baseAddr) const = 0;

    const std::string& GetAttrName() const { return mAttrName; }
    bool IsMultiValue() const { return mIsMultiValue; }
    FieldType GetFieldType() const { return mFieldType; }

    // for test
    virtual bool GetStrValue(const char* baseAddr, std::string& value, autil::mem_pool::Pool* pool) const = 0;

    virtual bool LessThan(const char* lftAddr, const char* rhtAddr) const = 0;

    virtual size_t CalculateDataLength(const char* fieldData) const = 0;

    int32_t GetFixMultiValueCount() const { return mFixedMultiValueCount; }

    bool IsFixedMultiValue() const { return mFixedMultiValueCount != -1; }

    config::CompressTypeOption GetCompressType() const { return mCompressType; }

    bool EnableImpact() const { return mOffset.isImpactFormat(); }
    bool NeedVarLenHeader() const { return mOffset.needVarLenHeader(); }

protected:
    ReferenceOffset mOffset;
    std::string mAttrName;
    config::CompressTypeOption mCompressType;
    int32_t mFixedMultiValueCount; // -1 means un-specified
    std::string mSeparator;
    FieldType mFieldType;
    bool mIsMultiValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeReference);
}} // namespace indexlib::common
