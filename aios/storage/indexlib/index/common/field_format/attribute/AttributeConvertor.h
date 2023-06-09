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

#include <string>

#include "autil/NoCopyable.h"
#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::index {

struct AttrValueMeta {
    AttrValueMeta() : hashKey(-1) {}
    explicit AttrValueMeta(uint64_t key, const std::string& str) : hashKey(key), data(str.c_str(), str.length()) {}
    explicit AttrValueMeta(uint64_t key, const autil::StringView& str) : hashKey(key), data(str) {}
    uint64_t hashKey;
    autil::StringView data;
};

class AttributeConvertor : private autil::NoCopyable
{
public:
    AttributeConvertor(bool needHash, const std::string& fieldName)
        : _needHash(needHash)
        , _encodeEmpty(false)
        , _fieldName(fieldName)
    {
    }
    virtual ~AttributeConvertor() {}

public:
    enum class EncodeStatus { ES_OK = 0, ES_TYPE_ERROR = 1, ES_VALUE_ERROR = 2, ES_UNKNOWN_ERROR = -1 };

public:
    virtual AttrValueMeta Decode(const autil::StringView& str) = 0;
    virtual std::pair<Status, std::string> EncodeNullValue() = 0;
    virtual autil::StringView EncodeFromAttrValueMeta(const AttrValueMeta& attrValueMeta,
                                                      autil::mem_pool::Pool* memPool) = 0;

    virtual autil::StringView EncodeFromRawIndexValue(const autil::StringView& rawValue,
                                                      autil::mem_pool::Pool* memPool) = 0;
    virtual bool DecodeLiteralField(const autil::StringView& str, std::string& value)
    {
        assert(false);
        return false;
    }
    inline autil::StringView Encode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                    bool& hasFormatError)
    {
        assert(memPool);
        std::string str;
        if (!attrData.empty() || _encodeEmpty) {
            EncodeStatus status = EncodeStatus::ES_OK;
            autil::StringView ret = InnerEncode(attrData, memPool, str, /*outBuf*/ nullptr, status);
            if (status != EncodeStatus::ES_OK) {
                hasFormatError = true;
            }
            return ret;
        }
        return attrData;
    }

    // legacy interface
    inline autil::StringView Encode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool)
    {
        assert(memPool);
        std::string str;
        if (!attrData.empty() || _encodeEmpty) {
            EncodeStatus status = EncodeStatus::ES_OK;
            return InnerEncode(attrData, memPool, str, nullptr, status);
        }
        return attrData;
    }

    inline std::string Encode(const std::string& str)
    {
        std::string resultStr;
        autil::StringView attrData(str);
        if (!attrData.empty() || _encodeEmpty) {
            EncodeStatus status = EncodeStatus::ES_OK;
            InnerEncode(attrData, /*memPool*/ nullptr, resultStr, /*outBuf*/ nullptr, status);
        }
        return resultStr;
    }

    inline autil::StringView Encode(const autil::StringView& attrData, char* buffer)
    {
        assert(buffer);
        std::string str;
        if (!attrData.empty() || _encodeEmpty) {
            EncodeStatus status = EncodeStatus::ES_OK;
            return InnerEncode(attrData, /*memPool*/ nullptr, str, /*outBuf*/ buffer, status);
        }
        return attrData;
    }

    void SetEncodeEmpty(bool encodeEmpty) { _encodeEmpty = encodeEmpty; }
    const std::string& GetFieldName() const { return _fieldName; }

private:
    virtual autil::StringView InnerEncode(const autil::StringView& attrData, autil::mem_pool::Pool* memPool,
                                          std::string& strResult, char* outBuffer, EncodeStatus& status) = 0;

protected:
    inline char* Allocate(autil::mem_pool::Pool* memPool, std::string& resultStr, char* outBuffer, size_t size)
    {
        if (outBuffer != nullptr) {
            return outBuffer;
        }
        if (memPool) {
            return (char*)memPool->allocate(size);
        }
        assert(resultStr.empty());
        resultStr.resize(size);
        return const_cast<char*>(resultStr.data());
    }

protected:
    bool _needHash;
    bool _encodeEmpty;
    std::string _fieldName;
};

} // namespace indexlibv2::index
