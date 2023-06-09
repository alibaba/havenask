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
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::index {

class FieldValueExtractor
{
public:
    FieldValueExtractor(PackAttributeFormatter* formatter, autil::StringView packValue, autil::mem_pool::Pool* pool)
        : _formatter(formatter)
        , _packValue(std::move(packValue))
        , _pool(pool)
    {
        assert(formatter);
    }

    ~FieldValueExtractor() {}

public:
    template <typename T>
    bool GetTypedValue(const std::string& name, T& value) const __ALWAYS_INLINE;

    template <typename T>
    bool GetTypedValue(size_t idx, T& value) const __ALWAYS_INLINE;

    size_t GetFieldCount() const __ALWAYS_INLINE;
    bool GetValueMeta(const std::string& name, size_t& idx, FieldType& type, bool& isMulti) const __ALWAYS_INLINE;
    bool GetValueMeta(size_t idx, std::string& name, FieldType& type, bool& isMulti) const __ALWAYS_INLINE;
    bool GetStringValue(size_t idx, std::string& value) const __ALWAYS_INLINE;
    bool GetStringValue(size_t idx, std::string& name, std::string& value) const __ALWAYS_INLINE;
    bool GetBinaryField(size_t idx, autil::StringView& value) const __ALWAYS_INLINE;

    const autil::StringView& GetPackValue() const { return _packValue; }

private:
    PackAttributeFormatter* _formatter = nullptr;
    autil::StringView _packValue;
    autil::mem_pool::Pool* _pool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename T>
inline bool FieldValueExtractor::GetTypedValue(const std::string& name, T& value) const
{
    size_t idx = 0;
    if (!_formatter->GetAttributeStoreIndex(name, idx)) {
        return false;
    }
    return GetTypedValue(idx, value);
}

template <typename T>
inline bool FieldValueExtractor::GetTypedValue(size_t idx, T& value) const
{
    auto& referenceVec = _formatter->GetAttributeReferences();
    if (idx >= referenceVec.size() || _packValue.empty()) {
        return false;
    }

    if (indexlib::CppType2IndexlibFieldType<T>::GetFieldType() != referenceVec[idx]->GetFieldType() ||
        indexlib::CppType2IndexlibFieldType<T>::IsMultiValue() != referenceVec[idx]->IsMultiValue()) {
        return false;
    }
    AttributeReferenceTyped<T>* refType = static_cast<AttributeReferenceTyped<T>*>(referenceVec[idx].get());
    return refType->GetValue(_packValue.data(), value, _pool);
}

inline size_t FieldValueExtractor::GetFieldCount() const { return _formatter->GetAttributeReferences().size(); }

inline bool FieldValueExtractor::GetValueMeta(const std::string& name, size_t& idx, FieldType& type,
                                              bool& isMulti) const
{
    if (!_formatter->GetAttributeStoreIndex(name, idx)) {
        return false;
    }
    auto& referenceVec = _formatter->GetAttributeReferences();
    assert(idx < referenceVec.size());
    type = referenceVec[idx]->GetFieldType();
    isMulti = referenceVec[idx]->IsMultiValue();
    return true;
}

inline bool FieldValueExtractor::GetValueMeta(size_t idx, std::string& name, FieldType& type, bool& isMulti) const
{
    auto& referenceVec = _formatter->GetAttributeReferences();
    if (idx >= referenceVec.size()) {
        return false;
    }
    name = referenceVec[idx]->GetAttrName();
    type = referenceVec[idx]->GetFieldType();
    isMulti = referenceVec[idx]->IsMultiValue();
    return true;
}

inline bool FieldValueExtractor::GetBinaryField(size_t idx, autil::StringView& value) const
{
    auto& referenceVec = _formatter->GetAttributeReferences();
    if (idx >= referenceVec.size() || _packValue.empty()) {
        return false;
    }
    auto dataValue = referenceVec[idx]->GetDataValue(_packValue.data());
    value = dataValue.valueStr;
    return true;
}

inline bool FieldValueExtractor::GetStringValue(size_t idx, std::string& value) const
{
    auto& referenceVec = _formatter->GetAttributeReferences();
    if (idx >= referenceVec.size() || _packValue.empty()) {
        return false;
    }
    referenceVec[idx]->GetStrValue(_packValue.data(), value, _pool);
    return true;
}

inline bool FieldValueExtractor::GetStringValue(size_t idx, std::string& name, std::string& value) const
{
    auto& referenceVec = _formatter->GetAttributeReferences();
    if (idx >= referenceVec.size() || _packValue.empty()) {
        return false;
    }
    name = referenceVec[idx]->GetAttrName();
    return referenceVec[idx]->GetStrValue(_packValue.data(), value, _pool);
}

} // namespace indexlibv2::index
