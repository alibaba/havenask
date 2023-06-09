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

#include "indexlib/index/inverted_index/truncate/Reference.h"

namespace indexlib::index {

template <typename T>
class ReferenceTyped : public Reference
{
public:
    ReferenceTyped(size_t offset, FieldType fieldType, bool supportNull) : Reference(offset, fieldType, supportNull) {}
    virtual ~ReferenceTyped() = default;

public:
    std::string GetStringValue(DocInfo* docInfo) override;

public:
    void Get(const DocInfo* docInfo, T& value, bool& isNull) const;
    void Set(const T& value, const bool& isNull, DocInfo* docInfo);
    uint32_t GetRefSize() const { return _supportNull ? sizeof(T) + sizeof(bool) : sizeof(T); }
};

//////////////////////////////////////////////////////////////////////
template <typename T>
void ReferenceTyped<T>::Get(const DocInfo* docInfo, T& value, bool& isNull) const
{
    if (!_supportNull) {
        value = *(T*)(docInfo->Get(_offset));
        isNull = false;
        return;
    }

    isNull = *(bool*)(docInfo->Get(_offset));
    if (!isNull) {
        value = *(T*)(docInfo->Get(_offset + sizeof(bool)));
    }
}

template <typename T>
void ReferenceTyped<T>::Set(const T& value, const bool& isNull, DocInfo* docInfo)
{
    if (!_supportNull) {
        uint8_t* buffer = docInfo->Get(_offset);
        *((T*)buffer) = value;
        return;
    }

    memcpy(docInfo->Get(_offset), &isNull, sizeof(bool));
    if (!isNull) {
        memcpy(docInfo->Get(_offset + sizeof(bool)), &value, sizeof(T));
    }
}

template <typename T>
std::string ReferenceTyped<T>::GetStringValue(DocInfo* docInfo)
{
    T value {};
    bool isNull = false;
    Get(docInfo, value, isNull);
    if (!isNull) {
        return autil::StringUtil::toString(value);
    }
    return "NULL";
}

} // namespace indexlib::index
