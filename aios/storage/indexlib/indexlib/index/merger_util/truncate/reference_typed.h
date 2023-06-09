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
#ifndef __INDEXLIB_REFERENCE_TYPED_H
#define __INDEXLIB_REFERENCE_TYPED_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/reference.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

template <typename T>
class ReferenceTyped : public Reference
{
public:
    ReferenceTyped(size_t offset, FieldType fieldType, bool supportNull) : Reference(offset, fieldType, supportNull) {}

    virtual ~ReferenceTyped() {};

public:
    void Get(const DocInfo* docInfo, T& value, bool& isNull);
    void Set(const T& value, const bool& isNull, DocInfo* docInfo);
    std::string GetStringValue(DocInfo* docInfo) override;

    uint32_t GetRefSize() const { return mSupportNull ? sizeof(T) + sizeof(bool) : sizeof(T); }

private:
    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
void ReferenceTyped<T>::Get(const DocInfo* docInfo, T& value, bool& isNull)
{
    if (!mSupportNull) {
        value = *(T*)(docInfo->Get(mOffset));
        isNull = false;
        return;
    }

    isNull = *(bool*)(docInfo->Get(mOffset));
    if (!isNull) {
        value = *(T*)(docInfo->Get(mOffset + sizeof(bool)));
    }
}

template <typename T>
void ReferenceTyped<T>::Set(const T& value, const bool& isNull, DocInfo* docInfo)
{
    if (!mSupportNull) {
        uint8_t* buffer = docInfo->Get(mOffset);
        *((T*)buffer) = value;
        return;
    }

    memcpy(docInfo->Get(mOffset), &isNull, sizeof(bool));
    if (!isNull) {
        memcpy(docInfo->Get(mOffset + sizeof(bool)), &value, sizeof(T));
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
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_REFERENCE_TYPED_H
