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
#ifndef __INDEXLIB_ATTR_FIELD_VALUE_H
#define __INDEXLIB_ATTR_FIELD_VALUE_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlib { namespace index {

class AttrFieldValue
{
public:
    union AttrIdentifier {
        fieldid_t fieldId;
        packattrid_t packAttrId;
    };

public:
    AttrFieldValue();
    ~AttrFieldValue();

private:
    AttrFieldValue(const AttrFieldValue& other);

public:
    uint8_t* Data() const { return (uint8_t*)mBuffer.GetBuffer(); }

    const autil::StringView* GetConstStringData() const { return &mConstStringValue; }

    void SetDataSize(size_t size)
    {
        mSize = size;
        mConstStringValue = {mBuffer.GetBuffer(), mSize};
    }

    size_t GetDataSize() const { return mSize; }

    void ReserveBuffer(size_t size) { mBuffer.Reserve(size); }

    size_t BufferLength() const { return mBuffer.GetBufferSize(); }

    void SetFieldId(fieldid_t fieldId) { mIdentifier.fieldId = fieldId; }
    fieldid_t GetFieldId() const { return mIdentifier.fieldId; }

    void SetPackAttrId(packattrid_t packAttrId) { mIdentifier.packAttrId = packAttrId; }
    packattrid_t GetPackAttrId() const { return mIdentifier.packAttrId; }

    void SetIsPackAttr(bool isPacked) { mIsPackAttr = isPacked; }
    bool IsPackAttr() const { return mIsPackAttr; }

    void SetDocId(docid_t docId) { mDocId = docId; }
    docid_t GetDocId() const { return mDocId; }

    void SetIsSubDocId(bool isSubDocId) { mIsSubDocId = isSubDocId; }
    bool IsSubDocId() const { return mIsSubDocId; }

    void SetIsNull(bool isNull) { mIsNull = isNull; }
    bool IsNull() const { return mIsNull; }
    void Reset();

private:
    autil::StringView mConstStringValue;
    size_t mSize;
    util::MemBuffer mBuffer;
    AttrIdentifier mIdentifier;
    docid_t mDocId;
    bool mIsSubDocId;
    bool mIsPackAttr;
    bool mIsNull;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttrFieldValue);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTR_FIELD_VALUE_H
