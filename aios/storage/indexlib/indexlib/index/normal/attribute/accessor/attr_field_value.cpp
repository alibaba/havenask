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
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttrFieldValue);

AttrFieldValue::AttrFieldValue() { Reset(); }

AttrFieldValue::~AttrFieldValue() {}

void AttrFieldValue::Reset()
{
    mIdentifier.fieldId = INVALID_FIELDID;
    mSize = 0;
    mDocId = INVALID_DOCID;
    mIsSubDocId = false;
    mConstStringValue = {mBuffer.GetBuffer(), mSize};
    mIsPackAttr = false;
    mIsNull = false;
}

AttrFieldValue::AttrFieldValue(const AttrFieldValue& other)
{
    ReserveBuffer(other.mBuffer.GetBufferSize());
    mIdentifier = other.mIdentifier;
    mSize = other.mSize;
    if (mBuffer.GetBuffer() != NULL) {
        memcpy(mBuffer.GetBuffer(), other.mBuffer.GetBuffer(), other.mSize);
    }
    mDocId = other.mDocId;
    mIsSubDocId = other.mIsSubDocId;
    mConstStringValue = {(char*)mBuffer.GetBuffer(), mSize};
    mIsPackAttr = other.mIsPackAttr;
    mIsNull = other.mIsNull;
}
}} // namespace indexlib::index
