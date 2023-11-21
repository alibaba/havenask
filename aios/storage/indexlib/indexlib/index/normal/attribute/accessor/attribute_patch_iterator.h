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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributePatchIterator : public PatchIterator
{
public:
    enum AttrPatchType { APT_SINGLE = 0, APT_PACK = 1 };

public:
    AttributePatchIterator(AttrPatchType type, bool isSubDocId)
        : mDocId(INVALID_DOCID)
        , mType(type)
        , mAttrIdentifier(INVALID_FIELDID)
        , mIsSub(isSubDocId)
    {
    }

    virtual ~AttributePatchIterator() {}

public:
    bool HasNext() const { return mDocId != INVALID_DOCID; }

    virtual void Next(AttrFieldValue& value) = 0;
    virtual void Reserve(AttrFieldValue& value) = 0;

    AttrPatchType GetType() const { return mType; }
    docid_t GetCurrentDocId() const { return mDocId; }

    bool operator<(const AttributePatchIterator& other) const
    {
        if (mDocId != other.mDocId) {
            return mDocId < other.mDocId;
        }
        if (mType != other.mType) {
            return mType < other.mType;
        }
        return mAttrIdentifier < other.mAttrIdentifier;
    }

protected:
    docid_t mDocId;
    AttrPatchType mType;
    uint32_t mAttrIdentifier;
    bool mIsSub;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchIterator);
}} // namespace indexlib::index
