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

#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/patch/PatchIterator.h"

namespace indexlibv2::index {

class AttributePatchIterator : public PatchIterator
{
public:
    enum AttrPatchType { APT_SINGLE = 0, APT_PACK = 1 };

public:
    AttributePatchIterator(AttrPatchType type, bool isSubDocId)
        : _docId(INVALID_DOCID)
        , _type(type)
        , _attrIdentifier(INVALID_FIELDID)
        , _isSub(isSubDocId)
    {
    }

    virtual ~AttributePatchIterator() {}

public:
    bool HasNext() const override { return _docId != INVALID_DOCID; }

    AttrPatchType GetType() const { return _type; }
    docid_t GetCurrentDocId() const { return _docId; }

    bool operator<(const AttributePatchIterator& other) const
    {
        if (_docId != other._docId) {
            return _docId < other._docId;
        }
        if (_type != other._type) {
            return _type < other._type;
        }
        return _attrIdentifier < other._attrIdentifier;
    }

protected:
    docid_t _docId;
    AttrPatchType _type;
    uint32_t _attrIdentifier;
    bool _isSub;
};

} // namespace indexlibv2::index
