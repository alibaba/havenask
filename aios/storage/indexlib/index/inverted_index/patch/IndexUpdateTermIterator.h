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

#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"

namespace indexlib::index {

class IndexUpdateTermIterator
{
public:
    IndexUpdateTermIterator(index::DictKeyInfo termKey, segmentid_t segId, indexid_t indexId)
        : _termKey(termKey)
        , _segmentId(segId)
        , _indexId(indexId)
        , _isSub(false)
    {
    }
    IndexUpdateTermIterator() : _segmentId(0), _indexId(INVALID_INDEXID), _isSub(false) {}
    virtual ~IndexUpdateTermIterator() {}

public:
    index::DictKeyInfo GetTermKey() const { return _termKey; }
    segmentid_t GetSegmentId() const { return _segmentId; }
    indexid_t GetIndexId() const { return _indexId; }
    void SetIsSub(bool isSub) { _isSub = isSub; }
    bool IsSub() const { return _isSub; }

    virtual bool Next(ComplexDocId* complexDocId) = 0;
    virtual size_t GetProcessedDocs() const = 0;

private:
    index::DictKeyInfo _termKey;
    segmentid_t _segmentId;
    indexid_t _indexId;
    bool _isSub;
};
} // namespace indexlib::index
