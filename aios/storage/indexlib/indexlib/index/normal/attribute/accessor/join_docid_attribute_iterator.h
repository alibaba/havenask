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
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class JoinDocidAttributeIterator : public AttributeIteratorTyped<docid_t>
{
public:
    JoinDocidAttributeIterator(const std::vector<SegmentReaderPtr>& segReaders,
                               const BuildingAttributeReaderPtr& buildingAttributeReader,
                               const std::vector<uint64_t>& segmentDocCount, docid_t buildingBaseDocId,
                               const std::vector<docid_t>& joinedBaseDocIds,
                               const std::vector<docid_t>& joinedBuildingBaseDocIds, autil::mem_pool::Pool* pool)
        : AttributeIteratorTyped<docid_t>(segReaders, buildingAttributeReader, segmentDocCount, buildingBaseDocId,
                                          nullptr, pool)
        , mJoinedBaseDocIds(joinedBaseDocIds)
        , mJoinedBuildingBaseDocIds(joinedBuildingBaseDocIds)
    {
    }

    ~JoinDocidAttributeIterator() {}

public:
    inline bool Seek(docid_t docId, docid_t& value) __ALWAYS_INLINE;

private:
    std::vector<docid_t> mJoinedBaseDocIds;
    std::vector<docid_t> mJoinedBuildingBaseDocIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinDocidAttributeIterator);

/////////////////////////////////////////////////////////////
inline bool JoinDocidAttributeIterator::Seek(docid_t docId, docid_t& value)
{
    // TODO:test
    if (!AttributeIteratorTyped<docid_t>::Seek(docId, value)) {
        return false;
    }

    if (!mBuildingAttributeReader || docId < mBuildingBaseDocId) {
        assert(!mJoinedBaseDocIds.empty());
        assert(mJoinedBaseDocIds.size() == mSegmentDocCount.size());
        assert(mSegmentCursor < mSegmentDocCount.size());
        value += mJoinedBaseDocIds[mSegmentCursor];
    } else {
        value += mJoinedBuildingBaseDocIds[mBuildingSegIdx];
    }
    return true;
}
}} // namespace indexlib::index
