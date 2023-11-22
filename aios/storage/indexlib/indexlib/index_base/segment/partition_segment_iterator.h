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
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class PartitionSegmentIterator : public SegmentIterator
{
public:
    PartitionSegmentIterator(bool isOnline = true);
    ~PartitionSegmentIterator();

public:
    void Init(const std::vector<SegmentData>& onDiskSegments,
              const std::vector<InMemorySegmentPtr>& dumpingInMemSegments,
              const InMemorySegmentPtr& currentBuildingSegment, segmentid_t nextBuildingSegmentId = INVALID_SEGMENTID);

    bool IsValid() const override { return mCurrentIter->IsValid(); }

    void MoveToNext() override;
    void Reset() override;

    InMemorySegmentPtr GetInMemSegment() const override { return mCurrentIter->GetInMemSegment(); }

    SegmentData GetSegmentData() const override;

    exdocid_t GetBaseDocId() const override { return mCurrentIter->GetBaseDocId(); }

    segmentid_t GetSegmentId() const override { return mCurrentIter->GetSegmentId(); }

    SegmentIteratorType GetType() const override { return mCurrentIter->GetType(); }

    PartitionSegmentIterator* CreateSubPartitionSegmentItertor() const;

public:
    SegmentIteratorPtr CreateIterator(SegmentIteratorType type);
    bool GetLastSegmentData(SegmentData& segData) const;
    segmentid_t GetLastBuiltSegmentId() const;

    exdocid_t GetBuildingBaseDocId() const { return mBuildingBaseDocId; }

    size_t GetSegmentCount() const;

    segmentid_t GetNextBuildingSegmentId() const { return mNextBuildingSegmentId; }
    segmentid_t GetBuildingSegmentId() const { return mBuildingSegmentId; }

private:
    void InitNextSegmentId(const std::vector<SegmentData>& onDiskSegments,
                           const std::vector<InMemorySegmentPtr>& dumpingInMemSegments,
                           const InMemorySegmentPtr& currentBuildingSegment);

private:
    std::vector<SegmentData> mOnDiskSegments;
    std::vector<InMemorySegmentPtr> mBuildingSegments;
    exdocid_t mBuildingBaseDocId;
    SegmentIteratorPtr mCurrentIter;
    segmentid_t mNextBuildingSegmentId;
    segmentid_t mBuildingSegmentId;
    bool mIsOnline;

private:
    friend class BuildingPartitionDataTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionSegmentIterator);
}} // namespace indexlib::index_base
