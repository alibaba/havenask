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
#include "indexlib/index_base/segment/partition_segment_iterator.h"

#include "indexlib/index_base/segment/building_segment_iterator.h"
#include "indexlib/index_base/segment/built_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_writer.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionSegmentIterator);

PartitionSegmentIterator::PartitionSegmentIterator(bool isOnline)
    : mBuildingBaseDocId(INVALID_DOCID)
    , mNextBuildingSegmentId(INVALID_SEGMENTID)
    , mBuildingSegmentId(INVALID_SEGMENTID)
    , mIsOnline(isOnline)
{
}

PartitionSegmentIterator::~PartitionSegmentIterator() {}

void PartitionSegmentIterator::Init(const vector<SegmentData>& onDiskSegments,
                                    const vector<InMemorySegmentPtr>& dumpingInMemSegments,
                                    const InMemorySegmentPtr& currentBuildingSegment, segmentid_t nextBuildingSegmentId)
{
    mOnDiskSegments = onDiskSegments;
    mBuildingSegments = dumpingInMemSegments;
    mNextBuildingSegmentId = nextBuildingSegmentId;
    if (currentBuildingSegment) {
        mBuildingSegments.push_back(currentBuildingSegment);
        mBuildingSegmentId = currentBuildingSegment->GetSegmentData().GetSegmentId();
    }

    mBuildingBaseDocId = 0;
    for (size_t i = 0; i < mOnDiskSegments.size(); i++) {
        mBuildingBaseDocId += mOnDiskSegments[i].GetSegmentInfo()->docCount;
    }

    InitNextSegmentId(onDiskSegments, dumpingInMemSegments, currentBuildingSegment);
    Reset();
}

SegmentIteratorPtr PartitionSegmentIterator::CreateIterator(SegmentIteratorType type)
{
    if (type == SIT_BUILT) {
        BuiltSegmentIteratorPtr builtSegIter(new BuiltSegmentIterator);
        builtSegIter->Init(mOnDiskSegments);
        return builtSegIter;
    }

    assert(type == SIT_BUILDING);
    BuildingSegmentIteratorPtr buildingSegIter(new BuildingSegmentIterator);
    buildingSegIter->Init(mBuildingSegments, mBuildingBaseDocId);
    return buildingSegIter;
}

void PartitionSegmentIterator::Reset()
{
    mCurrentIter = CreateIterator(SIT_BUILT);
    if (!mCurrentIter->IsValid()) {
        mCurrentIter = CreateIterator(SIT_BUILDING);
    }
}

void PartitionSegmentIterator::MoveToNext()
{
    mCurrentIter->MoveToNext();
    if (!mCurrentIter->IsValid() && mCurrentIter->GetType() == SIT_BUILT) {
        mCurrentIter = CreateIterator(SIT_BUILDING);
    }
}

bool PartitionSegmentIterator::GetLastSegmentData(SegmentData& segData) const
{
    if (GetSegmentCount() == 0) {
        return false;
    }

    if (!mBuildingSegments.empty()) {
        InMemorySegmentPtr inMemSegment = *mBuildingSegments.rbegin();
        assert(inMemSegment);
        segData = inMemSegment->GetSegmentData();
        SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
        if (segWriter) {
            segData.SetSegmentInfo(*segWriter->GetSegmentInfo());
            segData.SetSegmentMetrics(*segWriter->GetSegmentMetrics());
        }
        return true;
    }
    assert(!mOnDiskSegments.empty());
    segData = *mOnDiskSegments.rbegin();
    return true;
}

segmentid_t PartitionSegmentIterator::GetLastBuiltSegmentId() const
{
    return mOnDiskSegments.empty() ? INVALID_SEGMENTID : mOnDiskSegments.rbegin()->GetSegmentId();
}

void PartitionSegmentIterator::InitNextSegmentId(const vector<SegmentData>& onDiskSegments,
                                                 const vector<InMemorySegmentPtr>& dumpingInMemSegments,
                                                 const InMemorySegmentPtr& currentBuildingSegment)
{
    if (mNextBuildingSegmentId != INVALID_SEGMENTID) {
        return;
    }

    if (currentBuildingSegment) {
        mNextBuildingSegmentId = currentBuildingSegment->GetSegmentData().GetSegmentId() + 1;
        return;
    }

    if (!dumpingInMemSegments.empty()) {
        const InMemorySegmentPtr& lastDumpInMemSeg = *dumpingInMemSegments.rbegin();
        assert(lastDumpInMemSeg);
        mNextBuildingSegmentId = lastDumpInMemSeg->GetSegmentData().GetSegmentId() + 1;
        return;
    }

    mNextBuildingSegmentId = mIsOnline ? RealtimeSegmentDirectory::ConvertToRtSegmentId(0) : 0;
    if (!onDiskSegments.empty()) {
        const SegmentData& segData = *onDiskSegments.rbegin();
        mNextBuildingSegmentId = std::max(mNextBuildingSegmentId, segData.GetSegmentId() + 1);
    }
}

SegmentData PartitionSegmentIterator::GetSegmentData() const { return mCurrentIter->GetSegmentData(); }

size_t PartitionSegmentIterator::GetSegmentCount() const { return mOnDiskSegments.size() + mBuildingSegments.size(); }

PartitionSegmentIterator* PartitionSegmentIterator::CreateSubPartitionSegmentItertor() const
{
    unique_ptr<PartitionSegmentIterator> subIter(new PartitionSegmentIterator(mIsOnline));
    vector<SegmentData> onDiskSegments;
    exdocid_t buildingBaseDocId = 0;
    for (auto& seg : mOnDiskSegments) {
        SegmentDataPtr subSeg = seg.GetSubSegmentData();
        if (!subSeg) {
            return NULL;
        }
        onDiskSegments.push_back(*subSeg);
        buildingBaseDocId += subSeg->GetSegmentInfo()->docCount;
    }

    vector<InMemorySegmentPtr> buildingSegs;
    for (auto& seg : mBuildingSegments) {
        InMemorySegmentPtr subSeg = seg->GetSubInMemorySegment();
        if (!subSeg) {
            return NULL;
        }
        buildingSegs.push_back(subSeg);
    }
    subIter->mOnDiskSegments = onDiskSegments;
    subIter->mBuildingSegments = buildingSegs;
    subIter->mBuildingSegmentId = mBuildingSegmentId;
    subIter->mNextBuildingSegmentId = mNextBuildingSegmentId;
    subIter->mBuildingBaseDocId = buildingBaseDocId;
    subIter->Reset();
    return subIter.release();
}
}} // namespace indexlib::index_base
