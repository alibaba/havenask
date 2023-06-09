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
#include "indexlib/index_base/segment/building_segment_iterator.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, BuildingSegmentIterator);

BuildingSegmentIterator::BuildingSegmentIterator() : mCurrentBaseDocId(INVALID_DOCID), mCursor(0) {}

BuildingSegmentIterator::~BuildingSegmentIterator() {}

void BuildingSegmentIterator::Init(const vector<InMemorySegmentPtr>& inMemSegments, exdocid_t baseDocId)
{
    mInMemSegments = inMemSegments;
    mCurrentBaseDocId = baseDocId;
    mCursor = 0;
}
bool BuildingSegmentIterator::IsValid() const { return mCursor < mInMemSegments.size(); }

InMemorySegmentPtr BuildingSegmentIterator::GetInMemSegment() const
{
    if (!IsValid()) {
        return InMemorySegmentPtr();
    }
    return mInMemSegments[mCursor];
}

exdocid_t BuildingSegmentIterator::GetBaseDocId() const { return IsValid() ? mCurrentBaseDocId : INVALID_DOCID; }

segmentid_t BuildingSegmentIterator::GetSegmentId() const
{
    return IsValid() ? GetSegmentData().GetSegmentId() : INVALID_SEGMENTID;
}

SegmentIteratorType BuildingSegmentIterator::GetType() const { return SIT_BUILDING; }

void BuildingSegmentIterator::Reset()
{
    mCurrentBaseDocId = 0;
    mCursor = 0;
}

SegmentData BuildingSegmentIterator::GetSegmentData() const
{
    if (IsValid()) {
        InMemorySegmentPtr inMemSegment = GetInMemSegment();
        assert(inMemSegment);
        SegmentData segData = inMemSegment->GetSegmentData();
        SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
        if (segWriter) {
            segData.SetSegmentInfo(*segWriter->GetSegmentInfo());
            segData.SetSegmentMetrics(*segWriter->GetSegmentMetrics());
        }
        segData.SetBaseDocId(mCurrentBaseDocId);
        return segData;
    }
    SegmentData segData;
    return segData;
}
void BuildingSegmentIterator::MoveToNext()
{
    if (!IsValid()) {
        return;
    }
    mCurrentBaseDocId += GetSegmentData().GetSegmentInfo()->docCount;
    mCursor++;
}
}} // namespace indexlib::index_base
