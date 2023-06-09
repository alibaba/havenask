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
#include "indexlib/index_base/segment/built_segment_iterator.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, BuiltSegmentIterator);

BuiltSegmentIterator::BuiltSegmentIterator() : mCurrentBaseDocId(INVALID_DOCID), mCursor(0) {}

BuiltSegmentIterator::~BuiltSegmentIterator() {}

bool BuiltSegmentIterator::IsValid() const { return mCursor < mSegmentDatas.size(); }

void BuiltSegmentIterator::MoveToNext()
{
    if (!IsValid()) {
        return;
    }
    mCurrentBaseDocId += mSegmentDatas[mCursor].GetSegmentInfo()->docCount;
    mCursor++;
}

InMemorySegmentPtr BuiltSegmentIterator::GetInMemSegment() const { return InMemorySegmentPtr(); }

SegmentData BuiltSegmentIterator::GetSegmentData() const
{
    SegmentData segData = IsValid() ? mSegmentDatas[mCursor] : SegmentData();
    segData.SetBaseDocId(mCurrentBaseDocId);
    return segData;
}

exdocid_t BuiltSegmentIterator::GetBaseDocId() const { return mCurrentBaseDocId; }

segmentid_t BuiltSegmentIterator::GetSegmentId() const
{
    return IsValid() ? mSegmentDatas[mCursor].GetSegmentId() : INVALID_SEGMENTID;
}

SegmentIteratorType BuiltSegmentIterator::GetType() const { return SIT_BUILT; }

void BuiltSegmentIterator::Reset()
{
    mCurrentBaseDocId = 0;
    mCursor = 0;
}

void BuiltSegmentIterator::Init(const std::vector<SegmentData>& segmentDatas)
{
    mSegmentDatas = segmentDatas;
    mCurrentBaseDocId = 0;
    mCursor = 0;
}
}} // namespace indexlib::index_base
