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
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

#include "fslib/fslib.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapReader);

DeletionMapReader::DeletionMapReader(bool isSharedWriter) : mWriter(!isSharedWriter), mInMemBaseDocId(0) {}

DeletionMapReader::DeletionMapReader(uint32_t totalDocCount, bool isSharedWriter)
    : mWriter(!isSharedWriter)
    , mInMemBaseDocId(totalDocCount)
{
    mBitmap.reset(new Bitmap(totalDocCount));
}

DeletionMapReader::~DeletionMapReader() {}

bool DeletionMapReader::Open(PartitionData* partitionData)
{
    IE_LOG(INFO, "open deletion map reader begin");
    assert(partitionData);
    mWriter.Init(partitionData);
    mBitmap = mWriter.CreateGlobalBitmap();
    mInMemBaseDocId = mBitmap->GetItemCount();
    InitSegmentMetaMap(partitionData);

    InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
    if (inMemSegment) {
        mInMemDeletionMapReader = inMemSegment->GetSegmentReader()->GetInMemDeletionMapReader();
    }
    IE_LOG(INFO, "open deletion map reader end");
    return true;
}

void DeletionMapReader::InitSegmentMetaMap(PartitionData* partitionData)
{
    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid()) {
        if (iter->GetType() == SIT_BUILDING && iter->GetInMemSegment()->GetStatus() == InMemorySegment::BUILDING) {
            // building segment
            break;
        }
        segmentid_t segId = iter->GetSegmentId();
        docid_t baseDocid = iter->GetBaseDocId();
        mSegmentMetaMap[segId] = make_pair(baseDocid, iter->GetSegmentData().GetSegmentInfo()->docCount);
        iter->MoveToNext();
    }
}

uint32_t DeletionMapReader::GetDeletedDocCount(segmentid_t segId) const
{
    DeletionMapSegmentWriterPtr segmentWriter = mWriter.GetSegmentWriter(segId);
    if (segmentWriter) {
        return segmentWriter->GetDeletedCount();
    }
    SegmentMetaMap::const_iterator iter = mSegmentMetaMap.find(segId);
    if (iter != mSegmentMetaMap.end()) {
        SegmentMeta segMeta = iter->second;
        docid_t baseDocid = segMeta.first;
        uint32_t docCount = segMeta.second;

        if (docCount == 0) {
            return 0;
        }

        return mBitmap->GetSetCount((uint32_t)baseDocid, baseDocid + docCount - 1);
    }

    if (mInMemDeletionMapReader && segId == mInMemDeletionMapReader->GetInMemSegmentId()) {
        return mInMemDeletionMapReader->GetDeletedDocCount();
    }
    return 0;
}

bool DeletionMapReader::MergeBuildingBitmap(const DeletionMapReader& other)
{
    if (!mInMemDeletionMapReader || !other.mInMemDeletionMapReader) {
        IE_LOG(ERROR, "can not merge building bitmap with nullptr");
        return false;
    }
    if (mInMemDeletionMapReader->GetInMemSegmentId() != other.mInMemDeletionMapReader->GetInMemSegmentId()) {
        IE_LOG(ERROR, "segment id not equal this [%d] vs other [%d]", mInMemDeletionMapReader->GetInMemSegmentId(),
               other.mInMemDeletionMapReader->GetInMemSegmentId());
        return false;
    }
    util::ExpandableBitmap* thisBitMap = mInMemDeletionMapReader->GetBitmap();
    util::ExpandableBitmap* otherBitMap = other.mInMemDeletionMapReader->GetBitmap();
    if (thisBitMap->GetItemCount() != otherBitMap->GetItemCount()) {
        uint32_t maxCount = max(thisBitMap->GetItemCount(), otherBitMap->GetItemCount());
        thisBitMap->ReSize(maxCount);
        otherBitMap->ReSize(maxCount);
    }
    *thisBitMap |= *otherBitMap;
    return true;
}

const ExpandableBitmap* DeletionMapReader::GetSegmentDeletionMap(segmentid_t segId) const
{
    return mWriter.GetSegmentDeletionMap(segId);
}
}} // namespace indexlib::index
