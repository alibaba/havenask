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
#include "indexlib/partition/segment/dump_segment_queue.h"

#include "autil/StringUtil.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/table/building_segment_reader.h"
#include "indexlib/table/table_writer.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using namespace indexlib::table;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpSegmentQueue);

DumpSegmentQueue::DumpSegmentQueue() {}

DumpSegmentQueue::DumpSegmentQueue(const DumpSegmentQueue& other) : mDumpItems(other.mDumpItems) {}

DumpSegmentQueue::~DumpSegmentQueue() {}

DumpSegmentQueue* DumpSegmentQueue::Clone()
{
    ScopedLock lock(mLock);
    return new DumpSegmentQueue(*this);
}

void DumpSegmentQueue::PushDumpItem(const CustomSegmentDumpItemPtr& dumpItem)
{
    ScopedLock lock(mLock);
    mDumpItems.emplace_back(dumpItem);
}

void DumpSegmentQueue::GetDumpedItems(std::vector<CustomSegmentDumpItemPtr>& dumpedItems) const
{
    ScopedLock lock(mLock);
    dumpedItems.reserve(mDumpItems.size());
    for (auto item : mDumpItems) {
        if (item->IsDumped()) {
            dumpedItems.push_back(item);
        }
    }
}

void DumpSegmentQueue::PopDumpedItems()
{
    ScopedLock lock(mLock);

    stringstream ss;
    uint32_t popCounter = 0;

    while (!mDumpItems.empty()) {
        auto& item = mDumpItems.front();
        if (!item->IsDumped()) {
            break;
        }
        popCounter++;
        ss << item->GetSegmentId() << " ";
        mDumpItems.pop_front();
    }

    if (popCounter > 0) {
        IE_LOG(INFO, "DumpItems:[%s] poped from DumpSegmentQueue", ss.str().c_str());
    }
}

void DumpSegmentQueue::GetBuildingSegmentReaders(vector<table::BuildingSegmentReaderPtr>& dumpingSegmentReaders,
                                                 int64_t reclaimTimestamp)
{
    ScopedLock lock(mLock);
    for (const auto& item : mDumpItems) {
        if (reclaimTimestamp >= 0 && item->GetSegmentInfo()->timestamp < reclaimTimestamp) {
            continue;
        }
        auto tableWriter = item->GetTableWriter();
        auto segmentReader = tableWriter->GetBuildingSegmentReader();
        dumpingSegmentReaders.emplace_back(segmentReader);
    }
}

bool DumpSegmentQueue::ProcessOneItem(bool& hasNewDumpedSegment)
{
    hasNewDumpedSegment = false;
    CustomSegmentDumpItemPtr oldestItem;
    {
        ScopedLock lock(mLock);
        if (mDumpItems.empty()) {
            return true;
        }
        oldestItem = mDumpItems.front();
    }
    {
        ScopedLock lock(mDumpLock);
        if (oldestItem->IsDumped()) {
            hasNewDumpedSegment = false;
            return true;
        }
        try {
            oldestItem->Dump();
        } catch (const util::ExceptionBase& e) {
            IE_LOG(ERROR,
                   "flush dump segment[%d] end. "
                   "because catch exception:[%s].",
                   oldestItem->GetSegmentId(), e.what());
            return false;
        }
        hasNewDumpedSegment = true;
        return true;
    }
}

uint64_t DumpSegmentQueue::GetDumpingSegmentsSize() const
{
    ScopedLock lock(mLock);
    uint64_t totalSize = 0;
    for (const auto& item : mDumpItems) {
        totalSize += (item->GetEstimateDumpSize() + item->GetInMemoryMemUse());
    }
    return totalSize;
}

size_t DumpSegmentQueue::GetEstimateDumpSize() const
{
    ScopedLock lock(mLock);
    size_t totalSize = 0;
    for (const auto& item : mDumpItems) {
        totalSize += item->GetEstimateDumpSize();
    }
    return totalSize;
}

CustomSegmentDumpItemPtr DumpSegmentQueue::GetLastSegment()
{
    ScopedLock lock(mLock);
    if (!mDumpItems.empty()) {
        return mDumpItems.back();
    }
    return CustomSegmentDumpItemPtr();
}

void DumpSegmentQueue::ReclaimSegments(int64_t reclaimTimestamp)
{
    ScopedLock lock(mLock);
    if (reclaimTimestamp < 0) {
        return;
    }
    vector<segmentid_t> toReclaimRtSegIds;
    toReclaimRtSegIds.reserve(mDumpItems.size());
    for (auto iter = mDumpItems.begin(); iter != mDumpItems.end();) {
        if ((*iter)->GetSegmentInfo()->timestamp < reclaimTimestamp) {
            toReclaimRtSegIds.emplace_back((*iter)->GetSegmentId());
            iter = mDumpItems.erase(iter);
            continue;
        }
        ++iter;
    }
    if (!toReclaimRtSegIds.empty()) {
        string segListStr = StringUtil::toString(toReclaimRtSegIds, ",");
        IE_LOG(INFO, "dumpingSegments: %s will be reclaimed with relcaimTs[%ld]", segListStr.c_str(), reclaimTimestamp);
    }
}
}} // namespace indexlib::partition
