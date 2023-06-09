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
#include "indexlib/partition/segment/dump_segment_container.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpSegmentContainer);

DumpSegmentContainer::DumpSegmentContainer()
    : mInMemSegContainer(new InMemorySegmentContainer)
    , mReclaimTimestamp(INVALID_TIMESTAMP)
{
}

DumpSegmentContainer::DumpSegmentContainer(const DumpSegmentContainer& other)
    : mDumpItems(other.mDumpItems)
    , mInMemSegContainer(other.mInMemSegContainer)
    , mReclaimTimestamp(other.mReclaimTimestamp)
{
}

DumpSegmentContainer::~DumpSegmentContainer() {}

void DumpSegmentContainer::PushDumpItem(const NormalSegmentDumpItemPtr& dumpItem)
{
    ScopedLock lock(mLock);
    dumpItem->SetReleaseOperationAfterDump(false);
    mDumpItems.push_back(dumpItem);
}

size_t DumpSegmentContainer::GetUnDumpedSegmentSize()
{
    ScopedLock lock(mLock);
    size_t count = 0;
    for (auto item : mDumpItems) {
        if (!item->IsDumped()) {
            count++;
        }
    }
    return count;
}

void DumpSegmentContainer::GetDumpingSegments(std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    ScopedLock lock(mLock);
    dumpingSegments.clear();
    for (auto item : mDumpItems) {
        dumpingSegments.push_back(item->GetInMemorySegment());
    }
}

void DumpSegmentContainer::GetSubDumpingSegments(std::vector<InMemorySegmentPtr>& dumpingSegments)
{
    ScopedLock lock(mLock);
    dumpingSegments.clear();
    for (auto item : mDumpItems) {
        dumpingSegments.push_back(item->GetInMemorySegment()->GetSubInMemorySegment());
    }
}

DumpSegmentContainer* DumpSegmentContainer::Clone()
{
    ScopedLock lock(mLock);
    return new DumpSegmentContainer(*this);
}

InMemorySegmentPtr DumpSegmentContainer::GetLastSegment()
{
    ScopedLock lock(mLock);
    if (mDumpItems.empty()) {
        return InMemorySegmentPtr();
    }
    return mDumpItems.back()->GetInMemorySegment();
}

NormalSegmentDumpItemPtr DumpSegmentContainer::GetOneSegmentItemToDump()
{
    ScopedLock lock(mLock);
    if (mDumpItems.empty()) {
        return NormalSegmentDumpItemPtr();
    }
    for (size_t i = 0; i < mDumpItems.size(); i++) {
        if (!mDumpItems[i]->IsDumped()) {
            return mDumpItems[i];
        }
    }
    return NormalSegmentDumpItemPtr();
}

void DumpSegmentContainer::ClearDumpedSegment()
{
    ScopedLock lock(mLock);
    std::vector<NormalSegmentDumpItemPtr> dumpItems;
    for (auto item : mDumpItems) {
        if (!item->IsDumped()) {
            dumpItems.push_back(item);
        }
    }
    mDumpItems.swap(dumpItems);
}

void DumpSegmentContainer::GetDumpedSegments(std::vector<InMemorySegmentPtr>& dumpedSegments)
{
    ScopedLock lock(mLock);
    for (auto item : mDumpItems) {
        if (item->IsDumped()) {
            dumpedSegments.push_back(item->GetInMemorySegment());
        }
    }
}

void DumpSegmentContainer::TrimObsoleteDumpingSegment(int64_t ts)
{
    ScopedLock lock(mLock);
    std::vector<NormalSegmentDumpItemPtr> dumpItems;
    size_t idx = 0;
    for (idx = 0; idx < mDumpItems.size(); idx++) {
        if (mDumpItems[idx]->GetInMemorySegment()->GetTimestamp() >= ts) {
            break;
        }
    }

    for (; idx < mDumpItems.size(); idx++) {
        dumpItems.push_back(mDumpItems[idx]);
    }
    mDumpItems.swap(dumpItems);
}

uint64_t DumpSegmentContainer::GetDumpingSegmentsSize()
{
    ScopedLock lock(mLock);
    uint64_t totalSize = 0;
    for (size_t i = 0; i < mDumpItems.size(); i++) {
        totalSize += mDumpItems[i]->GetEstimateDumpSize();
    }
    return totalSize;
}

uint64_t DumpSegmentContainer::GetMaxDumpingSegmentExpandMemUse()
{
    ScopedLock lock(mLock);
    uint64_t dumpingMemUse = 0;
    for (size_t i = 0; i < mDumpItems.size(); i++) {
        if (!mDumpItems[i]->IsDumped()) {
            dumpingMemUse = max(dumpingMemUse, mDumpItems[i]->GetEstimateDumpSize());
        }
    }
    return dumpingMemUse;
}

NormalSegmentDumpItemPtr DumpSegmentContainer::GetOneValidSegmentItemToDump() const
{
    ScopedLock lock(mLock);
    if (mDumpItems.empty()) {
        return NormalSegmentDumpItemPtr();
    }
    for (size_t i = 0; i < mDumpItems.size(); ++i) {
        const auto item = mDumpItems[i];
        if (!item->IsDumped() && item->GetInMemorySegment()->GetTimestamp() >= mReclaimTimestamp) {
            return item;
        }
    }
    return NormalSegmentDumpItemPtr();
}

void DumpSegmentContainer::SetReclaimTimestamp(uint64_t reclaimTimestamp)
{
    ScopedLock lock(mLock);
    mReclaimTimestamp = reclaimTimestamp;
}

int64_t DumpSegmentContainer::GetReclaimTimestamp() const
{
    ScopedLock lock(mLock);
    return mReclaimTimestamp;
}
}} // namespace indexlib::partition
