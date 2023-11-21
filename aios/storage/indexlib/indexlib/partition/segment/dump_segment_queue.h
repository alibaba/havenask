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

#include <list>
#include <memory>
#include <vector>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/building_segment_reader.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, CustomSegmentDumpItem);

namespace indexlib { namespace partition {

class DumpSegmentQueue
{
public:
    DumpSegmentQueue();
    ~DumpSegmentQueue();
    DumpSegmentQueue(const DumpSegmentQueue& other);

public:
    size_t Size() const
    {
        autil::ScopedLock lock(mLock);
        return mDumpItems.size();
    }
    void GetBuildingSegmentReaders(std::vector<table::BuildingSegmentReaderPtr>& buildingSegmentReaders,
                                   int64_t reclaimTimestamp = -1);
    DumpSegmentQueue* Clone();
    CustomSegmentDumpItemPtr GetLastSegment();
    void PushDumpItem(const CustomSegmentDumpItemPtr& dumpItem);
    // size_t GetDumpedQueueSize() const
    // {
    //     autil::ScopedLock lock(mLock);
    //     return mDumpedQueue.size();
    // };
    // size_t GetUnDumpedQueueSize() const
    // {
    //     autil::ScopedLock lock(mLock);
    //     return mDumpItems.size();
    // };
    void Clear()
    {
        autil::ScopedLock lock(mLock);
        mDumpItems.clear();
    }
    void GetDumpedItems(std::vector<CustomSegmentDumpItemPtr>& dumpedItems) const;
    void PopDumpedItems();
    bool ProcessOneItem(bool& hasNewDumpedSegment);
    uint64_t GetDumpingSegmentsSize() const;
    size_t GetEstimateDumpSize() const;
    void ReclaimSegments(int64_t reclaimTimestamp);

public:
    void LockQueue() { mLock.lock(); }
    void UnLockQueue() { mLock.unlock(); }

private:
    std::list<CustomSegmentDumpItemPtr> mDumpItems;
    mutable autil::RecursiveThreadMutex mLock;
    mutable autil::RecursiveThreadMutex mDumpLock;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpSegmentQueue);
}} // namespace indexlib::partition
