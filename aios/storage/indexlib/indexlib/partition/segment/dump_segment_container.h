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
#ifndef __INDEXLIB_DUMP_SEGMENT_CONTAINER_H
#define __INDEXLIB_DUMP_SEGMENT_CONTAINER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, NormalSegmentDumpItem);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);

namespace indexlib { namespace partition {

class DumpSegmentContainer
{
public:
    DumpSegmentContainer();
    virtual ~DumpSegmentContainer();
    DumpSegmentContainer(const DumpSegmentContainer& other);

public:
    void PushDumpItem(const NormalSegmentDumpItemPtr& dumpItem);
    InMemorySegmentContainerPtr GetInMemSegmentContainer() { return mInMemSegContainer; }
    void GetDumpingSegments(std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void GetSubDumpingSegments(std::vector<index_base::InMemorySegmentPtr>& dumpingSegments);
    void GetDumpedSegments(std::vector<index_base::InMemorySegmentPtr>& dumpItems);
    uint64_t GetMaxDumpingSegmentExpandMemUse();
    index_base::InMemorySegmentPtr GetLastSegment();
    DumpSegmentContainer* Clone();
    virtual void ClearDumpedSegment();
    size_t GetDumpItemSize()
    {
        autil::ScopedLock lock(mLock);
        return mDumpItems.size();
    }
    uint64_t GetDumpingSegmentsSize();
    size_t GetUnDumpedSegmentSize();
    void TrimObsoleteDumpingSegment(int64_t ts);
    virtual NormalSegmentDumpItemPtr GetOneSegmentItemToDump();
    virtual NormalSegmentDumpItemPtr GetOneValidSegmentItemToDump() const;
    void SetReclaimTimestamp(uint64_t reclaimTimestamp);
    int64_t GetReclaimTimestamp() const;

private:
    std::vector<NormalSegmentDumpItemPtr> mDumpItems;
    InMemorySegmentContainerPtr mInMemSegContainer;
    mutable autil::RecursiveThreadMutex mLock;
    int64_t mReclaimTimestamp;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_DUMP_SEGMENT_CONTAINER_H
