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
#include "indexlib/partition/partition_info_holder.h"

#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionInfoHolder);

PartitionInfoHolder::PartitionInfoHolder() {}

PartitionInfoHolder::~PartitionInfoHolder() {}

void PartitionInfoHolder::Init(const Version& version, const PartitionMeta& partitionMeta,
                               const SegmentDataVector& segmentDatas,
                               const std::vector<InMemorySegmentPtr>& dumpingSegments,
                               const index::DeletionMapReaderPtr& deletionMapReader)
{
    mPartitionInfo.reset(new index::PartitionInfo);
    PartitionMetaPtr partitionMetaPtr(new PartitionMeta);
    *partitionMetaPtr = partitionMeta;
    mPartitionInfo->Init(version, partitionMetaPtr, segmentDatas, dumpingSegments, deletionMapReader);
}

void PartitionInfoHolder::SetSubPartitionInfoHolder(const PartitionInfoHolderPtr& subPartitionInfoHolder)
{
    mSubPartitionInfoHolder = subPartitionInfoHolder;
    mPartitionInfo->SetSubPartitionInfo(subPartitionInfoHolder->GetPartitionInfo());
}

void PartitionInfoHolder::AddInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    PartitionInfoPtr clonePartitionInfo(mPartitionInfo->Clone());
    clonePartitionInfo->AddInMemorySegment(inMemSegment);

    SetPartitionInfo(clonePartitionInfo);
}

void PartitionInfoHolder::UpdatePartitionInfo(const InMemorySegmentPtr& inMemSegment)
{
    if (!mPartitionInfo->NeedUpdate(inMemSegment)) {
        return;
    }
    PartitionInfoPtr clonePartitionInfo(mPartitionInfo->Clone());
    clonePartitionInfo->UpdateInMemorySegment(inMemSegment);

    if (mSubPartitionInfoHolder) {
        const PartitionInfoPtr& subPartitionInfo = clonePartitionInfo->GetSubPartitionInfo();
        assert(subPartitionInfo);
        mSubPartitionInfoHolder->SetPartitionInfo(subPartitionInfo);
    }

    SetPartitionInfo(clonePartitionInfo);
}

PartitionInfoPtr PartitionInfoHolder::GetPartitionInfo() const
{
    ScopedReadWriteLock lock(mPartitionInfoLock, 'r');
    return mPartitionInfo;
}

void PartitionInfoHolder::SetPartitionInfo(const index::PartitionInfoPtr& partitionInfo)
{
    ScopedReadWriteLock lock(mPartitionInfoLock, 'w');
    mPartitionInfo = partitionInfo;
}
}} // namespace indexlib::partition
