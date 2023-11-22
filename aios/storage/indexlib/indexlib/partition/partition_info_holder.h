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

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);

namespace indexlib { namespace partition {

class PartitionInfoHolder;
DEFINE_SHARED_PTR(PartitionInfoHolder);

class PartitionInfoHolder
{
public:
    PartitionInfoHolder();
    ~PartitionInfoHolder();

public:
    void Init(const index_base::Version& version, const index_base::PartitionMeta& partitionMeta,
              const std::vector<index_base::SegmentData>& segmentDatas,
              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
              const index::DeletionMapReaderPtr& deletionMapReader);

    void SetSubPartitionInfoHolder(const PartitionInfoHolderPtr& subPartitionInfoHolder);

    PartitionInfoHolderPtr GetSubPartitionInfoHolder() const { return mSubPartitionInfoHolder; }

    void AddInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);

    void UpdatePartitionInfo(const index_base::InMemorySegmentPtr& inMemSegment);

    index::PartitionInfoPtr GetPartitionInfo() const;
    void SetPartitionInfo(const index::PartitionInfoPtr& partitionInfo);

private:
    mutable autil::ReadWriteLock mPartitionInfoLock;
    index::PartitionInfoPtr mPartitionInfo;
    PartitionInfoHolderPtr mSubPartitionInfoHolder;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
