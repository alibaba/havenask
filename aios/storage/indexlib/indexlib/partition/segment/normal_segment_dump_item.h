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

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/partition_modifier_task_item.h"
#include "indexlib/partition/segment/segment_dump_item.h"
#include "indexlib/util/metrics/Metric.h"

DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);

namespace indexlib { namespace partition {

class NormalSegmentDumpItem : public SegmentDumpItem
{
public:
    NormalSegmentDumpItem(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                          const std::string& partitionName);
    virtual ~NormalSegmentDumpItem();

public:
    void Init(util::MetricPtr mdumpSegmentLatencyMetric, const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierDumpTaskItemPtr& partitionModifierDumpTaskItem,
              const FlushedLocatorContainerPtr& flushedLocatorContainer, bool releaseOperationAfterDump);

    void Dump() override;
    bool DumpWithMemLimit() override;
    bool IsDumped() const override;
    uint64_t GetEstimateDumpSize() const override;
    size_t GetTotalMemoryUse() const override;
    segmentid_t GetSegmentId() const override { return mInMemorySegment->GetSegmentId(); }
    index_base::SegmentInfoPtr GetSegmentInfo() const override { return mSegmentWriter->GetSegmentInfo(); }

public:
    index_base::InMemorySegmentPtr GetInMemorySegment() { return mInMemorySegment; }
    void SetReleaseOperationAfterDump(bool flag) { mReleaseOperationAfterDump = flag; }

private:
    struct MemoryStatusSnapShot {
        size_t fileSystemMemUse;
        size_t totalMemUse;
        size_t estimateDumpMemUse;
        size_t estimateDumpFileSize;
    };

private:
    MemoryStatusSnapShot TakeMemoryStatusSnapShot();
    bool CanDumpSegment(const util::MemoryReserverPtr& fileSystemMemReserver,
                        const util::MemoryReserverPtr& dumpTmpMemReserver);
    size_t EstimateDumpMemoryUse() const;
    void PrintBuildResourceMetrics();
    void CheckMemoryEstimation(MemoryStatusSnapShot& snapShot);
    size_t EstimateDumpFileSize() const;
    uint64_t GetInMemoryMemUse() const override { return mInMemorySegment->GetTotalMemoryUse(); }

private:
    void CleanResource();

private:
    util::MetricPtr mdumpSegmentLatencyMetric;
    file_system::DirectoryPtr mDirectory;
    OperationWriterPtr mOperationWriter;
    PartitionModifierDumpTaskItemPtr mPartitionModifierDumpTaskItem;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    index_base::SegmentWriterPtr mSegmentWriter;
    file_system::IFileSystemPtr mFileSystem;
    volatile bool mReleaseOperationAfterDump;
    uint64_t mEstimateDumpSize;

protected:
    // protected for test
    index_base::InMemorySegmentPtr mInMemorySegment;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalSegmentDumpItem);
}} // namespace indexlib::partition
