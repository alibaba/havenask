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
#include "indexlib/partition/segment/normal_segment_dump_item.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, NormalSegmentDumpItem);

#define IE_LOG_PREFIX mPartitionName.c_str()

NormalSegmentDumpItem::NormalSegmentDumpItem(const config::IndexPartitionOptions& options,
                                             const config::IndexPartitionSchemaPtr& schema, const string& partitionName)
    : SegmentDumpItem(options, schema, partitionName)
{
}

NormalSegmentDumpItem::~NormalSegmentDumpItem()
{
    mSegmentWriter.reset();
    mOperationWriter.reset();
}

void NormalSegmentDumpItem::Init(util::MetricPtr dumpSegmentLatencyMetric, const PartitionDataPtr& partitionData,
                                 const PartitionModifierDumpTaskItemPtr& partitionModifierDumpTaskItem,
                                 const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                 bool releaseOperationAfterDump)
{
    mdumpSegmentLatencyMetric = dumpSegmentLatencyMetric;
    mInMemorySegment = partitionData->GetInMemorySegment();
    mInMemorySegment->SetStatus(InMemorySegment::WAITING_TO_DUMP);
    mSegmentWriter = mInMemorySegment->GetSegmentWriter();
    mOperationWriter = DYNAMIC_POINTER_CAST(OperationWriter, mInMemorySegment->GetOperationWriter());
    mPartitionModifierDumpTaskItem = partitionModifierDumpTaskItem;
    mFlushedLocatorContainer = flushedLocatorContainer;
    mDirectory = partitionData->GetRootDirectory();
    mFileSystem = mDirectory->GetFileSystem();
    mReleaseOperationAfterDump = releaseOperationAfterDump;
    mEstimateDumpSize = EstimateDumpMemoryUse() + EstimateDumpFileSize();
}

void NormalSegmentDumpItem::Dump()
{
    ScopeLatencyReporter scopeTime(mdumpSegmentLatencyMetric.get());
    CleanResource();
    mInMemorySegment->SetStatus(InMemorySegment::DUMPING);
    MemoryStatusSnapShot snapShot = TakeMemoryStatusSnapShot();
    PrintBuildResourceMetrics();
    if (mOperationWriter) {
        mOperationWriter->SetReleaseBlockAfterDump(mReleaseOperationAfterDump);
    }
    mInMemorySegment->BeginDump();
    const DirectoryPtr& directory = mInMemorySegment->GetDirectory();
    if (mPartitionModifierDumpTaskItem != nullptr) {
        mPartitionModifierDumpTaskItem->Dump(mSchema, directory, mInMemorySegment->GetSegmentId(),
                                             mOptions.GetBuildConfig().dumpThreadCount);
    }
    mInMemorySegment->EndDump();
    CheckMemoryEstimation(snapShot);

    PrintBuildResourceMetrics();

    int64_t dumpMemoryPeak = mInMemorySegment->GetDumpMemoryUse();
    if (mPartitionModifierDumpTaskItem != nullptr) {
        const util::BuildResourceMetricsPtr buildMetrics = mPartitionModifierDumpTaskItem->buildResourceMetrics;
        assert(buildMetrics);
        dumpMemoryPeak = max(dumpMemoryPeak, buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    }

    auto flushFuture = mDirectory->Sync(false);
    if (mFlushedLocatorContainer) {
        document::Locator locator(mSegmentWriter->GetSegmentInfo()->GetLocator().Serialize());
        mFlushedLocatorContainer->Push(flushFuture, locator);
    }
    mInMemorySegment->SetStatus(InMemorySegment::DUMPED);
    IE_PREFIX_LOG(DEBUG, "Dump segment[%s] finished", directory->DebugString().c_str());
}

bool NormalSegmentDumpItem::DumpWithMemLimit()
{
    util::MemoryReserverPtr fileSystemMemReserver = mFileSystem->CreateMemoryReserver("partition_writer");
    util::MemoryReserverPtr dumpTmpMemReserver = mInMemorySegment->CreateMemoryReserver("partition_writer");
    if (!CanDumpSegment(fileSystemMemReserver, dumpTmpMemReserver)) {
        return false;
    }
    Dump();
    return true;
}

bool NormalSegmentDumpItem::IsDumped() const { return mInMemorySegment->GetStatus() == InMemorySegment::DUMPED; }

uint64_t NormalSegmentDumpItem::GetEstimateDumpSize() const { return mEstimateDumpSize; }

size_t NormalSegmentDumpItem::GetTotalMemoryUse() const
{
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mPartitionModifierDumpTaskItem != nullptr) {
        buildResourceMetrics = mPartitionModifierDumpTaskItem->buildResourceMetrics;
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.GetCurrentMemoryUse();
}

NormalSegmentDumpItem::MemoryStatusSnapShot NormalSegmentDumpItem::TakeMemoryStatusSnapShot()
{
    MemoryStatusSnapShot snapShot;
    snapShot.fileSystemMemUse = mFileSystem->GetFileSystemMemoryUse();
    snapShot.totalMemUse = GetTotalMemoryUse();
    snapShot.estimateDumpMemUse = EstimateDumpMemoryUse();
    snapShot.estimateDumpFileSize = EstimateDumpFileSize();
    return snapShot;
}

size_t NormalSegmentDumpItem::EstimateDumpMemoryUse() const
{
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mPartitionModifierDumpTaskItem != nullptr) {
        buildResourceMetrics = mPartitionModifierDumpTaskItem->buildResourceMetrics;
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.EstimateDumpMemoryUse();
}

void NormalSegmentDumpItem::PrintBuildResourceMetrics()
{
    if (mSegmentWriter) {
        const util::BuildResourceMetricsPtr& buildMetrics = mSegmentWriter->GetBuildResourceMetrics();
        if (buildMetrics) {
            buildMetrics->Print("SegmentWriter");
        }
    }
    util::BuildResourceMetricsPtr buildMetrics = nullptr;
    if (mPartitionModifierDumpTaskItem != nullptr) {
        buildMetrics = mPartitionModifierDumpTaskItem->buildResourceMetrics;
    }

    if (buildMetrics) {
        buildMetrics->Print("Modifier");
    }
    if (mOperationWriter) {
        const util::BuildResourceMetricsPtr& buildMetrics = mOperationWriter->GetBuildResourceMetrics();
        if (buildMetrics) {
            buildMetrics->Print("OperationWriter");
        }
    }
}

void NormalSegmentDumpItem::CheckMemoryEstimation(MemoryStatusSnapShot& snapShot)
{
    size_t actualDumpFileSize = mFileSystem->GetFileSystemMemoryUse() - snapShot.fileSystemMemUse;

    size_t totalMemUseAfterDump = GetTotalMemoryUse();
    size_t actualDumpMemUse = mInMemorySegment->GetDumpMemoryUse() + totalMemUseAfterDump - snapShot.totalMemUse;
    IE_PREFIX_LOG(INFO, "actualDumpFileSize[%lu], actualDumpMemUse[%lu]", actualDumpFileSize, actualDumpMemUse);
    IE_PREFIX_LOG(INFO, "estimateDumpFileSize[%lu], estimateDumpMemUse[%lu]", snapShot.estimateDumpFileSize,
                  snapShot.estimateDumpMemUse);

    if (actualDumpFileSize > snapShot.estimateDumpFileSize) {
        IE_PREFIX_LOG(INFO, "actualDumpFileSize[%lu] is larger than estimateDumpFileSize[%lu]", actualDumpFileSize,
                      snapShot.estimateDumpFileSize);
        // assert(false);
    }
    if (actualDumpMemUse > snapShot.estimateDumpMemUse) {
        IE_PREFIX_LOG(INFO, "actualDumpMemUse[%lu] is larger than estimateDumpMemUse[%lu]", actualDumpMemUse,
                      snapShot.estimateDumpMemUse);
        // assert(false);
    }
}

size_t NormalSegmentDumpItem::EstimateDumpFileSize() const
{
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mPartitionModifierDumpTaskItem != nullptr) {
        buildResourceMetrics = mPartitionModifierDumpTaskItem->buildResourceMetrics;
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.EstimateDumpFileSize();
}

bool NormalSegmentDumpItem::CanDumpSegment(const util::MemoryReserverPtr& fileSystemMemReserver,
                                           const util::MemoryReserverPtr& dumpTmpMemReserver)
{
    size_t dumpFileSize = EstimateDumpFileSize();
    if (!fileSystemMemReserver->Reserve(dumpFileSize)) {
        IE_PREFIX_LOG(WARN, "memory not enough for dump file[size %lu B]", dumpFileSize);
        return false;
    }
    size_t dumpTmpSize = EstimateDumpMemoryUse();
    if (!dumpTmpMemReserver->Reserve(dumpTmpSize)) {
        IE_PREFIX_LOG(WARN, "memory not enough for dump temp memory use[size %lu B]", dumpTmpSize);
        return false;
    }
    return true;
}

void NormalSegmentDumpItem::CleanResource()
{
    mFileSystem->Sync(true).GetOrThrow();
    mFileSystem->CleanCache();
}
}} // namespace indexlib::partition
