#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/common/document_deduper_helper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/util/memory_control/memory_reserver.h"

IE_NAMESPACE_USE(misc);
using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);


IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, NormalSegmentDumpItem);

#define IE_LOG_PREFIX mPartitionName.c_str()

NormalSegmentDumpItem::NormalSegmentDumpItem(
    const config::IndexPartitionOptions& options,
    const config::IndexPartitionSchemaPtr& schema,
    const string& partitionName)
    : SegmentDumpItem(options, schema, partitionName)
{
}

NormalSegmentDumpItem::~NormalSegmentDumpItem()
{
    mSegmentWriter.reset();
    mOperationWriter.reset();
}

void NormalSegmentDumpItem::Init(misc::MetricPtr dumpSegmentLatencyMetric,
                           const PartitionDataPtr& partitionData,
                           const PartitionModifierPtr& modifier,
                           const FlushedLocatorContainerPtr& flushedLocatorContainer,
                           bool releaseOperationAfterDump)
{
    mdumpSegmentLatencyMetric = dumpSegmentLatencyMetric;
    mInMemorySegment = partitionData->GetInMemorySegment();
    mInMemorySegment->SetStatus(InMemorySegment::WAITING_TO_DUMP);
    mSegmentWriter = mInMemorySegment->GetSegmentWriter();
    mOperationWriter = DYNAMIC_POINTER_CAST(OperationWriter, mInMemorySegment->GetOperationWriter());
    mModifier = modifier;
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
    EndSegment();
    if (mOperationWriter)
    {
        mOperationWriter->SetReleaseBlockAfterDump(mReleaseOperationAfterDump); 
    }
    mInMemorySegment->BeginDump();
    const DirectoryPtr& directory = mInMemorySegment->GetDirectory();
    if (mModifier)
    {
        mModifier->SetDumpThreadNum(mOptions.GetBuildConfig().dumpThreadCount);
        mModifier->Dump(directory, mInMemorySegment->GetSegmentId());
    }
    mInMemorySegment->EndDump();
    CheckMemoryEstimation(snapShot);
    
    PrintBuildResourceMetrics();

    int64_t dumpMemoryPeak = mInMemorySegment->GetDumpMemoryUse();
    
    if (mModifier)
    {
        const util::BuildResourceMetricsPtr buildMetrics =
            mModifier->GetBuildResourceMetrics();
        assert(buildMetrics);
        dumpMemoryPeak = max(dumpMemoryPeak,
                             buildMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE));
    }
    auto flushFuture = mDirectory->Sync(false);
    if (mFlushedLocatorContainer)
    {
        document::Locator locator = mSegmentWriter->GetSegmentInfo()->locator;
        mFlushedLocatorContainer->Push(flushFuture, locator);
    }
    mInMemorySegment->SetStatus(InMemorySegment::DUMPED);
    IE_PREFIX_LOG(DEBUG, "Dump segment[%s] finished", directory->GetPath().c_str());
}

bool NormalSegmentDumpItem::DumpWithMemLimit()
{
    util::MemoryReserverPtr fileSystemMemReserver = mFileSystem->CreateMemoryReserver("partition_writer");
    util::MemoryReserverPtr dumpTmpMemReserver = mInMemorySegment->CreateMemoryReserver("partition_writer");
    if (!CanDumpSegment(fileSystemMemReserver, dumpTmpMemReserver))
    {
        return false;
    }
    Dump();
    return true;
}

bool NormalSegmentDumpItem::IsDumped() const
{
    return mInMemorySegment->GetStatus() == InMemorySegment::DUMPED;
}

uint64_t NormalSegmentDumpItem::GetEstimateDumpSize() const
{
    return mEstimateDumpSize;
}

size_t NormalSegmentDumpItem::GetTotalMemoryUse() const
{
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
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
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.EstimateDumpMemoryUse();    
}

void NormalSegmentDumpItem::PrintBuildResourceMetrics()
{
    if (mSegmentWriter)
    {
        const util::BuildResourceMetricsPtr& buildMetrics =
            mSegmentWriter->GetBuildResourceMetrics();
        if (buildMetrics)
        {
            buildMetrics->Print("SegmentWriter");
        }        
    }
    if (mModifier)
    {
        const util::BuildResourceMetricsPtr& buildMetrics =
            mModifier->GetBuildResourceMetrics();
        if (buildMetrics)
        {
            buildMetrics->Print("Modifier");
        }        
    }
    if (mOperationWriter)
    {
        const util::BuildResourceMetricsPtr& buildMetrics =
            mOperationWriter->GetBuildResourceMetrics();
        if (buildMetrics)
        {
            buildMetrics->Print("OperationWriter");
        }                
    }
}

void NormalSegmentDumpItem::EndSegment()
{
    bool delayDedupDocument = common::DocumentDeduperHelper::DelayDedupDocument(
        mOptions, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    if (!delayDedupDocument)
    {
        return;
    }

    DocumentDeduperPtr deduper = 
        DocumentDeduperCreator::CreateBuildingSegmentDeduper(
                mSchema, mOptions, mInMemorySegment, mModifier);
    if (deduper)
    {
        deduper->Dedup();
    }
}

void NormalSegmentDumpItem::CheckMemoryEstimation(MemoryStatusSnapShot& snapShot)
{
    size_t actualDumpFileSize = mFileSystem->GetFileSystemMemoryUse() - snapShot.fileSystemMemUse;
    
    size_t totalMemUseAfterDump = GetTotalMemoryUse();
    size_t actualDumpMemUse = mInMemorySegment->GetDumpMemoryUse() + 
                              totalMemUseAfterDump - snapShot.totalMemUse;
    IE_PREFIX_LOG(INFO, "actualDumpFileSize[%lu], actualDumpMemUse[%lu]",
                  actualDumpFileSize, actualDumpMemUse);
    IE_PREFIX_LOG(INFO, "estimateDumpFileSize[%lu], estimateDumpMemUse[%lu]",
                  snapShot.estimateDumpFileSize, snapShot.estimateDumpMemUse);

    if (actualDumpFileSize > snapShot.estimateDumpFileSize)
    {
        IE_PREFIX_LOG(INFO, "actualDumpFileSize[%lu] is larger than estimateDumpFileSize[%lu]",
                      actualDumpFileSize, snapShot.estimateDumpFileSize);
        // assert(false);
    }
    if (actualDumpMemUse > snapShot.estimateDumpMemUse)
    {
        IE_PREFIX_LOG(INFO, "actualDumpMemUse[%lu] is larger than estimateDumpMemUse[%lu]",
                      actualDumpMemUse, snapShot.estimateDumpMemUse);
        // assert(false);
    }
}

size_t NormalSegmentDumpItem::EstimateDumpFileSize() const
{
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.EstimateDumpFileSize();        
}

bool NormalSegmentDumpItem::CanDumpSegment(
        const util::MemoryReserverPtr& fileSystemMemReserver,
        const util::MemoryReserverPtr& dumpTmpMemReserver)
{
    size_t dumpFileSize = EstimateDumpFileSize();
    if (!fileSystemMemReserver->Reserve(dumpFileSize))
    {
        IE_PREFIX_LOG(WARN, "memory not enough for dump file[size %lu B]", dumpFileSize);
        return false;
    }
    size_t dumpTmpSize = EstimateDumpMemoryUse();
    if (!dumpTmpMemReserver->Reserve(dumpTmpSize))
    {
        IE_PREFIX_LOG(WARN, "memory not enough for dump temp memory use[size %lu B]", dumpTmpSize);
        return false;
    }
    return true;
}

void NormalSegmentDumpItem::CleanResource()
{
    mFileSystem->Sync(true);
    mFileSystem->CleanCache();
}



IE_NAMESPACE_END(partition);

