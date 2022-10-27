#include <beeper/beeper.h>
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/sort_build_checker.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/normal/ttl_decoder.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/document/document_rewriter/document_rewriter_chain.h"
#include "indexlib/document/document.h"
#include "indexlib/common/document_deduper_helper.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

OfflinePartitionWriter::OfflinePartitionWriter(
        const IndexPartitionOptions& options, 
        const DumpSegmentContainerPtr& container,
        const FlushedLocatorContainerPtr& flushedLocatorContainer,
        MetricProviderPtr metricProvider,
        const string& partitionName,
        const PartitionRange& range)
    : PartitionWriter(options, partitionName)
    , mReleaseOperationAfterDump(true)
    , mDumpSegmentContainer(container)
    , mFlushedLocatorContainer(flushedLocatorContainer)
    , mMetricProvider(metricProvider)
    , mMemoryUseRatio(MEMORY_USE_RATIO)
    , mDelayDedupDocument(false)
    , mInit(false)
    , mPartitionRange(range)
{
}

OfflinePartitionWriter::~OfflinePartitionWriter() 
{
    if (!mSchema)
    {
        return;
    }

    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv)
    {
        return;
    }
    
    if (IsDirty())
    {
        IE_PREFIX_LOG(ERROR, "Some data still in memory.");
    }

}

void OfflinePartitionWriter::Open(
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData,
        const PartitionModifierPtr& modifier)
{
    IE_PREFIX_LOG(INFO, "OfflinePartitionWriter open begin");
    assert(schema);
    mSchema = schema;
    mDelayDedupDocument = common::DocumentDeduperHelper::DelayDedupDocument(
            mOptions, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    assert(partitionData);
    mPartitionData = partitionData;
    if (mSchema->GetTableType() == tt_kv)
    {
        // for HashTable
        mMemoryUseRatio = 1.0;
    }
    RegisterMetrics(mSchema->GetTableType());
    InitDocumentRewriter(partitionData);
    ReOpenNewSegment(modifier);
    InitCounters(partitionData->GetCounterMap());
    mSortBuildChecker = CreateSortBuildChecker();    
    mTTLDecoder.reset(new TTLDecoder(mSchema));
    //TODO writer_metrics
    IE_PREFIX_LOG(INFO, "OfflinePartitionWriter open end");
}

SortBuildCheckerPtr OfflinePartitionWriter::CreateSortBuildChecker()
{
    PartitionMeta partitionMeta = mPartitionData->GetPartitionMeta();    
    if (mSchema->GetTableType() == tt_kkv)
    {
        return SortBuildCheckerPtr();
    }
    if (!mOptions.IsOnline() && partitionMeta.Size() > 0)
    {
        SortBuildCheckerPtr checker(new SortBuildChecker);
        if (!checker->Init(mSchema->GetAttributeSchema(), partitionMeta))
        {
            INDEXLIB_FATAL_ERROR(InitializeFailed,
                    "Init SortBuildChecker failed!");
        }

        return checker;
    }
    return SortBuildCheckerPtr();    
}

void OfflinePartitionWriter::ReOpenNewSegment(const PartitionModifierPtr& modifier)
{
    ScopedLock lock(mWriterLock);
    InitNewSegment();
    mModifier = modifier;
    mSortBuildChecker = CreateSortBuildChecker();
    mInit = true;
    //TODO writer_metrics
}

bool OfflinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    assert(mTTLDecoder);
    assert(doc->GetMessageCount() == 1u);
    mTTLDecoder->SetDocumentTTL(doc);

    bool ret = false;
    DocOperateType opType = doc->GetDocOperateType();
    switch (opType)
    {
    case ADD_DOC: 
    {
        ret = AddDocument(doc);
        break;
    }
    case DELETE_DOC: 
    case DELETE_SUB_DOC:
    {
        ret = RemoveDocument(doc);
        break;
    }
    case UPDATE_FIELD: 
    {
        // in addToUpdate, docId will be set in Document, so pk lookup phase can be skipped in UpdateDocument 
        // however, some case will reuse document, here set INVALID_DOCID to prevent misjudging addToUpdate scenario
        doc->SetDocId(INVALID_DOCID);
        ret = UpdateDocument(doc);
        break;
    }
    case SKIP_DOC: 
    {
        ret = true;
        break;
    }
    default:
        IE_LOG(WARN, "unknown operation, op type [%d]", opType);
        ERROR_COLLECTOR_LOG(WARN, "unknown operation, op type [%d]", opType);
    }
    mLastConsumedMessageCount = ret ? 1 : 0;
    return ret;
}

bool OfflinePartitionWriter::AddDocument(const DocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetDocOperateType() == ADD_DOC);
    assert(mSegmentWriter);
    
    if (mSortBuildChecker && !mSortBuildChecker->CheckDocument(doc))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "sort build document is not in order");
    }
    
    bool onlineFlushRtIndex = mOptions.IsOnline() &&
                              mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex;
    bool enableAsyncDumpSegment = mOptions.IsOnline() &&
                                  mOptions.GetOnlineConfig().enableAsyncDumpSegment;
    if (mModifier)
    {
        if (!onlineFlushRtIndex && !enableAsyncDumpSegment &&
            mModifier->SupportAutoAdd2Update())
        {
            if (mModifier->TryRewriteAdd2Update(doc))
            {
                return UpdateDocument(doc);
            }
            // no same pk document, no need dedup
        }
        else if (!mDelayDedupDocument)
        {
            mModifier->DedupDocument(doc);
        }
    }
    mInMemorySegment->UpdateSegmentInfo(doc);
    mInMemorySegment->UpdateMemUse();
    return mSegmentWriter->AddDocument(doc) && AddOperation(doc);
}

bool OfflinePartitionWriter::UpdateDocument(const DocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetDocOperateType() == UPDATE_FIELD);

    if (mOptions.IsOnline() && mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex)
    {
        IE_LOG(ERROR, "table [%s] not support UpdateDocument for online build"
               " when onDiskFlushRealtimeIndex = true!",
               mSchema->GetSchemaName().c_str());
        return false;
    }

    if (mOptions.IsOnline() && mOptions.GetOnlineConfig().enableAsyncDumpSegment)
    {
        IE_LOG(ERROR, "table [%s] not support UpdateDocument for online build"
               " when enableAsyncDumpSegment = true!",
               mSchema->GetSchemaName().c_str());
        return false;
    }
    mInMemorySegment->UpdateSegmentInfo(doc);
    mInMemorySegment->UpdateMemUse();
    if (mModifier)
    {
        return mModifier->UpdateDocument(doc) && AddOperation(doc);
    }
    IE_PREFIX_LOG(DEBUG, "UpdateDocument without modifier");
    return false;
}

bool OfflinePartitionWriter::RemoveDocument(const DocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetDocOperateType() == DELETE_DOC ||
           doc->GetDocOperateType() == DELETE_SUB_DOC);

    mInMemorySegment->UpdateSegmentInfo(doc);
    mInMemorySegment->UpdateMemUse();
    if (mModifier)
    {
        return mModifier->RemoveDocument(doc) && AddOperation(doc);
    }
    IE_PREFIX_LOG(DEBUG, "RemoveDocument without modifier");
    return false;
}

bool OfflinePartitionWriter::NeedDump(size_t memoryQuota) const
{
    if (!mSegmentWriter)
    {
        return false;
    }
    //TODO: not consider sub doc count
    if (mSegmentInfo->docCount >= mOptions.GetBuildConfig().maxDocCount)
    {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "DumpSegment for reach doc count limit : "
                "docCount [%lu] over maxDocCount [%u]",
                mSegmentInfo->docCount, mOptions.GetBuildConfig().maxDocCount);
        return true;
    }
    // need force dump
    if (mSegmentWriter->NeedForceDump())
    {
        int64_t latency = 0;
        {
            ScopedTime timer(latency);
            CleanResource();
        }
        IE_PREFIX_LOG(DEBUG, "clean resource elapse %ld ms", latency / 1000);
        BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "DumpSegment for segmentWriter needForceDump.");
        return true;
    }
    // memory
    size_t curSegMaxMemUse = EstimateMaxMemoryUseOfCurrentSegment();
    if (mOptions.IsOnline())
    {
        return curSegMaxMemUse >= memoryQuota;
    }
    // only for offline build below
    if (curSegMaxMemUse >= memoryQuota * mMemoryUseRatio)
    {
        IE_PREFIX_LOG(INFO, "estimate current segment memory use [%lu] is greater than "
                      "memory quota [%lu] multiply by ratio [%lf] ",
                      curSegMaxMemUse, memoryQuota, mMemoryUseRatio);

        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "DumpSegment for memory reach limit: "
                "estimate current segment memory use [%lu] is greater than "
                "memory quota [%lu] multiply by ratio [%lf] ",
                curSegMaxMemUse, memoryQuota, mMemoryUseRatio);
        return true;
    }
    size_t resourceMemUse = GetResourceMemoryUse();
    if (resourceMemUse + curSegMaxMemUse >= memoryQuota)
    {
        int64_t latency = 0;
        {
            ScopedTime timer(latency);
            CleanResource();
        }
        IE_PREFIX_LOG(DEBUG, "clean resource elapse %ld ms", latency / 1000);
    }
    resourceMemUse = GetResourceMemoryUse();
    if (resourceMemUse >= memoryQuota)
    {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "[%s] no memory for segment writer, [memory quota %lu MB],"
                             "[BuildReaderMem %lu MB]", IE_LOG_PREFIX,
                             memoryQuota / (1024 * 1024),
                             resourceMemUse / (1024 * 1024));
    }
    
    if (resourceMemUse + curSegMaxMemUse >= memoryQuota)
    {
        IE_PREFIX_LOG(INFO, "estimate current segment memory use [%lu] and "
                      "resource memory use [%lu] is greater than memory quota [%lu] ",
                      curSegMaxMemUse, resourceMemUse, memoryQuota);
        
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "DumpSegment for memory reach limit: "
                "estimate current segment memory use [%lu] and "
                "resource memory use [%lu] is greater than memory quota [%lu] ",
                curSegMaxMemUse, resourceMemUse, memoryQuota);                
        return true;
    }
    return false;
}

bool OfflinePartitionWriter::IsDirty() const
{
    if (!mSegmentWriter)
    {
        return false;
    }

    if (mSegmentWriter->IsDirty())
    {
        return true;
    }

    if (mModifier && mModifier->IsDirty())
    {
        return true;
    }

    if (mOperationWriter && mOperationWriter->IsDirty())
    {
        return true;
    }
    return false;
}

void OfflinePartitionWriter::RegisterMetrics(TableType tableType)
{
#define INIT_BUILD_METRIC(metric, unit)                                 \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "build/"#metric, kmonitor::GAUGE, unit)

    INIT_BUILD_METRIC(buildMemoryUse, "byte");
    INIT_BUILD_METRIC(segmentWriterMemoryUse, "byte");
    INIT_BUILD_METRIC(currentSegmentMemoryQuotaUse, "byte");
    INIT_BUILD_METRIC(estimateMaxMemoryUseOfCurrentSegment, "byte");
    INIT_BUILD_METRIC(dumpSegmentLatency, "ms");
    
    if (tableType != tt_kv && tableType != tt_kkv)
    {
        INIT_BUILD_METRIC(modifierMemoryUse, "byte");
        INIT_BUILD_METRIC(operationWriterTotalSerializeSize, "byte");
        INIT_BUILD_METRIC(operationWriterTotalDumpSize, "byte");
        INIT_BUILD_METRIC(operationWriterMemoryUse, "byte");
        INIT_BUILD_METRIC(operationWriterMaxOpSerializeSize, "byte");
        IE_INIT_METRIC_GROUP(mMetricProvider, operationWriterTotalOpCount, 
                             "build/operationWriterTotalOpCount", 
                             kmonitor::STATUS, "count");
    }

#undef INIT_BUILD_METRIC
}

void OfflinePartitionWriter::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (!counterMap)
    {
        IE_PREFIX_LOG(ERROR, "init counters failed due to NULL counterMap");
        return;
    }

    string nodePrefix = mOptions.IsOnline() ? "online.build" : "offline.build";
    auto InitCounter = [&counterMap, &nodePrefix] (const string& counterName) {
        string counterPath = nodePrefix + "." + counterName;
        auto accCounter = counterMap->GetAccCounter(counterPath);
        if (!accCounter)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed",
                    counterPath.c_str());
        }
        return accCounter;
    };
    mAddDocCountCounter = InitCounter("addDocCount");
    mUpdateDocCountCounter = InitCounter("updateDocCount");
    mDelDocCountCounter = InitCounter("deleteDocCount");
}

bool OfflinePartitionWriter::CanDumpSegment(
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

bool OfflinePartitionWriter::DumpSegmentWithMemLimit()
{
    if (unlikely(!mInit))
    {
        assert(!mInMemorySegment);
        INDEXLIB_FATAL_ERROR(InitializeFailed, "OfflinePartitionWriter does not initialize");
    }
    IndexlibFileSystemPtr fileSystem = mPartitionData->GetRootDirectory()->GetFileSystem();
    util::MemoryReserverPtr fileSystemMemReserver = fileSystem->CreateMemoryReserver("partition_writer");
    util::MemoryReserverPtr dumpTmpMemReserver = mInMemorySegment->CreateMemoryReserver("partition_writer");
    if (!CanDumpSegment(fileSystemMemReserver, dumpTmpMemReserver))
    {
        return false;
    }
    DumpSegment();
    return true;
}

void OfflinePartitionWriter::DumpSegment()
{
    IE_PREFIX_LOG(INFO, "dump segment begin");
    //ScopedLock lock(mWriterLock); //TODO: need lock
    if (!IsDirty())
    {
        return;
    }

    mPartitionData->UpdatePartitionInfo();
    mSegmentWriter->CollectSegmentMetrics();

    NormalSegmentDumpItemPtr segmentDumpItem(
        new NormalSegmentDumpItem(mOptions, mSchema, mPartitionName));
    
    segmentDumpItem->Init(mdumpSegmentLatencyMetric,
                      mPartitionData,
                      mModifier,
                      mFlushedLocatorContainer,
                      mReleaseOperationAfterDump);

    if (mOptions.IsOnline() && mOptions.GetOnlineConfig().enableAsyncDumpSegment)
    {
        mDumpSegmentContainer->PushDumpItem(segmentDumpItem);
    }
    else
    {
        CleanResource();
        segmentDumpItem->Dump();
        InMemorySegmentPtr inMemSegment = segmentDumpItem->GetInMemorySegment();
        // MAYBE: pass SegmentFileList from temporary in mem directory to avoid load empty SegmentFileList on disk when async flushing
        mPartitionData->AddBuiltSegment(
            inMemSegment->GetSegmentId(), inMemSegment->GetSegmentInfo());
        if (mOptions.IsOnline())
        {
            mPartitionData->CommitVersion();
        }
    }
    Reset();
    IE_PREFIX_LOG(INFO, "dump segment end");
}

void OfflinePartitionWriter::Close()
{
    IE_PREFIX_LOG(INFO, "close OfflinePartitionWriter begin");
    ReportMetrics();
    DumpSegment();
    DedupBuiltSegments();
    Reset();
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    rootDirectory->Sync(true);
    mPartitionData->CommitVersion();
    VersionCommitter::CleanVersions(rootDirectory,
                                    mOptions.GetBuildConfig().keepVersionCount,
                                    mOptions.GetReservedVersions());
    IE_PREFIX_LOG(INFO, "close OfflinePartitionWriter end");
}

void OfflinePartitionWriter::DedupBuiltSegments()
{
    if (!mDelayDedupDocument)
    {
        return;
    }

    mPartitionData->CreateNewSegment();
    ReOpenNewSegment(PartitionModifierCreator::CreateOfflineModifier(
                    mSchema, mPartitionData,
                    true, mOptions.GetBuildConfig().enablePackageFile));
    DocumentDeduperPtr deduper =
        DocumentDeduperCreator::CreateBuiltSegmentsDeduper(
                mSchema, mOptions, mModifier);
    if (deduper)
    {
        deduper->Dedup();
        DumpSegment();
    }
}

size_t OfflinePartitionWriter::GetTotalMemoryUse() const
{
    ScopedLock lock(mWriterLock);
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.GetCurrentMemoryUse();
}

size_t OfflinePartitionWriter::GetResourceMemoryUse() const
{
    size_t memUse = GetFileSystemMemoryUse();
    if (mOptions.IsOffline() && mDumpSegmentContainer->GetInMemSegmentContainer())
    {
        memUse += mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse();
    }
    return memUse;
}

void OfflinePartitionWriter::CleanResource() const
{
    if (mOptions.IsOnline())
    {
        return;
    }
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    IndexlibFileSystemPtr fileSystem = rootDirectory->GetFileSystem();
    fileSystem->Sync(true);
    fileSystem->CleanCache();
    if (mDumpSegmentContainer->GetInMemSegmentContainer())
    {
        mDumpSegmentContainer->GetInMemSegmentContainer()->WaitEmpty();
    }
}

size_t OfflinePartitionWriter::GetFileSystemMemoryUse() const
{
    const DirectoryPtr& directory = mPartitionData->GetRootDirectory();
    const IndexlibFileSystemPtr& fileSystem = directory->GetFileSystem();
    return fileSystem->GetFileSystemMemoryUse();
}

size_t OfflinePartitionWriter::EstimateMaxMemoryUseOfCurrentSegment() const
{
    ScopedLock lock(mWriterLock);    
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.EstimateMaxMemoryUseOfCurrentSegment();
}

size_t OfflinePartitionWriter::EstimateDumpMemoryUse() const
{
    ScopedLock lock(mWriterLock);    
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.EstimateDumpMemoryUse();    
}

size_t OfflinePartitionWriter::EstimateDumpFileSize() const
{
    ScopedLock lock(mWriterLock);    
    PartitionWriterResourceCalculator calculator(mOptions);
    calculator.Init(mSegmentWriter, mModifier, mOperationWriter);
    return calculator.EstimateDumpFileSize();        
}

void OfflinePartitionWriter::InitDocumentRewriter(
        const PartitionDataPtr& partitionData)
{
    mDocRewriteChain.reset(new DocumentRewriterChain(mSchema, mOptions,
                    partitionData->GetPartitionMeta().GetSortDescriptions()));
    mDocRewriteChain->Init();
}

void OfflinePartitionWriter::InitNewSegment()
{
    mInMemorySegment = mPartitionData->CreateNewSegment();
    assert(mInMemorySegment);
    mSegmentWriter = mInMemorySegment->GetSegmentWriter();
    mOperationWriter = DYNAMIC_POINTER_CAST(OperationWriter, mInMemorySegment->GetOperationWriter());
    mSegmentInfo = mSegmentWriter->GetSegmentInfo();
}

void OfflinePartitionWriter::RewriteDocument(const DocumentPtr& doc)
{
    assert(mDocRewriteChain);
    mDocRewriteChain->Rewrite(doc);
}

void OfflinePartitionWriter::CommitVersion()
{
    mPartitionData->CommitVersion();
}

void OfflinePartitionWriter::ReportMetrics() const
{
    ScopedLock lock(mWriterLock);
    size_t segWriterMemUse = 0;
    if (mSegmentWriter)
    {
        segWriterMemUse = mSegmentWriter->GetTotalMemoryUse();
    }
    size_t modifierMemUse = 0;
    if (mModifier)
    {
        const util::BuildResourceMetricsPtr buildMetrics =
            mModifier->GetBuildResourceMetrics();
        assert(buildMetrics);
        modifierMemUse = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE); 
    }
    size_t operationWriterMemUse = 0;
    size_t totalOperationSerializeSize = 0;
    size_t totalOperationDumpSize = 0;
    size_t totalOpCount = 0;
    size_t maxOpSerialzeSize = 0;
    if (mOperationWriter)
    {
        operationWriterMemUse = mOperationWriter->GetTotalMemoryUse();
        totalOperationDumpSize = mOperationWriter->GetDumpSize();
        totalOperationSerializeSize = mOperationWriter->GetTotalOperationSerializeSize();
        totalOpCount = mOperationWriter->GetTotalOperationCount();
        maxOpSerialzeSize = mOperationWriter->GetMaxOperationSerializeSize();
    }
    size_t buildingMemUse = segWriterMemUse + modifierMemUse + operationWriterMemUse;
    if (mOptions.IsOffline())
    {
        buildingMemUse += GetResourceMemoryUse();
    }
    size_t memoryQuotaUse = 0;
    if (mInMemorySegment)
    {
        memoryQuotaUse = mInMemorySegment->GetUsedQuota();
    }
    IE_REPORT_METRIC(estimateMaxMemoryUseOfCurrentSegment, 
                     EstimateMaxMemoryUseOfCurrentSegment());
    IE_REPORT_METRIC(buildMemoryUse, buildingMemUse);
    IE_REPORT_METRIC(segmentWriterMemoryUse, segWriterMemUse);
    IE_REPORT_METRIC(currentSegmentMemoryQuotaUse, memoryQuotaUse);
    IE_REPORT_METRIC(modifierMemoryUse, modifierMemUse);
    IE_REPORT_METRIC(operationWriterMemoryUse, operationWriterMemUse);
    IE_REPORT_METRIC(operationWriterTotalSerializeSize, totalOperationSerializeSize);
    IE_REPORT_METRIC(operationWriterTotalDumpSize, totalOperationDumpSize);
    IE_REPORT_METRIC(operationWriterTotalOpCount, totalOpCount);
    IE_REPORT_METRIC(operationWriterMaxOpSerializeSize, maxOpSerialzeSize);
}

void OfflinePartitionWriter::Reset()
{
    ScopedLock lock(mWriterLock);
    mInit = false;
    mSegmentInfo.reset();
    mSegmentWriter.reset();
    mModifier.reset();
    mOperationWriter.reset();
    mInMemorySegment.reset();
}

void OfflinePartitionWriter::UpdateBuildCounters(DocOperateType op)
{
    switch(op)
    {
    case ADD_DOC: if(mAddDocCountCounter) {mAddDocCountCounter->Increase(1);} break;
    case UPDATE_FIELD: if(mUpdateDocCountCounter) {mUpdateDocCountCounter->Increase(1);} break;
    case DELETE_DOC: if (mDelDocCountCounter) {mDelDocCountCounter->Increase(1);} break;
    default:
        break;
    }
}

bool OfflinePartitionWriter::AddOperation(const DocumentPtr& doc)
{
    UpdateBuildCounters(doc->GetDocOperateType());    
    if (!mOperationWriter)
    {
        assert(mOptions.IsOffline() || 
               mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv);
        return true;
    }
    return mOperationWriter->AddOperation(doc);
}

bool OfflinePartitionWriter::NeedRewriteDocument(const document::DocumentPtr& doc)
{
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv)
    {
        return true;
    }
    return doc->GetDocOperateType() == ADD_DOC;
}

misc::Status OfflinePartitionWriter::GetStatus() const 
{
    return mSegmentWriter->GetStatus();
}


#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);

