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
#include "indexlib/partition/offline_partition_writer.h"

#include "beeper/beeper.h"
#include "indexlib/config/document_deduper_helper.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/document/document_rewriter/document_rewriter_chain.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/ttl_decoder.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/util/build_profiling_metrics.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/partition/doc_build_work_item_generator.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/main_sub_doc_id_manager.h"
#include "indexlib/partition/main_sub_util.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/normal_doc_id_manager.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/sort_build_checker.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/GroupedThreadPool.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/memory_control/WaitMemoryQuotaController.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

OfflinePartitionWriter::OfflinePartitionWriter(const IndexPartitionOptions& options,
                                               const DumpSegmentContainerPtr& container,
                                               const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                               MetricProviderPtr metricProvider, const string& partitionName,
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
    , mFixedCostMemorySize(0)
    , mEnableAsyncDump(false)
{
    mBuildingSegmentInfo.first = INVALID_SEGMENTID;
    mBuildingSegmentInfo.second = 0;
}

OfflinePartitionWriter::~OfflinePartitionWriter()
{
    if (!mSchema) {
        return;
    }

    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
        return;
    }

    if (IsDirty()) {
        IE_PREFIX_LOG(ERROR, "Some data still in memory.");
    }
}

void OfflinePartitionWriter::Open(const IndexPartitionSchemaPtr& schema, const PartitionDataPtr& partitionData,
                                  const PartitionModifierPtr& modifier)
{
    IE_PREFIX_LOG(INFO, "OfflinePartitionWriter open begin");
    assert(schema);
    mSchema = schema;

    mDelayDedupDocument =
        DocumentDeduperHelper::DelayDedupDocument(mOptions, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    assert(partitionData);
    mPartitionData = partitionData;
    mFixedCostMemorySize = mOptions.IsOnline() ? 0 : GetFileSystemMemoryUse();
    _docCollectorMemController.reset(new util::WaitMemoryQuotaController(
        mOptions.GetBuildConfig().GetBatchBuildMaxCollectMemoryInM() * 1024 * 1024));

    if (mSchema->GetTableType() == tt_kv) {
        // for HashTable
        mMemoryUseRatio = 1.0;
    }

    mEnableAsyncDump = false;
    if (mOptions.GetOnlineConfig().enableAsyncDumpSegment || mOptions.GetBuildConfig(true).EnableAsyncDump()) {
        if (mOptions.IsOnline()) {
            mEnableAsyncDump = true;
        } else {
            IE_LOG(WARN, "offline not support asyncdump");
        }
    }
    InitDocumentRewriter(partitionData);
    ReOpenNewSegment(modifier);
    RegisterMetrics(mSchema->GetTableType());

    InitCounters(partitionData->GetCounterMap());
    mSortBuildChecker = CreateSortBuildChecker();
    mTTLDecoder.reset(new TTLDecoder(mSchema));
    // TODO writer_metrics
    IE_PREFIX_LOG(INFO, "OfflinePartitionWriter open end");
}

SortBuildCheckerPtr OfflinePartitionWriter::CreateSortBuildChecker()
{
    PartitionMeta partitionMeta = mPartitionData->GetPartitionMeta();
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv) {
        return SortBuildCheckerPtr();
    }
    if (!mOptions.IsOnline() && partitionMeta.Size() > 0) {
        SortBuildCheckerPtr checker(new SortBuildChecker);
        if (!checker->Init(mSchema->GetAttributeSchema(), partitionMeta)) {
            INDEXLIB_FATAL_ERROR(InitializeFailed, "Init SortBuildChecker failed!");
        }

        return checker;
    }
    return SortBuildCheckerPtr();
}

BuildProfilingMetricsPtr OfflinePartitionWriter::CreateBuildProfilingMetrics()
{
    BuildProfilingMetricsPtr metrics(new BuildProfilingMetrics);
    if (!metrics->Init(mSchema, mMetricProvider)) {
        return BuildProfilingMetricsPtr();
    }
    return metrics;
}

bool OfflinePartitionWriter::CheckFixedCostMemorySize(int64_t fixedCostMemorySize)
{
    if (mOptions.IsOnline()) {
        return true;
    }

    auto buildResourceMemLimit = mOptions.GetBuildConfig().buildResourceMemoryLimit * 1024 * 1024;
    if (buildResourceMemLimit > 0 && buildResourceMemLimit < fixedCostMemorySize) {
        return false;
    }
    return true;
}

void OfflinePartitionWriter::ReinitDocIdManager()
{
    if (!mModifier || mSchema->GetTableType() != tt_index) {
        _docIdManager = nullptr;
        return;
    }

    if (!_docIdManager) {
        if (mSchema->GetSubIndexPartitionSchema()) {
            _docIdManager.reset(new MainSubDocIdManager(mSchema));
        } else {
            bool onlineFlushRtIndex = mOptions.IsOnline() && mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex;
            // Attention: 这里必须由 mModifier->SupportAutoAdd2Update() 判断后传入，而不是 docIdManager 自己根据 _schema
            // 判断。因为 Online 时，二者的 schema 不同，分别对应 OnlinePartition 的 mSchema 和 mRtSchema，后者多了
            // virtual_timestamp_index。 See ut: IndexBuilderInteTest.TestAddOverLengthPackAttribute
            bool enableAutoAdd2Update = (!onlineFlushRtIndex) && mModifier->SupportAutoAdd2Update();
            _docIdManager.reset(new NormalDocIdManager(mSchema, enableAutoAdd2Update));
        }
        switch (_buildMode) {
        case PartitionWriter::BUILD_MODE_STREAM:
            _buildThreadPool = nullptr;
            break;
        case PartitionWriter::BUILD_MODE_CONSISTENT_BATCH:
            _buildThreadPool = std::make_unique<util::GroupedThreadPool>();
            _buildThreadPool->Start("parallel-batch-build", mOptions.GetBuildConfig().GetBatchBuildMaxThreadCount(true),
                                    65536, 128);
            _buildThreadPool->GetThreadPool()->start();
            break;
        case PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH:
            _buildThreadPool = std::make_unique<util::GroupedThreadPool>();
            _buildThreadPool->Start("parallel-batch-build",
                                    mOptions.GetBuildConfig().GetBatchBuildMaxThreadCount(false), 65536, 128);
            _buildThreadPool->GetThreadPool()->start();
            break;
        default:
            assert(false);
        }
    }
    _docIdManager->Reinit(mPartitionData, mModifier, mSegmentWriter, _buildMode, mDelayDedupDocument);
}

void OfflinePartitionWriter::SwitchBuildMode(PartitionWriter::BuildMode buildMode)
{
    EndIndex();
    _docIdManager.reset();
    IE_LOG(INFO, "Switch build mode [%s] => [%s]", PartitionWriter::BuildModeToString(_buildMode).c_str(),
           PartitionWriter::BuildModeToString(buildMode).c_str());
    _buildMode = buildMode;
}

void OfflinePartitionWriter::ReOpenNewSegment(const PartitionModifierPtr& modifier)
{
    ScopedLock lock(mWriterLock);
    EndIndex();
    if (!CheckFixedCostMemorySize(mFixedCostMemorySize)) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "fixed memory (pk) [%ld] exceed memory use limit [%ld], "
                             "please increase build_total_memory",
                             mFixedCostMemorySize, mOptions.GetBuildConfig().buildResourceMemoryLimit * 1024 * 1024);
    }
    InitNewSegment();
    _docIdManager.reset();
    mModifier = modifier;
    mSortBuildChecker = CreateSortBuildChecker();
    mInit = true;
    // TODO writer_metrics
}

void OfflinePartitionWriter::ReportBuildDocumentMetrics(const document::DocumentPtr& doc, uint32_t successMsgCount)
{
    // _buildDocumentMetrics should not be nullptr in prod, only in some UT it might be nullptr.
    if (_buildDocumentMetrics != nullptr) {
        _buildDocumentMetrics->ReportBuildDocumentMetrics(doc, successMsgCount);
    }
}

bool OfflinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    ScopeProfilingDocumentReporter profiler(doc, mBuildProfilingMetrics);

    ReinitDocIdManager();
    if (!PreprocessDocument(doc)) {
        ReportBuildDocumentMetrics(doc, /*successMsgCount=*/0);
        return false;
    }

    bool ret = false;
    DocOperateType opType = doc->GetDocOperateType();
    switch (opType) {
    case ADD_DOC: {
        ret = AddDocument(doc);
        break;
    }
    case DELETE_DOC:
    case DELETE_SUB_DOC: {
        ret = RemoveDocument(doc);
        break;
    }
    case UPDATE_FIELD: {
        ret = UpdateDocument(doc);
        break;
    }
    case SKIP_DOC: {
        ret = true;
        break;
    }
    case CHECKPOINT_DOC: {
        ret = true;
        break;
    }
    default:
        IE_LOG(WARN, "unknown operation, op type [%d]", opType);
        ERROR_COLLECTOR_LOG(WARN, "unknown operation, op type [%d]", opType);
    }
    ReportBuildDocumentMetrics(doc, /*successMsgCount=*/ret ? doc->GetDocumentCount() : 0);

    mLastConsumedMessageCount = ret ? doc->GetDocumentCount() : 0;
    return ret;
}

bool OfflinePartitionWriter::BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector)
{
    assert(_buildMode == BUILD_MODE_CONSISTENT_BATCH || _buildMode == BUILD_MODE_INCONSISTENT_BATCH);
    int64_t startTimeInMs = autil::TimeUtility::currentTimeInMicroSeconds() / 1000;
    IE_LOG(INFO, "Begin batch build [%ld] docs", docCollector->Size());
    ReinitDocIdManager();
    assert(_buildThreadPool != nullptr);
    PreprocessDocuments(docCollector.get());

    // PreprocessDocuments 可能添加字段，在Allocate之后内存不会再变化了
    int64_t docCollectorMemUse = docCollector->EstimateMemory();
    while (!_docCollectorMemController->Allocate(docCollectorMemUse)) {
        IE_LOG(WARN, "collect_doc_memory_quota not enough for one batch, will expand it [%.1f]M => [%.1f]M",
               _docCollectorMemController->GetTotalQuota() / 1024.0 / 1024, docCollectorMemUse / 1024.0 / 1024);
        _docCollectorMemController->AddQuota(docCollectorMemUse - _docCollectorMemController->GetTotalQuota());
    }

    std::shared_ptr<document::DocumentCollector> sharedDocCollector(docCollector.release());
    sharedDocCollector->RemoveNullDocuments();

    _buildThreadPool->StartNewBatch();
    // 索引构建任务
    _buildThreadPool->PushTask("__ADD_OP__", [this, sharedDocCollector]() {
        for (const DocumentPtr& doc : sharedDocCollector->GetDocuments()) {
            AddOperation(doc);
        }
    });
    DocBuildWorkItemGenerator::Generate(_buildMode, mSchema, mSegmentWriter, mModifier, sharedDocCollector,
                                        _buildThreadPool.get());

    // 通过 hook 并行析构参与 build 的 docs，确保不会干预 build 任务
    size_t destructDocParallelNum = sharedDocCollector->GetSuggestDestructParallelNum(_buildThreadPool->GetThreadNum());
    assert(destructDocParallelNum <= _buildThreadPool->GetThreadNum());
    for (int32_t parallelIdx = 0; parallelIdx < destructDocParallelNum; ++parallelIdx) {
        _buildThreadPool->AddBatchFinishHook(
            [this, destructDocParallelNum, parallelIdx, sharedDocCollector, docCollectorMemUse]() {
                sharedDocCollector->DestructDocumentsForParallel(destructDocParallelNum, parallelIdx);
                // 按照平均值来释放。因为doc的实际内存估计值（理论上）可能变化，而我们必须强保证Allocate()和Free()的总和相等。
                // 但我们可以接受少量估计偏差。另外，doc的内存估计还是有一定性能开销的
                _docCollectorMemController->Free((docCollectorMemUse + parallelIdx) / destructDocParallelNum);
            });
    };
    if (_buildMode == BUILD_MODE_CONSISTENT_BATCH) {
        _buildThreadPool->WaitCurrentBatchWorkItemsFinish();
    }

    IE_LOG(INFO, "End generation workitems for batch build [%ld] docs, use [%ld] ms", sharedDocCollector->Size(),
           autil::TimeUtility::currentTimeInMicroSeconds() / 1000 - startTimeInMs);

    // TODO(panghai.hj): Remove mLastConsumedMessageCount. mLastConsumedMessageCount is used to report successful
    // build document metrics.
    mLastConsumedMessageCount = 0;
    return true;
}

bool OfflinePartitionWriter::PreprocessDocument(const DocumentPtr& doc)
{
    if (doc == nullptr) {
        IE_LOG(WARN, "empty document to build");
        ERROR_COLLECTOR_LOG(WARN, "empty document to build");
        return false;
    }
    if (mSchema->GetTableType() == tt_index) {
        document::NormalDocumentPtr normalDoc = std::dynamic_pointer_cast<document::NormalDocument>(doc);
        if (normalDoc and normalDoc->GetSchemaVersionId() != mSchema->GetSchemaVersionId()) {
            IE_INTERVAL_LOG2(10, INFO, "doc schema version [%d] not match with schema version [%d]",
                             normalDoc->GetSchemaVersionId(), mSchema->GetSchemaVersionId());
            _buildDocumentMetrics->ReportSchemaVersionMismatch();
        }
    }

    assert(mTTLDecoder);
    mTTLDecoder->SetDocumentTTL(doc);

    // For ADD, we need to delete the original document if exists and then add the new document.
    // TODO: Put this logic in a more appropriate place.
    if (!Validate(doc)) {
        return false;
    }

    mInMemorySegment->UpdateSegmentInfo(doc);
    mInMemorySegment->UpdateMemUse();

    if (_docIdManager && !_docIdManager->Process(doc)) {
        return false;
    }
    return true;
}

void OfflinePartitionWriter::PreprocessDocuments(document::DocumentCollector* docCollector)
{
    const std::vector<document::DocumentPtr>& docs = docCollector->GetDocuments();
    int failedCount = 0;
    for (size_t i = 0; i < docs.size(); ++i) {
        if (PreprocessDocument(docs[i])) {
            // We assume no failure after preprocess, thus we collect build document metrics here.
            ReportBuildDocumentMetrics(docs[i], /*successMsgCount=*/docs[i]->GetDocumentCount());
        } else {
            failedCount++;
            ReportBuildDocumentMetrics(docs[i], /*successMsgCount=*/0);
            docCollector->LogicalDelete(i);
        }
    }
    IE_LOG(INFO, "batch build preprocess [%lu] documents, [%d] failed", docs.size(), failedCount);
}

bool OfflinePartitionWriter::AddDocument(const DocumentPtr& doc)
{
    bool ret = mSegmentWriter->AddDocument(doc) && AddOperation(doc);
    if (unlikely(_docIdManager && !ret)) {
        assert(false);
        // docIdManager 应该处理完所有前置检查，tt_index 不应该出现构建失败
        INDEXLIB_FATAL_ERROR(InconsistentState, "Build doc [%s] failed with docIdManager",
                             doc->GetPrimaryKey().c_str());
    }
    return ret;
}

bool OfflinePartitionWriter::UpdateDocument(const DocumentPtr& doc)
{
    assert(doc->GetDocOperateType() == UPDATE_FIELD);
    return mModifier->UpdateDocument(doc) && AddOperation(doc);
}

bool OfflinePartitionWriter::RemoveDocument(const DocumentPtr& doc)
{
    assert(doc->GetDocOperateType() == DELETE_DOC || doc->GetDocOperateType() == DELETE_SUB_DOC);
    return mModifier->RemoveDocument(doc) && AddOperation(doc);
}

bool OfflinePartitionWriter::NeedDump(size_t memoryQuota, const DocumentCollector* docCollector) const
{
    if (!mSegmentWriter) {
        return false;
    }
    // TODO: not consider sub doc count
    if (mSegmentInfo->docCount + (docCollector ? docCollector->Size() : 0) >= mOptions.GetBuildConfig().maxDocCount) {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "DumpSegment for reach doc count limit : "
                                          "docCount [%lu] over maxDocCount [%u]",
                                          mSegmentInfo->docCount, mOptions.GetBuildConfig().maxDocCount);
        return true;
    }
    // need force dump
    if (mSegmentWriter->NeedForceDump()) {
        int64_t latency = 0;
        {
            ScopedTime timer(latency);
            CleanResource();
        }
        IE_PREFIX_LOG(DEBUG, "clean resource elapse %ld ms", latency / 1000);
        BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "DumpSegment for segmentWriter needForceDump.");
        return true;
    }
    // memory
    size_t curSegMaxMemUse = EstimateMaxMemoryUseOfCurrentSegment();
    if (mOptions.IsOnline()) {
        return curSegMaxMemUse >= memoryQuota;
    }
    // only for offline build below
    if (curSegMaxMemUse >= memoryQuota * mMemoryUseRatio) {
        IE_PREFIX_LOG(INFO,
                      "estimate current segment memory use [%lu] is greater than "
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
    if (resourceMemUse + curSegMaxMemUse >= memoryQuota) {
        int64_t latency = 0;
        {
            ScopedTime timer(latency);
            CleanResource();
        }
        IE_PREFIX_LOG(DEBUG, "clean resource elapse %ld ms", latency / 1000);
    }
    resourceMemUse = GetResourceMemoryUse();
    if (resourceMemUse >= memoryQuota) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "[%s] no memory for segment writer, [memory quota %lu MB],"
                             "[BuildReaderMem %lu MB]",
                             IE_LOG_PREFIX, memoryQuota / (1024 * 1024), resourceMemUse / (1024 * 1024));
    }

    if (resourceMemUse + curSegMaxMemUse >= memoryQuota) {
        IE_PREFIX_LOG(INFO,
                      "estimate current segment memory use [%lu] and "
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
    if (!mSegmentWriter) {
        return false;
    }

    if (mSegmentWriter->IsDirty()) {
        return true;
    }
    if (mModifier && mModifier->IsDirty()) {
        return true;
    }

    if (mOperationWriter && mOperationWriter->IsDirty()) {
        return true;
    }
    return false;
}

void OfflinePartitionWriter::RegisterMetrics(TableType tableType)
{
#define INIT_BUILD_METRIC(metric, unit)                                                                                \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "build/" #metric, kmonitor::GAUGE, unit)

    INIT_BUILD_METRIC(buildMemoryUse, "byte");
    INIT_BUILD_METRIC(segmentWriterMemoryUse, "byte");
    INIT_BUILD_METRIC(currentSegmentMemoryQuotaUse, "byte");
    INIT_BUILD_METRIC(estimateMaxMemoryUseOfCurrentSegment, "byte");
    INIT_BUILD_METRIC(dumpSegmentLatency, "ms");
    INIT_BUILD_METRIC(segmentBuildingTime, "us");

    if (tableType != tt_kv && tableType != tt_kkv) {
        INIT_BUILD_METRIC(modifierMemoryUse, "byte");
        INIT_BUILD_METRIC(docCollectorMemoryUse, "byte");
        INIT_BUILD_METRIC(operationWriterTotalSerializeSize, "byte");
        INIT_BUILD_METRIC(operationWriterTotalDumpSize, "byte");
        INIT_BUILD_METRIC(operationWriterMemoryUse, "byte");
        INIT_BUILD_METRIC(operationWriterMaxOpSerializeSize, "byte");
        IE_INIT_METRIC_GROUP(mMetricProvider, operationWriterTotalOpCount, "build/operationWriterTotalOpCount",
                             kmonitor::STATUS, "count");
    }

#undef INIT_BUILD_METRIC

    _buildDocumentMetrics = std::make_unique<BuildDocumentMetrics>(mMetricProvider, mSchema->GetTableType());
    _buildDocumentMetrics->RegisterMetrics();
}

void OfflinePartitionWriter::InitCounters(const util::CounterMapPtr& counterMap)
{
    if (!counterMap) {
        IE_PREFIX_LOG(ERROR, "init counters failed due to NULL counterMap");
        return;
    }

    string nodePrefix = mOptions.IsOnline() ? "online.build" : "offline.build";
    auto InitCounter = [&counterMap, &nodePrefix](const string& counterName) {
        string counterPath = nodePrefix + "." + counterName;
        auto accCounter = counterMap->GetAccCounter(counterPath);
        if (!accCounter) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
        }
        return accCounter;
    };
    mAddDocCountCounter = InitCounter("addDocCount");
    mUpdateDocCountCounter = InitCounter("updateDocCount");
    mDelDocCountCounter = InitCounter("deleteDocCount");
}

bool OfflinePartitionWriter::CanDumpSegment(const util::MemoryReserverPtr& fileSystemMemReserver,
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

void OfflinePartitionWriter::EndIndex()
{
    if (_buildThreadPool) {
        _buildThreadPool->WaitFinish();
    }
}

bool OfflinePartitionWriter::DumpSegmentWithMemLimit()
{
    EndIndex();
    if (unlikely(!mInit)) {
        assert(!mInMemorySegment);
        INDEXLIB_FATAL_ERROR(InitializeFailed, "OfflinePartitionWriter does not initialize");
    }
    IFileSystemPtr fileSystem = mPartitionData->GetRootDirectory()->GetFileSystem();
    util::MemoryReserverPtr fileSystemMemReserver = fileSystem->CreateMemoryReserver("partition_writer");
    util::MemoryReserverPtr dumpTmpMemReserver = mInMemorySegment->CreateMemoryReserver("partition_writer");
    if (!CanDumpSegment(fileSystemMemReserver, dumpTmpMemReserver)) {
        return false;
    }
    DumpSegment();
    return true;
}

void OfflinePartitionWriter::DumpSegment()
{
    EndIndex();
    IE_PREFIX_LOG(INFO, "dump segment begin");
    // ScopedLock lock(mWriterLock); //TODO: need lock
    if (!IsDirty()) {
        return;
    }

    mPartitionData->UpdatePartitionInfo();
    mSegmentWriter->CollectSegmentMetrics();

    do {
        bool delayDedupDocument =
            DocumentDeduperHelper::DelayDedupDocument(mOptions, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        if (!delayDedupDocument) {
            break;
        }

        DocumentDeduperPtr deduper =
            DocumentDeduperCreator::CreateBuildingSegmentDeduper(mSchema, mOptions, mInMemorySegment, mModifier);
        if (deduper) {
            deduper->Dedup();
        }
    } while (0);

    PartitionModifierDumpTaskItemPtr partitionModifierDumpTaskItem = nullptr;
    if (mModifier != nullptr) {
        partitionModifierDumpTaskItem = mModifier->GetDumpTaskItem(mModifier);
    }
    NormalSegmentDumpItemPtr segmentDumpItem(new NormalSegmentDumpItem(mOptions, mSchema, mPartitionName));

    segmentDumpItem->Init(mdumpSegmentLatencyMetric, mPartitionData, partitionModifierDumpTaskItem,
                          mFlushedLocatorContainer, mReleaseOperationAfterDump);
    segmentDumpItem->GetInMemorySegment()->EndSegment();

    if (mEnableAsyncDump) {
        mDumpSegmentContainer->PushDumpItem(segmentDumpItem);
    } else {
        CleanResource();
        segmentDumpItem->Dump();
        InMemorySegmentPtr inMemSegment = segmentDumpItem->GetInMemorySegment();
        // MAYBE: pass SegmentFileList from temporary in mem directory to avoid load empty SegmentFileList on disk
        // when async flushing
        mPartitionData->AddBuiltSegment(inMemSegment->GetSegmentId(), inMemSegment->GetSegmentInfo());
        if (mOptions.IsOffline() && mSchema->EnableTemperatureLayer()) {
            SegmentTemperatureMeta metaInfo;
            if (!mSegmentWriter->GetSegmentTemperatureMeta(metaInfo)) {
                INDEXLIB_FATAL_ERROR(Runtime, "offline segment writer get temperature meta failed");
            }
            mPartitionData->AddSegmentTemperatureMeta(metaInfo);
        }
        if (mOptions.IsOnline()) {
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
    if (_buildThreadPool) {
        _buildThreadPool->Stop();
    }
    DumpSegment();
    DedupBuiltSegments();
    Reset();
    mPartitionData->ResetInMemorySegment();
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    rootDirectory->Sync(true);
    // TODO(xingwo) re-impl clean version with entryTable
    mPartitionData->CommitVersion();

    if (mOptions.IsOnline()) {
        IE_LOG(INFO, "Online coustomPartitionWriter will clean versions");
        file_system::DirectoryPtr dir = file_system::Directory::GetPhysicalDirectory(rootDirectory->GetOutputPath());
        VersionCommitter::CleanVersions(dir, mOptions.GetBuildConfig().keepVersionCount,
                                        mOptions.GetReservedVersions());
    }
    IE_PREFIX_LOG(INFO, "close OfflinePartitionWriter end");
}

void OfflinePartitionWriter::DedupBuiltSegments()
{
    if (!mDelayDedupDocument) {
        return;
    }

    mPartitionData->CreateNewSegment();
    ReOpenNewSegment(PartitionModifierCreator::CreateOfflineModifier(mSchema, mPartitionData, true,
                                                                     mOptions.GetBuildConfig().enablePackageFile));
    DocumentDeduperPtr deduper = DocumentDeduperCreator::CreateBuiltSegmentsDeduper(mSchema, mOptions, mModifier);
    if (deduper) {
        deduper->Dedup();
        DumpSegment();
    }
}

size_t OfflinePartitionWriter::GetTotalMemoryUse() const
{
    ScopedLock lock(mWriterLock);
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mModifier != nullptr) {
        buildResourceMetrics = mModifier->GetBuildResourceMetrics();
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.GetCurrentMemoryUse() + _docCollectorMemController->GetUsedQuota();
}

size_t OfflinePartitionWriter::GetResourceMemoryUse() const
{
    size_t memUse = GetFileSystemMemoryUse();
    if (mOptions.IsOffline() && mDumpSegmentContainer->GetInMemSegmentContainer()) {
        memUse += mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse();
    }
    return memUse;
}

void OfflinePartitionWriter::CleanResource() const
{
    if (mOptions.IsOnline()) {
        return;
    }
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    IFileSystemPtr fileSystem = rootDirectory->GetFileSystem();
    fileSystem->Sync(true).GetOrThrow();
    fileSystem->CleanCache();
    mFixedCostMemorySize = mOptions.IsOnline() ? 0 : GetFileSystemMemoryUse();
    if (mDumpSegmentContainer->GetInMemSegmentContainer()) {
        mDumpSegmentContainer->GetInMemSegmentContainer()->WaitEmpty();
    }
}

size_t OfflinePartitionWriter::GetFileSystemMemoryUse() const
{
    const DirectoryPtr& directory = mPartitionData->GetRootDirectory();
    const IFileSystemPtr& fileSystem = directory->GetFileSystem();
    return fileSystem->GetFileSystemMemoryUse();
}

size_t OfflinePartitionWriter::EstimateMaxMemoryUseOfCurrentSegment() const
{
    ScopedLock lock(mWriterLock);
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mModifier != nullptr) {
        buildResourceMetrics = mModifier->GetBuildResourceMetrics();
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.EstimateMaxMemoryUseOfCurrentSegment();
}

size_t OfflinePartitionWriter::EstimateDumpMemoryUse() const
{
    ScopedLock lock(mWriterLock);
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mModifier != nullptr) {
        buildResourceMetrics = mModifier->GetBuildResourceMetrics();
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.EstimateDumpMemoryUse();
}

size_t OfflinePartitionWriter::EstimateDumpFileSize() const
{
    ScopedLock lock(mWriterLock);
    PartitionWriterResourceCalculator calculator(mOptions);
    util::BuildResourceMetricsPtr buildResourceMetrics = nullptr;
    if (mModifier != nullptr) {
        buildResourceMetrics = mModifier->GetBuildResourceMetrics();
    }
    calculator.Init(mSegmentWriter, buildResourceMetrics, mOperationWriter);
    return calculator.EstimateDumpFileSize();
}

void OfflinePartitionWriter::InitDocumentRewriter(const PartitionDataPtr& partitionData)
{
    mDocRewriteChain.reset(
        new DocumentRewriterChain(mSchema, mOptions, partitionData->GetPartitionMeta().GetSortDescriptions()));
    mDocRewriteChain->Init();
}

void OfflinePartitionWriter::InitNewSegment()
{
    mInMemorySegment = mPartitionData->CreateNewSegment();
    assert(mInMemorySegment);
    mSegmentWriter = mInMemorySegment->GetSegmentWriter();
    mOperationWriter = DYNAMIC_POINTER_CAST(OperationWriter, mInMemorySegment->GetOperationWriter());
    mSegmentInfo = mSegmentWriter->GetSegmentInfo();

    mBuildProfilingMetrics = CreateBuildProfilingMetrics();

    SingleSegmentWriterPtr singleSegWriter = DYNAMIC_POINTER_CAST(SingleSegmentWriter, mSegmentWriter);
    if (singleSegWriter) {
        singleSegWriter->SetBuildProfilingMetrics(mBuildProfilingMetrics);
    }

    if (mInMemorySegment->GetSegmentId() != mBuildingSegmentInfo.first) {
        mBuildingSegmentInfo.first = mInMemorySegment->GetSegmentId();
        mBuildingSegmentInfo.second = autil::TimeUtility::currentTimeInMicroSeconds();
    }
}

void OfflinePartitionWriter::RewriteDocument(const DocumentPtr& doc)
{
    assert(mDocRewriteChain);
    mDocRewriteChain->Rewrite(doc);
}

void OfflinePartitionWriter::CommitVersion() { mPartitionData->CommitVersion(); }

void OfflinePartitionWriter::ReportMetrics() const
{
    ScopedLock lock(mWriterLock);

    kmonitor::MetricsTags tags;
    tags.AddTag("schemaName", mSchema->GetSchemaName());
    tags.AddTag("segmentId", autil::StringUtil::toString(mBuildingSegmentInfo.first));

    size_t segWriterMemUse = 0;
    if (mSegmentWriter) {
        segWriterMemUse = mSegmentWriter->GetTotalMemoryUse();
    }
    size_t modifierMemUse = 0;
    if (mModifier) {
        const util::BuildResourceMetricsPtr buildMetrics = mModifier->GetBuildResourceMetrics();
        assert(buildMetrics);
        modifierMemUse = buildMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
    }
    size_t operationWriterMemUse = 0;
    size_t totalOperationSerializeSize = 0;
    size_t totalOperationDumpSize = 0;
    size_t totalOpCount = 0;
    size_t maxOpSerialzeSize = 0;
    if (mOperationWriter) {
        operationWriterMemUse = mOperationWriter->GetTotalMemoryUse();
        totalOperationDumpSize = mOperationWriter->GetDumpSize();
        totalOperationSerializeSize = mOperationWriter->GetTotalOperationSerializeSize();
        totalOpCount = mOperationWriter->GetTotalOperationCount();
        maxOpSerialzeSize = mOperationWriter->GetMaxOperationSerializeSize();
    }
    size_t buildingMemUse = segWriterMemUse + modifierMemUse + operationWriterMemUse;
    if (mOptions.IsOffline()) {
        buildingMemUse += GetResourceMemoryUse();
    }
    size_t memoryQuotaUse = 0;
    if (mInMemorySegment) {
        memoryQuotaUse = mInMemorySegment->GetUsedQuota();
    }
    IE_REPORT_METRIC_WITH_TAGS(estimateMaxMemoryUseOfCurrentSegment, &tags, EstimateMaxMemoryUseOfCurrentSegment());
    IE_REPORT_METRIC_WITH_TAGS(buildMemoryUse, &tags, buildingMemUse);
    IE_REPORT_METRIC_WITH_TAGS(segmentWriterMemoryUse, &tags, segWriterMemUse);
    IE_REPORT_METRIC_WITH_TAGS(currentSegmentMemoryQuotaUse, &tags, memoryQuotaUse);
    IE_REPORT_METRIC_WITH_TAGS(modifierMemoryUse, &tags, modifierMemUse);
    IE_REPORT_METRIC_WITH_TAGS(docCollectorMemoryUse, &tags, _docCollectorMemController->GetUsedQuota());
    IE_REPORT_METRIC_WITH_TAGS(operationWriterMemoryUse, &tags, operationWriterMemUse);
    IE_REPORT_METRIC_WITH_TAGS(operationWriterTotalSerializeSize, &tags, totalOperationSerializeSize);
    IE_REPORT_METRIC_WITH_TAGS(operationWriterTotalDumpSize, &tags, totalOperationDumpSize);
    IE_REPORT_METRIC_WITH_TAGS(operationWriterTotalOpCount, &tags, totalOpCount);
    IE_REPORT_METRIC_WITH_TAGS(operationWriterMaxOpSerializeSize, &tags, maxOpSerialzeSize);

    if (mBuildingSegmentInfo.second > 0) {
        int64_t buildTime = autil::TimeUtility::currentTimeInMicroSeconds() - mBuildingSegmentInfo.second;
        IE_REPORT_METRIC_WITH_TAGS(segmentBuildingTime, &tags, buildTime);
    }
}

void OfflinePartitionWriter::Reset()
{
    ScopedLock lock(mWriterLock);
    mInit = false;
    mSegmentInfo.reset();
    mSegmentWriter.reset();
    _docIdManager.reset();
    mModifier.reset();
    mOperationWriter.reset();
    mInMemorySegment.reset();
}

void OfflinePartitionWriter::UpdateBuildCounters(DocOperateType op)
{
    switch (op) {
    case ADD_DOC:
        if (mAddDocCountCounter) {
            mAddDocCountCounter->Increase(1);
        }
        break;
    case UPDATE_FIELD:
        if (mUpdateDocCountCounter) {
            mUpdateDocCountCounter->Increase(1);
        }
        break;
    case DELETE_DOC:
        if (mDelDocCountCounter) {
            mDelDocCountCounter->Increase(1);
        }
        break;
    default:
        break;
    }
}

bool OfflinePartitionWriter::AddOperation(const DocumentPtr& doc)
{
    UpdateBuildCounters(doc->GetDocOperateType());
    if (!mOperationWriter) {
        assert(mOptions.IsOffline() || mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv);
        return true;
    }
    return mOperationWriter->AddOperation(doc);
}

bool OfflinePartitionWriter::NeedRewriteDocument(const document::DocumentPtr& doc)
{
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv) {
        return false;
    }
    return doc->GetDocOperateType() == ADD_DOC;
}

util::Status OfflinePartitionWriter::GetStatus() const { return mSegmentWriter->GetStatus(); }

bool OfflinePartitionWriter::Validate(const document::DocumentPtr& doc)
{
    assert(doc);

    DocOperateType opType = doc->GetDocOperateType();
    switch (opType) {
    case ADD_DOC: {
        // Return true if the doc is valid or can be corrected. Return false is the doc is invalid.
        assert(mSegmentWriter);
        if (mSortBuildChecker && !mSortBuildChecker->CheckDocument(doc)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "sort build document is not in order");
        }
        return true;
    }
    case DELETE_DOC:
    case DELETE_SUB_DOC: {
        if (mModifier == nullptr) {
            IE_PREFIX_LOG(DEBUG, "RemoveDocument without modifier");
            return false;
        }
        return true;
    }
    case UPDATE_FIELD: {
        if (mOptions.IsOnline() && mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex) {
            IE_LOG(ERROR,
                   "table [%s] not support UpdateDocument for online build"
                   " when onDiskFlushRealtimeIndex = true!",
                   mSchema->GetSchemaName().c_str());
            return false;
        }
        if (!mModifier) {
            IE_PREFIX_LOG(DEBUG, "UpdateDocument without modifier");
            return false;
        }
        return true;
    }
    case SKIP_DOC:
    case CHECKPOINT_DOC: {
        return true;
    }
    default:
        IE_LOG(WARN, "unknown operation, op type [%d]", opType);
        ERROR_COLLECTOR_LOG(WARN, "unknown operation, op type [%d]", opType);
        return false;
    }
    return false;
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
