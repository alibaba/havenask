#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/partition/segment/multi_sharding_segment_writer.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/partition/segment/compress_ratio_calculator.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/memory_control/partition_memory_quota_controller.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, InMemorySegmentCreator);

InMemorySegmentCreator::InMemorySegmentCreator(uint32_t shardingColumnNum)
    : mColumnNum(shardingColumnNum)
{
}

InMemorySegmentCreator::~InMemorySegmentCreator()
{
}

void InMemorySegmentCreator::Init(const IndexPartitionSchemaPtr& schema,
                                  const IndexPartitionOptions& options)
{
    assert(schema);
    mSchema = schema;
    mOptions = options;

    vector<string> indexNames;
    ExtractNonTruncateIndexNames(schema, indexNames);
    if (indexNames.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index in schema!");
        return;
    }

    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    size_t hashMapInitSize = buildConfig.hashMapInitSize;
    if (hashMapInitSize == 0)
    {
        int64_t buildTotalMemory = buildConfig.buildTotalMemory;
        static const size_t initSizePerMB = 800;
        hashMapInitSize = (buildTotalMemory / indexNames.size() / 2) * initSizePerMB;
    }

    mSegmentMetrics.reset(new index_base::SegmentMetrics);
    for (size_t i = 0; i < indexNames.size(); ++i)
    {
        mSegmentMetrics->SetDistinctTermCount(indexNames[i], hashMapInitSize);
    }
}

InMemorySegmentPtr InMemorySegmentCreator::Create(
        const InMemoryPartitionDataPtr& inMemPartData,
        const util::PartitionMemoryQuotaControllerPtr& memController)
{
    assert(inMemPartData);
    UpdateSegmentMetrics(inMemPartData);
    BuildingSegmentData newSegmentData = inMemPartData->CreateNewSegmentData();
    SegmentDirectoryPtr segDir = inMemPartData->GetSegmentDirectory();
    segmentid_t segId = newSegmentData.GetSegmentId();

    PartitionSegmentIteratorPtr partSegIter = inMemPartData->CreateSegmentIterator();
    InMemorySegmentPtr inMemSegment = Create(partSegIter, newSegmentData,
            segDir->GetSegmentParentDirectory(segId), memController,
            inMemPartData->GetCounterMap(), inMemPartData->GetPluginManager());

    if (partSegIter.use_count() > 1) {
        IE_LOG(ERROR, "!! PartitionSegmentIteratorPtr should not be hold by segmentWriter,"
               "may cause building segment not release memory!");
    }
    inMemPartData->SetInMemorySegment(inMemSegment);
    return inMemSegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(
        const InMemoryPartitionDataPtr& inMemPartData,
        const InMemorySegmentPtr& inMemSegment)
{
    assert(inMemPartData);
    UpdateSegmentMetrics(inMemPartData);

    BuildingSegmentData newSegmentData = inMemPartData->CreateNewSegmentData();
    SegmentDirectoryPtr segDir = inMemPartData->GetSegmentDirectory();
    segmentid_t segId = newSegmentData.GetSegmentId();

    assert(segId == inMemSegment->GetSegmentId());
    if (segId != inMemSegment->GetSegmentId())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "new SegmentData [%d] not consistent with inherited InMemSegment [%d]",
                             segId, inMemSegment->GetSegmentId());
    }

    InMemorySegmentPtr newInMemSegment = Create(newSegmentData,
            segDir->GetSegmentParentDirectory(segId), inMemSegment);
    inMemPartData->SetInMemorySegment(newInMemSegment);
    return newInMemSegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(
        const PartitionSegmentIteratorPtr& partitionSegIter,
        const BuildingSegmentData& segmentData,
        const file_system::DirectoryPtr& segParentDirectory,
        const util::PartitionMemoryQuotaControllerPtr& memController,
        const util::CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    util::BlockMemoryQuotaControllerPtr segmentMemController(
            new util::BlockMemoryQuotaController(memController, "in_memory_segment"));
    BuildingSegmentData newSegmentData = segmentData;
    newSegmentData.Init(segParentDirectory);
    SegmentWriterPtr segmentWriter = DoCreate(partitionSegIter,
            newSegmentData, mSegmentMetrics, segmentMemController, counterMap, pluginManager);

    assert(segmentWriter);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    if (!subSchema)
    {
        return CreateInMemorySegment(
                newSegmentData, segmentWriter, buildConfig, false, segmentMemController, counterMap);
    }

    SubDocSegmentWriterPtr subDocSegmentWriter = DYNAMIC_POINTER_CAST(
            SubDocSegmentWriter, segmentWriter);
    InMemorySegmentPtr inMemorySegment = CreateInMemorySegment(
            newSegmentData, subDocSegmentWriter,
            buildConfig, false, segmentMemController, counterMap);

    util::BlockMemoryQuotaControllerPtr subSegmentMemController(
            new util::BlockMemoryQuotaController(memController, "in_memory_segment"));
    BuildingSegmentDataPtr subSegmentData =
        DYNAMIC_POINTER_CAST(BuildingSegmentData, newSegmentData.GetSubSegmentData());
    InMemorySegmentPtr subInMemorySegment = CreateInMemorySegment(
            *subSegmentData, subDocSegmentWriter->GetSubWriter(),
            buildConfig, true, subSegmentMemController, counterMap);

    inMemorySegment->SetSubInMemorySegment(subInMemorySegment);
    return inMemorySegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(
        const BuildingSegmentData& segmentData,
        const DirectoryPtr& segParentDirectory,
        const InMemorySegmentPtr& inMemorySegment)
{
    assert(inMemorySegment);
    BuildingSegmentData newSegmentData = segmentData;
    newSegmentData.Init(segParentDirectory);

    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();

    InMemorySegmentPtr newInMemorySegment(new InMemorySegment(*inMemorySegment, newSegmentData));
    if (!subSchema)
    {
        return newInMemorySegment;
    }

    BuildingSegmentDataPtr subSegmentData =
        DYNAMIC_POINTER_CAST(BuildingSegmentData, newSegmentData.GetSubSegmentData());
    const InMemorySegment& subInMemorySegment = *(inMemorySegment->GetSubInMemorySegment());
    assert(subSegmentData);
    InMemorySegmentPtr newSubInMemorySegment(new InMemorySegment(subInMemorySegment, *subSegmentData));
    newInMemorySegment->SetSubInMemorySegment(newSubInMemorySegment);
    return newInMemorySegment;
}

InMemorySegmentPtr InMemorySegmentCreator::CreateInMemorySegment(
        const BuildingSegmentData& segmentData,
        const SegmentWriterPtr& segmentWriter,
        const config::BuildConfig& buildConfig,
        bool isSubSegment,
        const util::BlockMemoryQuotaControllerPtr& segmentMemController,
        const util::CounterMapPtr& counterMap)
{
    InMemorySegmentPtr inMemorySegment(new InMemorySegment(buildConfig, segmentMemController, counterMap));
    inMemorySegment->Init(segmentData, segmentWriter , isSubSegment);

    if (!isSubSegment
        && !mOptions.IsOffline()
        && mSchema->GetTableType() != tt_kv
        && mSchema->GetTableType() != tt_kkv)
    {
        bool useCompressOperation = mOptions.GetOnlineConfig().enableCompressOperationBlock;
        OperationWriterPtr operationWriter;
        if (useCompressOperation)
        {
            operationWriter.reset(new CompressOperationWriter(
                            buildConfig.buildTotalMemory * 1024 * 1024));
        }
        else
        {
            operationWriter.reset(new OperationWriter);
        }
        operationWriter->Init(mSchema, mOptions.GetOnlineConfig().maxOperationQueueBlockSize);
        inMemorySegment->SetOperationWriter(operationWriter);
    }
    return inMemorySegment;
}

SegmentWriterPtr InMemorySegmentCreator::DoCreate(
        const PartitionSegmentIteratorPtr& partitionSegIter,
        BuildingSegmentData& segmentData,
        const index_base::SegmentMetricsPtr& metrics,
        const util::BlockMemoryQuotaControllerPtr& segmentMemController,
        const util::CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    assert(mColumnNum == 1);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema)
    {
        return CreateSingleSegmentWriter(
                segmentData, metrics, segmentMemController,
                counterMap, pluginManager, partitionSegIter);
    }

    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();
    SubDocSegmentWriterPtr subDocSegmentWriter(new SubDocSegmentWriter(mSchema, mOptions));

    size_t initMem = subDocSegmentWriter->EstimateInitMemUse(metrics,
            buildMemoryQuotaControler, pluginManager, partitionSegIter);

    index_base::SegmentMetricsPtr segMetrics = metrics;
    if (!segmentMemController->Reserve(initMem))
    {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed, set segment metrics empty", initMem);
        segMetrics.reset(new SegmentMetrics());
    }
    subDocSegmentWriter->Init(
            static_cast<SegmentData&>(segmentData), segMetrics, counterMap,
            pluginManager, partitionSegIter);
    return subDocSegmentWriter;
}

SingleSegmentWriterPtr InMemorySegmentCreator::CreateSingleSegmentWriter(
        const BuildingSegmentData& segmentData,
        const index_base::SegmentMetricsPtr& metrics,
        const util::BlockMemoryQuotaControllerPtr& segmentMemController,
        const util::CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager,
        const PartitionSegmentIteratorPtr& partitionSegIter)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();

    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(mSchema, mOptions));
    size_t initMem = segmentWriter->EstimateInitMemUse(metrics,
            buildMemoryQuotaControler, pluginManager, partitionSegIter);

    index_base::SegmentMetricsPtr segMetrics = metrics;
    if (!segmentMemController->Reserve(initMem))
    {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed, set segment metrics empty", initMem);
        segMetrics.reset(new SegmentMetrics());
    }
    segmentWriter->Init(segmentData, segMetrics,
                        util::BuildResourceMetricsPtr(),
                        counterMap, pluginManager, partitionSegIter);
    return segmentWriter;
}

void InMemorySegmentCreator::ExtractNonTruncateIndexNames(
        const IndexPartitionSchemaPtr& schema, vector<string>& indexNames)
{
    assert(schema);
    ExtractNonTruncateIndexNames(schema->GetIndexSchema(), indexNames);
    const IndexPartitionSchemaPtr& subSchema =
        schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        ExtractNonTruncateIndexNames(subSchema->GetIndexSchema(), indexNames);
    }
}

void InMemorySegmentCreator::ExtractNonTruncateIndexNames(
        const IndexSchemaPtr& indexSchemaPtr, vector<string>& indexNames)
{
    if(!indexSchemaPtr)
    {
        return;
    }
    auto indexConfigs = indexSchemaPtr->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        string nonTruncateIndexName = indexConfig->GetNonTruncateIndexName();
        if(!nonTruncateIndexName.empty())
        {
            continue;
        }
        indexNames.push_back(indexConfig->GetIndexName());
    }
}

void InMemorySegmentCreator::SetSegmentInitMemUseRatio(
        const index_base::SegmentMetricsPtr& metrics, size_t initMem,
        const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    size_t leftMemQuota = segmentMemController->GetFreeQuota();
    double segInitMemUseRatio = (initMem <= leftMemQuota) ? 1.0 :
                                (double)leftMemQuota / (double)initMem;
    mSegmentMetrics->Set(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO,
                         segInitMemUseRatio);
}

void InMemorySegmentCreator::UpdateSegmentMetrics(const PartitionDataPtr& partData)
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_kkv && tableType != tt_kv)
    {
        return;
    }

    assert(mSegmentMetrics);
    CompressRatioCalculator calculator;
    calculator.Init(partData, mSchema);
    double segInitMemUseRatio = 1.0;
    mSegmentMetrics->Set(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO,
                         segInitMemUseRatio);
}

IE_NAMESPACE_END(partition);
