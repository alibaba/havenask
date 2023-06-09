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
#include "indexlib/partition/segment/in_memory_segment_creator.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/partition/segment/compress_ratio_calculator.h"
#include "indexlib/partition/segment/multi_region_kkv_segment_writer.h"
#include "indexlib/partition/segment/multi_region_kv_segment_writer.h"
#include "indexlib/partition/segment/multi_sharding_segment_writer.h"
#include "indexlib/partition/segment/segment_writer_util.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/segment/sub_doc_segment_writer.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemorySegmentCreator);

InMemorySegmentCreator::InMemorySegmentCreator(uint32_t shardingColumnNum) : mColumnNum(shardingColumnNum) {}

InMemorySegmentCreator::~InMemorySegmentCreator() {}

void InMemorySegmentCreator::Init(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
{
    assert(schema);
    mSchema = schema;
    mOptions = options;

    vector<string> indexNames;
    ExtractNonTruncateIndexNames(schema, indexNames);
    if (indexNames.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "no index in schema!");
        return;
    }

    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    size_t hashMapInitSize = buildConfig.hashMapInitSize;
    if (hashMapInitSize == 0) {
        int64_t buildTotalMemory = buildConfig.buildTotalMemory;
        static const size_t initSizePerMB = 800;
        hashMapInitSize = (buildTotalMemory / indexNames.size() / 2) * initSizePerMB;
    }

    mSegmentMetrics.reset(new framework::SegmentMetrics);
    for (size_t i = 0; i < indexNames.size(); ++i) {
        mSegmentMetrics->SetDistinctTermCount(indexNames[i], hashMapInitSize);
    }
}

InMemorySegmentPtr InMemorySegmentCreator::Create(const InMemoryPartitionDataPtr& inMemPartData,
                                                  const util::PartitionMemoryQuotaControllerPtr& memController)
{
    assert(inMemPartData);
    UpdateSegmentMetrics(inMemPartData);
    BuildingSegmentData newSegmentData = inMemPartData->CreateNewSegmentData();
    SegmentDirectoryPtr segDir = inMemPartData->GetSegmentDirectory();
    segmentid_t segId = newSegmentData.GetSegmentId();

    PartitionSegmentIteratorPtr partSegIter = inMemPartData->CreateSegmentIterator();
    InMemorySegmentPtr inMemSegment =
        Create(partSegIter, newSegmentData, segDir->GetSegmentParentDirectory(segId), memController,
               inMemPartData->GetCounterMap(), inMemPartData->GetPluginManager());

    if (partSegIter.use_count() > 1) {
        IE_LOG(ERROR, "!! PartitionSegmentIteratorPtr should not be hold by segmentWriter,"
                      "may cause building segment not release memory!");
    }
    inMemPartData->SetInMemorySegment(inMemSegment);
    return inMemSegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(const InMemoryPartitionDataPtr& inMemPartData,
                                                  const InMemorySegmentPtr& inMemSegment,
                                                  InMemorySegmentCreator::CreateOption option)
{
    assert(inMemPartData);
    UpdateSegmentMetrics(inMemPartData);

    BuildingSegmentData newSegmentData = inMemPartData->CreateNewSegmentData();
    SegmentDirectoryPtr segDir = inMemPartData->GetSegmentDirectory();
    segmentid_t segId = newSegmentData.GetSegmentId();

    assert(segId == inMemSegment->GetSegmentId());
    if (segId != inMemSegment->GetSegmentId()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "new SegmentData [%d] not consistent with inherited InMemSegment [%d]",
                             segId, inMemSegment->GetSegmentId());
    }

    InMemorySegmentPtr newInMemSegment =
        Create(newSegmentData, segDir->GetSegmentParentDirectory(segId), inMemSegment, option);
    inMemPartData->SetInMemorySegment(newInMemSegment);
    return newInMemSegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(const PartitionSegmentIteratorPtr& partitionSegIter,
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
    SegmentWriterPtr segmentWriter =
        DoCreate(partitionSegIter, newSegmentData, mSegmentMetrics, segmentMemController, counterMap, pluginManager);

    assert(segmentWriter);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    if (!subSchema) {
        return CreateInMemorySegment(newSegmentData, segmentWriter, buildConfig, false, segmentMemController,
                                     counterMap);
    }

    SubDocSegmentWriterPtr subDocSegmentWriter = DYNAMIC_POINTER_CAST(SubDocSegmentWriter, segmentWriter);
    InMemorySegmentPtr inMemorySegment = CreateInMemorySegment(newSegmentData, subDocSegmentWriter, buildConfig, false,
                                                               segmentMemController, counterMap);

    util::BlockMemoryQuotaControllerPtr subSegmentMemController(
        new util::BlockMemoryQuotaController(memController, "in_memory_segment"));
    BuildingSegmentDataPtr subSegmentData =
        DYNAMIC_POINTER_CAST(BuildingSegmentData, newSegmentData.GetSubSegmentData());
    InMemorySegmentPtr subInMemorySegment = CreateInMemorySegment(
        *subSegmentData, subDocSegmentWriter->GetSubWriter(), buildConfig, true, subSegmentMemController, counterMap);

    inMemorySegment->SetSubInMemorySegment(subInMemorySegment);
    return inMemorySegment;
}

InMemorySegmentPtr InMemorySegmentCreator::Create(const BuildingSegmentData& segmentData,
                                                  const DirectoryPtr& segParentDirectory,
                                                  const InMemorySegmentPtr& inMemorySegment,
                                                  InMemorySegmentCreator::CreateOption option)
{
    InMemorySegment::CloneType cloneType = InMemorySegment::CT_SHARED;
    if (option == InMemorySegmentCreator::SHARED) {
        cloneType = InMemorySegment::CT_SHARED;
    } else {
        assert(option == PRIVATE);
        cloneType = InMemorySegment::CT_PRIVATE;
    }
    assert(inMemorySegment);
    BuildingSegmentData newSegmentData = segmentData;
    newSegmentData.Init(segParentDirectory);

    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();

    InMemorySegmentPtr newInMemorySegment(new InMemorySegment(*inMemorySegment, newSegmentData, cloneType));
    if (!subSchema) {
        return newInMemorySegment;
    }

    BuildingSegmentDataPtr subSegmentData =
        DYNAMIC_POINTER_CAST(BuildingSegmentData, newSegmentData.GetSubSegmentData());
    const InMemorySegment& subInMemorySegment = *(inMemorySegment->GetSubInMemorySegment());
    assert(subSegmentData);
    InMemorySegmentPtr newSubInMemorySegment(new InMemorySegment(subInMemorySegment, *subSegmentData, cloneType));
    newInMemorySegment->SetSubInMemorySegment(newSubInMemorySegment);
    return newInMemorySegment;
}

InMemorySegmentPtr InMemorySegmentCreator::CreateInMemorySegment(
    const BuildingSegmentData& segmentData, const SegmentWriterPtr& segmentWriter,
    const config::BuildConfig& buildConfig, bool isSubSegment,
    const util::BlockMemoryQuotaControllerPtr& segmentMemController, const util::CounterMapPtr& counterMap)
{
    InMemorySegmentPtr inMemorySegment(new InMemorySegment(buildConfig, segmentMemController, counterMap));
    inMemorySegment->Init(segmentData, segmentWriter, isSubSegment);

    if (!isSubSegment && !mOptions.IsOffline() && mSchema->GetTableType() != tt_kv &&
        mSchema->GetTableType() != tt_kkv) {
        bool useCompressOperation = mOptions.GetOnlineConfig().enableCompressOperationBlock;
        OperationWriterPtr operationWriter;
        if (useCompressOperation) {
            operationWriter.reset(new CompressOperationWriter(buildConfig.buildTotalMemory * 1024 * 1024));
        } else {
            operationWriter.reset(new OperationWriter);
        }
        operationWriter->Init(mSchema, mOptions.GetOnlineConfig().maxOperationQueueBlockSize,
                              mOptions.GetOnlineConfig().OperationLogFlushToDiskEnabled());
        inMemorySegment->SetOperationWriter(operationWriter);
    }
    return inMemorySegment;
}

SegmentWriterPtr
InMemorySegmentCreator::CreateKKVSegmentWriter(BuildingSegmentData& segmentData,
                                               const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                               const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    if (mColumnNum == 1) {
        return CreateNoShardingKKVSegmentWriter(segmentData, metrics, segmentMemController);
    }
    return CreateShardingKKVSegmentWriter(segmentData, metrics, segmentMemController);
}

SegmentWriterPtr
InMemorySegmentCreator::CreateKVSegmentWriter(BuildingSegmentData& segmentData,
                                              const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                              const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    if (mColumnNum == 1) {
        return CreateNoShardingKVSegmentWriter(segmentData, metrics, segmentMemController);
    }
    return CreateShardingKVSegmentWriter(segmentData, metrics, segmentMemController);
}

SegmentWriterPtr InMemorySegmentCreator::DoCreate(const PartitionSegmentIteratorPtr& partitionSegIter,
                                                  BuildingSegmentData& segmentData,
                                                  const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                  const util::BlockMemoryQuotaControllerPtr& segmentMemController,
                                                  const util::CounterMapPtr& counterMap,
                                                  const plugin::PluginManagerPtr& pluginManager)
{
    // TODO kv/kkv support counterMap
    if (mSchema->GetTableType() == tt_kv) {
        return CreateKVSegmentWriter(segmentData, metrics, segmentMemController);
    }
    if (mSchema->GetTableType() == tt_kkv) {
        return CreateKKVSegmentWriter(segmentData, metrics, segmentMemController);
    }

    assert(mColumnNum == 1);
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return CreateSingleSegmentWriter(segmentData, metrics, segmentMemController, counterMap, pluginManager,
                                         partitionSegIter);
    }

    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();
    SubDocSegmentWriterPtr subDocSegmentWriter(new SubDocSegmentWriter(mSchema, mOptions));

    size_t initMem =
        SubDocSegmentWriter::EstimateInitMemUse(metrics, mSchema, mOptions, pluginManager, partitionSegIter);

    std::shared_ptr<framework::SegmentMetrics> segMetrics = metrics;
    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed, set segment metrics empty", initMem);
        segMetrics.reset(new indexlib::framework::SegmentMetrics());
    }
    subDocSegmentWriter->Init(static_cast<SegmentData&>(segmentData), segMetrics, counterMap, pluginManager,
                              partitionSegIter);
    return subDocSegmentWriter;
}

SingleSegmentWriterPtr InMemorySegmentCreator::CreateSingleSegmentWriter(
    const BuildingSegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
    const util::BlockMemoryQuotaControllerPtr& segmentMemController, const util::CounterMapPtr& counterMap,
    const plugin::PluginManagerPtr& pluginManager, const PartitionSegmentIteratorPtr& partitionSegIter)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();

    SingleSegmentWriterPtr segmentWriter(new SingleSegmentWriter(mSchema, mOptions));
    size_t initMem =
        SingleSegmentWriter::EstimateInitMemUse(metrics, mSchema, mOptions, pluginManager, partitionSegIter);

    std::shared_ptr<framework::SegmentMetrics> segMetrics = metrics;
    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed, set segment metrics empty", initMem);
        segMetrics.reset(new indexlib::framework::SegmentMetrics());
    }
    segmentWriter->Init(segmentData, segMetrics, util::BuildResourceMetricsPtr(), counterMap, pluginManager,
                        partitionSegIter);
    return segmentWriter;
}

void InMemorySegmentCreator::ExtractNonTruncateIndexNames(const IndexPartitionSchemaPtr& schema,
                                                          vector<string>& indexNames)
{
    assert(schema);
    ExtractNonTruncateIndexNames(schema->GetIndexSchema(), indexNames);
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        ExtractNonTruncateIndexNames(subSchema->GetIndexSchema(), indexNames);
    }
}

void InMemorySegmentCreator::ExtractNonTruncateIndexNames(const IndexSchemaPtr& indexSchemaPtr,
                                                          vector<string>& indexNames)
{
    if (!indexSchemaPtr) {
        return;
    }
    auto indexConfigs = indexSchemaPtr->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        string nonTruncateIndexName = indexConfig->GetNonTruncateIndexName();
        if (!nonTruncateIndexName.empty()) {
            continue;
        }
        indexNames.push_back(indexConfig->GetIndexName());
    }
}

SegmentWriterPtr InMemorySegmentCreator::CreateNoShardingKKVSegmentWriter(
    BuildingSegmentData& segmentData, const std::shared_ptr<framework::SegmentMetrics>& metrics,
    const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();

    assert(mColumnNum == 1);
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvConfig);
    SegmentWriterPtr segmentWriter;
    if (mSchema->GetRegionCount() > 1) {
        segmentWriter.reset(new MultiRegionKKVSegmentWriter(mSchema, mOptions));
    } else {
        FieldType skeyDictType = GetSKeyDictKeyType(kkvConfig->GetSuffixFieldConfig()->GetFieldType());
        switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        typedef FieldTypeTraits<type>::AttrItemType WriterType;                                                        \
        KKVSegmentWriter<WriterType>* writer = new KKVSegmentWriter<WriterType>(mSchema, mOptions);                    \
        segmentWriter.reset(writer);                                                                                   \
        break;                                                                                                         \
    }
            NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        default: {
            assert(false);
            break;
        }
        }
    }
    size_t initMem = SegmentWriterUtil::EstimateInitMemUseForKKVSegmentWriter(
        kkvConfig, /*columnIdx=*/0, /*totalColumnCount=*/1, mOptions, metrics, buildMemoryQuotaControler);
    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed", initMem);
        SetSegmentInitMemUseRatio(metrics, initMem, segmentMemController);
    }
    segmentWriter->Init(segmentData, metrics, buildMemoryQuotaControler);
    return segmentWriter;
}

SegmentWriterPtr
InMemorySegmentCreator::CreateShardingKKVSegmentWriter(BuildingSegmentData& segmentData,
                                                       const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                       const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();
    assert(mColumnNum > 1);
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kkvConfig);

    FieldType skeyDictType = GetSKeyDictKeyType(kkvConfig->GetSuffixFieldConfig()->GetFieldType());
    vector<SegmentWriterPtr> segWriterVec;
    size_t initMem = 0;
    for (uint32_t i = 0; i < mColumnNum; i++) {
        if (mSchema->GetRegionCount() > 1) {
            segWriterVec.push_back(SegmentWriterPtr(new MultiRegionKKVSegmentWriter(mSchema, mOptions, i, mColumnNum)));
        } else {
            switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        typedef FieldTypeTraits<type>::AttrItemType WriterType;                                                        \
        KKVSegmentWriter<WriterType>* writer = new KKVSegmentWriter<WriterType>(mSchema, mOptions, i, mColumnNum);     \
        segWriterVec.push_back(SegmentWriterPtr(writer));                                                              \
        break;                                                                                                         \
    }
                NUMBER_FIELD_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
            default: {
                assert(false);
                break;
            }
            }
        }
        initMem += SegmentWriterUtil::EstimateInitMemUseForKKVSegmentWriter(
            kkvConfig, /*columnIdx=*/i, /*totalColumnCount=*/mColumnNum, mOptions, metrics, buildMemoryQuotaControler);
    }

    util::BuildResourceMetricsPtr buildMetrics(new util::BuildResourceMetrics());
    buildMetrics->Init();
    MultiShardingSegmentWriterPtr segWriter(new MultiShardingSegmentWriter(mSchema, mOptions, segWriterVec));
    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed", initMem);
        SetSegmentInitMemUseRatio(metrics, initMem, segmentMemController);
    }
    segWriter->Init(segmentData, metrics, buildMemoryQuotaControler, buildMetrics);
    return segWriter;
}

SegmentWriterPtr
InMemorySegmentCreator::CreateNoShardingKVSegmentWriter(BuildingSegmentData& segmentData,
                                                        const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                        const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();

    assert(mColumnNum == 1);
    KVSegmentWriterPtr kvSegWriter;
    if (mSchema->GetRegionCount() > 1) {
        kvSegWriter.reset(new MultiRegionKVSegmentWriter(mSchema, mOptions));
    } else {
        kvSegWriter.reset(new KVSegmentWriter(mSchema, mOptions));
    }
    KVIndexConfigPtr kvConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    assert(kvConfig);
    size_t initMem = KVSegmentWriter::EstimateInitMemUse(kvConfig, /*columnIdx=*/0, /*totalColumnCount=*/1, mOptions,
                                                         metrics, buildMemoryQuotaControler);

    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed", initMem);
        SetSegmentInitMemUseRatio(metrics, initMem, segmentMemController);
    }
    kvSegWriter->Init(segmentData, metrics, buildMemoryQuotaControler);
    return kvSegWriter;
}

SegmentWriterPtr
InMemorySegmentCreator::CreateShardingKVSegmentWriter(BuildingSegmentData& segmentData,
                                                      const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                      const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    util::QuotaControlPtr buildMemoryQuotaControler =
        segmentMemController->GetPartitionMemoryQuotaController()->GetBuildMemoryQuotaControler();

    assert(mColumnNum > 1);
    vector<SegmentWriterPtr> segWriterVec;
    size_t initMem = 0;
    for (uint32_t i = 0; i < mColumnNum; i++) {
        KVSegmentWriterPtr kvSegWriter;
        if (mSchema->GetRegionCount() > 1) {
            kvSegWriter.reset(new MultiRegionKVSegmentWriter(mSchema, mOptions, i, mColumnNum));
        } else {
            kvSegWriter.reset(new KVSegmentWriter(mSchema, mOptions, i, mColumnNum));
        }
        segWriterVec.push_back(kvSegWriter);
        KVIndexConfigPtr kvConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        assert(kvConfig);
        initMem += KVSegmentWriter::EstimateInitMemUse(kvConfig, /*columnIdx=*/i, /*totalColumnCount=*/mColumnNum,
                                                       mOptions, metrics, buildMemoryQuotaControler);
    }

    util::BuildResourceMetricsPtr buildMetrics(new util::BuildResourceMetrics());
    buildMetrics->Init();
    MultiShardingSegmentWriterPtr segWriter(new MultiShardingSegmentWriter(mSchema, mOptions, segWriterVec));
    if (!segmentMemController->Reserve(initMem)) {
        IE_LOG(INFO, "reserve writer init mem [%ld] failed", initMem);
        SetSegmentInitMemUseRatio(metrics, initMem, segmentMemController);
    }
    segWriter->Init(segmentData, metrics, buildMemoryQuotaControler, buildMetrics);
    return segWriter;
}

void InMemorySegmentCreator::SetSegmentInitMemUseRatio(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                       size_t initMem,
                                                       const util::BlockMemoryQuotaControllerPtr& segmentMemController)
{
    size_t leftMemQuota = segmentMemController->GetFreeQuota();
    double segInitMemUseRatio = (initMem <= leftMemQuota) ? 1.0 : (double)leftMemQuota / (double)initMem;
    mSegmentMetrics->Set(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO, segInitMemUseRatio);
}

void InMemorySegmentCreator::UpdateSegmentMetrics(const PartitionDataPtr& partData)
{
    TableType tableType = mSchema->GetTableType();
    if (tableType != tt_kkv && tableType != tt_kv) {
        return;
    }

    assert(mSegmentMetrics);
    CompressRatioCalculator calculator;
    calculator.Init(partData, mSchema);
    if (tableType == tt_kkv) {
        mSegmentMetrics->Set(KKV_COMPRESS_RATIO_GROUP_NAME, SUFFIX_KEY_FILE_NAME,
                             calculator.GetCompressRatio(SUFFIX_KEY_FILE_NAME));
        mSegmentMetrics->Set(KKV_COMPRESS_RATIO_GROUP_NAME, KKV_VALUE_FILE_NAME,
                             calculator.GetCompressRatio(KKV_VALUE_FILE_NAME));
    }

    if (tableType == tt_kv) {
        mSegmentMetrics->Set(KV_COMPRESS_RATIO_GROUP_NAME, KV_VALUE_FILE_NAME,
                             calculator.GetCompressRatio(KV_VALUE_FILE_NAME));
    }
    double segInitMemUseRatio = 1.0;
    mSegmentMetrics->Set(SEGMENT_MEM_CONTROL_GROUP, SEGMENT_INIT_MEM_USE_RATIO, segInitMemUseRatio);
}
}} // namespace indexlib::partition
