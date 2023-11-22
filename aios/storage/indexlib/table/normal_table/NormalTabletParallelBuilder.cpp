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
#include "indexlib/table/normal_table/NormalTabletParallelBuilder.h"

#include "autil/ThreadPool.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/index/ann/aitheta2/AithetaBuildWorkItem.h"
#include "indexlib/index/ann/aitheta2/SingleAithetaBuilder.h"
#include "indexlib/index/attribute/AttributeBuildWorkItem.h"
#include "indexlib/index/attribute/SingleAttributeBuilder.h"
#include "indexlib/index/deletionmap/DeletionMapBuildWorkItem.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/deletionmap/SingleDeletionMapBuilder.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/FieldMetaBuildWorkItem.h"
#include "indexlib/index/field_meta/SingleFieldMetaBuilder.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/inverted_index/InvertedIndexBuildWorkItem.h"
#include "indexlib/index/inverted_index/SingleInvertedIndexBuilder.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/operation_log/OperationLogBuildWorkItem.h"
#include "indexlib/index/primary_key/PrimaryKeyBuildWorkItem.h"
#include "indexlib/index/primary_key/SinglePrimaryKeyBuilder.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SingleSourceBuilder.h"
#include "indexlib/index/source/SourceBuildWorkItem.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/index/summary/SingleSummaryBuilder.h"
#include "indexlib/index/summary/SummaryBuildWorkItem.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalMemSegment.h"
#include "indexlib/table/normal_table/virtual_attribute/SingleVirtualAttributeBuilder.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeBuildWorkItem.h"
#include "indexlib/util/GroupedThreadPool.h"
#include "indexlib/util/memory_control/WaitMemoryQuotaController.h"

namespace indexlib::table {
namespace {
using indexlibv2::framework::OpenOptions;
}

AUTIL_LOG_SETUP(indexlib.table, NormalTabletParallelBuilder);

NormalTabletParallelBuilder::NormalTabletParallelBuilder()
    : _normalBuildingSegment(nullptr)
    , _buildThreadPool(nullptr)
    , _docBatchMemController(nullptr)
    , _consistentModeBuildThreadPool(nullptr)
    , _inconsistentModeBuildThreadPool(nullptr)
{
}

NormalTabletParallelBuilder::~NormalTabletParallelBuilder()
{
    if (_buildThreadPool != nullptr) {
        _buildThreadPool->WaitFinish();
        _buildThreadPool = nullptr;
    }
}

template <typename SingleBuilderType>
Status
NormalTabletParallelBuilder::InitSingleBuilders(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
                                                const std::string& indexTypeStr,
                                                std::vector<std::unique_ptr<SingleBuilderType>>* builders)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexTypeStr)) {
        auto singleBuilder = std::make_unique<SingleBuilderType>();
        auto status = singleBuilder->Init(*tabletData, indexConfig);
        RETURN_IF_STATUS_ERROR(status, "init single [%s] builder [%s] failed", indexTypeStr.c_str(),
                               indexConfig->GetIndexName().c_str());
        builders->emplace_back(std::move(singleBuilder));
    }
    return Status::OK();
}

Status NormalTabletParallelBuilder::InitSingleDeletionMapBuilders(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData,
    std::vector<std::unique_ptr<indexlib::index::SingleDeletionMapBuilder>>* builders)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexlibv2::index::DELETION_MAP_INDEX_TYPE_STR)) {
        auto singleBuilder = std::make_unique<indexlib::index::SingleDeletionMapBuilder>();
        auto status = singleBuilder->Init(*tabletData, indexConfig);
        RETURN_IF_STATUS_ERROR(status, "init single deletion map builder failed");
        builders->emplace_back(std::move(singleBuilder));
    }
    return Status::OK();
}

Status NormalTabletParallelBuilder::ValidateConfigs(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    for (const auto& indexConfig : schema->GetIndexConfigs()) {
        if (indexConfig->GetIndexType() != index::INVERTED_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::SUMMARY_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::SOURCE_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::ANN_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != index::OPERATION_LOG_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != index::FIELD_META_INDEX_TYPE_STR &&
            indexConfig->GetIndexType() != indexlibv2::index::DELETION_MAP_INDEX_TYPE_STR) {
            AUTIL_LOG(WARN, "type [%s] index [%s] does not support multi-thread build",
                      indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
            _parallelable = false;
            return SwitchBuildMode(OpenOptions::STREAM);
        }
    }
    return Status::OK();
}

Status
NormalTabletParallelBuilder::PrepareForWrite(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                             const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData)
{
    RETURN_STATUS_DIRECTLY_IF_ERROR(ValidateConfigs(schema));

    RETURN_IF_STATUS_ERROR(InitSingleInvertedIndexBuilders(schema, tabletData), "init inverted index builders failed");
    RETURN_IF_STATUS_ERROR(InitSingleAttributeBuilders(schema, tabletData), "init attribute builders failed");
    RETURN_IF_STATUS_ERROR(InitSingleVirtualAttributeBuilders(schema, tabletData), "init attribute builders failed");
    RETURN_IF_STATUS_ERROR(InitSinglePrimaryKeyBuilders(schema, tabletData), "init primary key builders failed");
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleDeletionMapBuilders(schema, tabletData, &_singleDeletionMapBuilders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleBuilders<index::ann::SingleAithetaBuilder>(
        schema, tabletData, indexlibv2::index::ANN_INDEX_TYPE_STR, &_singleAnnBuilders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleBuilders<index::SingleSummaryBuilder>(
        schema, tabletData, indexlibv2::index::SUMMARY_INDEX_TYPE_STR, &_singleSummaryBuilders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleBuilders<index::SingleSourceBuilder>(
        schema, tabletData, indexlibv2::index::SOURCE_INDEX_TYPE_STR, &_singleSourceBuilders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleBuilders<index::SingleOperationLogBuilder>(
        schema, tabletData, index::OPERATION_LOG_INDEX_TYPE_STR, &_singleOpLogBuilders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(InitSingleBuilders<index::SingleFieldMetaBuilder>(
        schema, tabletData, index::FIELD_META_INDEX_TYPE_STR, &_singleFieldMetaBuilders));
    _isPreparedForWrite = true;
    return Status::OK();
}

Status
NormalTabletParallelBuilder::Init(const std::shared_ptr<indexlibv2::table::NormalMemSegment>& normalBuildingSegment,
                                  const indexlibv2::config::TabletOptions* tabletOptions,
                                  const std::shared_ptr<autil::ThreadPool>& consistentModeBuildThreadPool,
                                  const std::shared_ptr<autil::ThreadPool>& inconsistentModeBuildThreadPool)
{
    _normalBuildingSegment = normalBuildingSegment;
    _isOnline = tabletOptions->IsOnline();
    _docBatchMemController = std::make_unique<indexlib::util::WaitMemoryQuotaController>((size_t)128 * 1024 * 1024);
    _consistentModeBuildThreadPool = consistentModeBuildThreadPool;
    _inconsistentModeBuildThreadPool = inconsistentModeBuildThreadPool;
    return Status::OK();
}

Status NormalTabletParallelBuilder::SwitchBuildMode(OpenOptions::BuildMode buildMode)
{
    if (!_parallelable) {
        buildMode = OpenOptions::STREAM;
    }
    if (buildMode == _buildMode) {
        return Status::OK();
    }
    if (_buildMode != OpenOptions::STREAM) {
        assert(_buildThreadPool != nullptr);
        _buildThreadPool->WaitFinish();
    }
    if (buildMode == OpenOptions::CONSISTENT_BATCH) {
        if (_consistentModeBuildThreadPool == nullptr) {
            AUTIL_LOG(INFO, "consistentModeBuildThreadPool is null, force using STREAM mode");
            _buildMode = OpenOptions::STREAM;
            _buildThreadPool = nullptr;
            return Status::OK();
        }
        _buildThreadPool = std::make_unique<indexlib::util::GroupedThreadPool>();
        RETURN_IF_STATUS_ERROR(
            _buildThreadPool->Start(_consistentModeBuildThreadPool, /*maxGroupCount=*/65536, /*maxBatchCount=*/128),
            "start build thread pool failed");
        _consistentModeBuildThreadPool->start();
        _buildMode = buildMode;
        return Status::OK();
    }
    if (buildMode == OpenOptions::INCONSISTENT_BATCH) {
        if (_inconsistentModeBuildThreadPool == nullptr) {
            AUTIL_LOG(INFO, "inconsistentModeBuildThreadPool is null, force using STREAM mode");
            _buildMode = OpenOptions::STREAM;
            _buildThreadPool = nullptr;
            return Status::OK();
        }
        _buildThreadPool = std::make_unique<indexlib::util::GroupedThreadPool>();
        RETURN_IF_STATUS_ERROR(
            _buildThreadPool->Start(_inconsistentModeBuildThreadPool, /*maxGroupCount=*/65536, /*maxBatchCount=*/128),
            "start build thread pool failed");
        _inconsistentModeBuildThreadPool->start();
        _buildMode = buildMode;
        return Status::OK();
    }
    assert(buildMode == OpenOptions::STREAM);
    _buildThreadPool = nullptr;
    _buildMode = buildMode;
    return Status::OK();
}

OpenOptions::BuildMode NormalTabletParallelBuilder::GetBuildMode() const { return _buildMode; }

Status NormalTabletParallelBuilder::InitSingleInvertedIndexBuilders(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig);
        if (invertedIndexConfig->IsDisabled() || !invertedIndexConfig->IsNormal()) {
            continue;
        }
        auto invertedIndexType = invertedIndexConfig->GetInvertedIndexType();
        if (invertedIndexType == it_primarykey64 || invertedIndexType == it_primarykey128 ||
            invertedIndexType == it_customized) {
            continue;
        }
        if (invertedIndexConfig->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
            auto shardingIndexConfigs = invertedIndexConfig->GetShardingIndexConfigs();
            assert(shardingIndexConfigs.size() > 0);
            // For n=shardingIndexConfigs.size() shards, we need n+1 builders. The extra is for section attribute.
            for (size_t i = 0; i <= shardingIndexConfigs.size(); ++i) {
                auto singleBuilder = std::make_unique<index::SingleInvertedIndexBuilder>();
                auto status = singleBuilder->Init(*tabletData, invertedIndexConfig,
                                                  i == shardingIndexConfigs.size() ? nullptr : shardingIndexConfigs[i],
                                                  i, _isOnline);
                RETURN_IF_STATUS_ERROR(status, "init single inverted index builder [%s] failed",
                                       shardingIndexConfigs[i]->GetIndexName().c_str());
                _singleInvertedIndexBuilders.emplace_back(std::move(singleBuilder));
            }
        } else {
            auto singleBuilder = std::make_unique<index::SingleInvertedIndexBuilder>();
            auto status = singleBuilder->Init(*tabletData, invertedIndexConfig, nullptr, INVALID_SHARDID, _isOnline);
            RETURN_IF_STATUS_ERROR(status, "init single inverted index builder [%s] failed",
                                   invertedIndexConfig->GetIndexName().c_str());
            _singleInvertedIndexBuilders.emplace_back(std::move(singleBuilder));
        }
    }
    return Status::OK();
}

Status NormalTabletParallelBuilder::InitSingleAttributeBuilders(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR)) {
        auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        assert(attributeConfig);
        if (attributeConfig->IsDisabled() || !attributeConfig->IsNormal()) {
            continue;
        }
        auto singleBuilder =
            std::make_unique<index::SingleAttributeBuilder<indexlibv2::index::AttributeDiskIndexer,
                                                           indexlibv2::index::AttributeMemIndexer>>(schema);
        auto status = singleBuilder->Init(*tabletData, attributeConfig, _isOnline);
        RETURN_IF_STATUS_ERROR(status, "init single attribute builder [%s] failed",
                               attributeConfig->GetIndexName().c_str());
        _singleAttributeBuilders.emplace_back(std::move(singleBuilder));
    }
    // TODO(ZQ): support pack attribute
    return Status::OK();
}

Status NormalTabletParallelBuilder::InitSingleVirtualAttributeBuilders(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexlibv2::table::VIRTUAL_ATTRIBUTE_INDEX_TYPE_STR)) {
        auto singleBuilder = std::make_unique<SingleVirtualAttributeBuilder>(schema);
        auto status = singleBuilder->Init(*tabletData, indexConfig, _isOnline);
        RETURN_IF_STATUS_ERROR(status, "Init single virtual attribute builder [%s] failed",
                               indexConfig->GetIndexName().c_str());
        _singleVirtualAttributeBuilders.emplace_back(std::move(singleBuilder));
    }
    return Status::OK();
}

Status NormalTabletParallelBuilder::InitSinglePrimaryKeyBuilders(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlibv2::framework::TabletData>& tabletData)
{
    for (const auto& indexConfig : schema->GetIndexConfigs(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR)) {
        auto primaryKeyIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(indexConfig);
        assert(primaryKeyIndexConfig);
        std::unique_ptr<index::ISinglePrimaryKeyBuilder> singleBuilder;
        if (primaryKeyIndexConfig->GetInvertedIndexType() == it_primarykey64) {
            singleBuilder = std::make_unique<index::SinglePrimaryKeyBuilder<uint64_t>>();
        } else {
            singleBuilder = std::make_unique<index::SinglePrimaryKeyBuilder<autil::uint128_t>>();
        }
        auto status = singleBuilder->Init(*tabletData, primaryKeyIndexConfig);
        RETURN_IF_STATUS_ERROR(status, "init single primary key builder [%s] failed",
                               primaryKeyIndexConfig->GetIndexName().c_str());
        _singlePrimaryKeyBuilders.emplace_back(std::move(singleBuilder));
    }
    // if (unlikely(_singlePrimaryKeyBuilders.size() == 0)) {
    //     return Status::Unimplement("not support multi-thread build when no primary key index found, table [%s]",
    //                                schema->GetTableName().c_str());
    // }
    return Status::OK();
}

template <typename BuilderType, typename BuildWorkItemType>
void NormalTabletParallelBuilder::CreateBuildWorkItems(
    const std::vector<std::unique_ptr<BuilderType>>& builders,
    const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    for (const auto& singleBuilder : builders) {
        auto buildWorkItem = std::make_unique<BuildWorkItemType>(singleBuilder.get(), batch.get());
        std::string name = buildWorkItem->Name();
        _buildThreadPool->PushWorkItem(name, std::move(buildWorkItem));
    }
}

// In CONSISTENT_BATCH build mode, this function will block until Build is complete in all threads.
// In INCONSISTENT_BATCH build mode, this function will return immediately after necessary
// build work is done. This will provide better pipelining performance and parallelism. The user can call WaitFinish()
// to wait for all previous Build() to complete.
Status NormalTabletParallelBuilder::Build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    if (unlikely(_buildThreadPool == nullptr)) {
        return Status::InternalError("Can not use multi-thread build without threadpool");
    }
    assert(_buildMode == OpenOptions::CONSISTENT_BATCH || _buildMode == OpenOptions::INCONSISTENT_BATCH);
    int64_t startTimeInMs = autil::TimeUtility::currentTimeInMicroSeconds() / 1000;
    AUTIL_INTERVAL_LOG2(60, INFO, "Begin batch build [%ld / %ld] docs, log interval 60s", batch->GetValidDocCount(),
                        batch->GetBatchSize());

    int64_t docBatchMemUse = batch->EstimateMemory();
    while (!_docBatchMemController->Allocate(docBatchMemUse)) {
        AUTIL_LOG(WARN, "collect_doc_memory_quota not enough for one batch, will expand it [%.1f]M => [%.1f]M",
                  _docBatchMemController->GetTotalQuota() / 1024.0 / 1024, docBatchMemUse / 1024.0 / 1024);
        _docBatchMemController->AddQuota(docBatchMemUse - _docBatchMemController->GetTotalQuota());
    }

    // 索引构建任务
    _buildThreadPool->StartNewBatch();

    // PK / DeletionMap 必须同步执行，来保证下一轮正确分配DocId，其它索引进线程池。
    for (const auto& singleBuilder : _singlePrimaryKeyBuilders) {
        auto buildWorkItem = std::make_unique<index::PrimaryKeyBuildWorkItem>(singleBuilder.get(), batch.get());
        if (_buildMode == OpenOptions::CONSISTENT_BATCH) {
            std::string name = buildWorkItem->Name();
            _buildThreadPool->PushWorkItem(name, std::move(buildWorkItem));
        } else {
            buildWorkItem->process();
        }
    }
    for (const auto& singleBuilder : _singleDeletionMapBuilders) {
        auto buildWorkItem = std::make_unique<index::DeletionMapBuildWorkItem>(singleBuilder.get(), batch.get());
        if (_buildMode == OpenOptions::CONSISTENT_BATCH) {
            std::string name = buildWorkItem->Name();
            _buildThreadPool->PushWorkItem(name, std::move(buildWorkItem));
        } else {
            buildWorkItem->process();
        }
    }

    auto lastLocator = batch->GetLastLocator();
    auto maxTimestamp = batch->GetMaxTimestamp();
    auto maxTTL = batch->GetMaxTTL();
    auto addDocCount = batch->GetAddedDocCount();
    if (_buildMode == OpenOptions::INCONSISTENT_BATCH) {
        // Update segment meta data after PK and DeletionMap is built. This will cause a small period of time that docs
        // are not fully built but can be queries partially.
        _normalBuildingSegment->PostBuildActions(lastLocator, maxTimestamp, maxTTL, addDocCount);
    }

    CreateBuildWorkItems<
        indexlib::index::SingleAttributeBuilder<indexlibv2::index::AttributeDiskIndexer,
                                                indexlibv2::index::AttributeMemIndexer>,
        index::AttributeBuildWorkItem<indexlibv2::index::AttributeDiskIndexer, indexlibv2::index::AttributeMemIndexer>>(
        _singleAttributeBuilders, batch);
    CreateBuildWorkItems<SingleVirtualAttributeBuilder, VirtualAttributeBuildWorkItem>(_singleVirtualAttributeBuilders,
                                                                                       batch);
    CreateBuildWorkItems<indexlib::index::SingleInvertedIndexBuilder, index::InvertedIndexBuildWorkItem>(
        _singleInvertedIndexBuilders, batch);
    CreateBuildWorkItems<indexlib::index::SingleSummaryBuilder, index::SummaryBuildWorkItem>(_singleSummaryBuilders,
                                                                                             batch);
    CreateBuildWorkItems<indexlib::index::SingleSourceBuilder, indexlib::index::SourceBuildWorkItem>(
        _singleSourceBuilders, batch);
    CreateBuildWorkItems<indexlib::index::ann::SingleAithetaBuilder, index::ann::AithetaBuildWorkItem>(
        _singleAnnBuilders, batch);
    CreateBuildWorkItems<indexlib::index::SingleOperationLogBuilder, index::OperationLogBuildWorkItem>(
        _singleOpLogBuilders, batch);
    CreateBuildWorkItems<indexlib::index::SingleFieldMetaBuilder, index::FieldMetaBuildWorkItem>(
        _singleFieldMetaBuilders, batch);

    // TODO (远轫）这里写的太丑了，在这里释放batch非常危险！
    // 通过 hook 并行析构参与 build 的 docs，确保不会干预 build 任务
    size_t batchSize = batch->GetBatchSize();
    size_t destructDocParallelNum = std::min(std::max((size_t)1, batchSize / 512), _buildThreadPool->GetThreadNum());
    for (int32_t parallelIdx = 0; parallelIdx < destructDocParallelNum; ++parallelIdx) {
        _buildThreadPool->AddBatchFinishHook([this, destructDocParallelNum, parallelIdx, batch, docBatchMemUse]() {
            size_t batchSize = batch->GetBatchSize();
            for (size_t i = parallelIdx; i < batchSize; i += destructDocParallelNum) {
                batch->ReleaseDoc(i);
            }
            // 按照平均值来释放。因为doc的实际内存估计值（理论上）可能变化，而我们必须强保证Allocate()和Free()的总和相等。
            // 但我们可以接受少量估计偏差。另外，doc的内存估计还是有一定性能开销的
            _docBatchMemController->Free((docBatchMemUse + parallelIdx) / destructDocParallelNum);
        });
    };
    if (_buildMode == OpenOptions::CONSISTENT_BATCH) {
        _buildThreadPool->WaitCurrentBatchWorkItemsFinish();
        _normalBuildingSegment->PostBuildActions(lastLocator, maxTimestamp, maxTTL, addDocCount);
    }
    AUTIL_INTERVAL_LOG2(60, INFO, "End generation workitems for batch build [%ld] docs, use [%ld] ms, log interval 60s",
                        batchSize, autil::TimeUtility::currentTimeInMicroSeconds() / 1000 - startTimeInMs);
    return Status::OK();
}

void NormalTabletParallelBuilder::WaitFinish()
{
    if (_buildThreadPool == nullptr) {
        return;
    }
    _buildThreadPool->WaitFinish();
}

} // namespace indexlib::table
