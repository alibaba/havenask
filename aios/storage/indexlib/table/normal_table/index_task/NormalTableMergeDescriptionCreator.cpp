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
#include "indexlib/table/normal_table/index_task/NormalTableMergeDescriptionCreator.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDataInfo.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "indexlib/table/normal_table/index_task/BucketMapOperation.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/NormalTableResourceCreator.h"
#include "indexlib/table/normal_table/index_task/OpLog2PatchOperation.h"
#include "indexlib/table/normal_table/index_task/ReclaimMapOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableMergeDescriptionCreator);

NormalTableMergeDescriptionCreator::NormalTableMergeDescriptionCreator(
    const std::shared_ptr<config::ITabletSchema>& schema, const std::string& mergeStrategy,
    const std::string& compactionType, bool isOptimizeMerge)
    : CommonMergeDescriptionCreator(schema)
    , _isOptimizeMerge(isOptimizeMerge)
    , _compactionType(compactionType)
    , _mergeStrategy(mergeStrategy)
{
    auto [status, sortDescs] = schema->GetRuntimeSettings().GetValue<config::SortDescriptions>("sort_descriptions");
    if (!status.IsOK() && !status.IsNotFound()) {
        assert(false);
    } else {
        _isSortedMerge = !sortDescs.empty();
    }
}

NormalTableMergeDescriptionCreator::~NormalTableMergeDescriptionCreator() {}

std::pair<Status, framework::IndexOperationDescription>
NormalTableMergeDescriptionCreator::CreateIndexMergeOperationDescription(
    const std::shared_ptr<MergePlan>& mergePlan, const std::shared_ptr<config::IIndexConfig>& indexConfig,
    framework::IndexOperationId operationId, size_t planIdx)
{
    auto [status, opDesc] = CommonMergeDescriptionCreator::CreateIndexMergeOperationDescription(mergePlan, indexConfig,
                                                                                                operationId, planIdx);
    if (!status.IsOK()) {
        return std::make_pair(status, opDesc);
    }
    opDesc.AddParameter(index::DocMapper::GetDocMapperType(),
                        NormalTableResourceCreator::GetReclaimMapName(planIdx, _isSortedMerge));
    const auto& invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    if (invertedIndexConfig && invertedIndexConfig->GetShardingType() ==
                                   indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_IS_SHARDING) {
        std::string shardingIndexName;
        if (indexlibv2::config::InvertedIndexConfig::GetIndexNameFromShardingIndexName(
                invertedIndexConfig->GetIndexName(), shardingIndexName)) {
            opDesc.AddParameter(SHARD_INDEX_NAME, shardingIndexName);
        } else {
            AUTIL_LOG(ERROR, "can not get shard index name for indexer with shading");
            return std::make_pair(Status::ConfigError("can not get shard index name for indexer with shading"), opDesc);
        }
    }
    const auto& attributeIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
    if (attributeIndexConfig) {
        if (attributeIndexConfig->GetSliceCount() > 1 && attributeIndexConfig->GetSliceIdx() != -1) {
            opDesc.AddParameter(index::ATTRIBUTE_SLICE_COUNT, std::to_string(attributeIndexConfig->GetSliceCount()));
            opDesc.AddParameter(index::ATTRIBUTE_SLICE_IDX, std::to_string(attributeIndexConfig->GetSliceIdx()));
        }
    }

    if (invertedIndexConfig != nullptr && invertedIndexConfig->HasTruncate()) {
        if (_bucketMapOpId != framework::INVALID_INDEX_OPERATION_ID) {
            opDesc.AddDepend(_bucketMapOpId);
        }
    }

    opDesc.AddParameter(indexlib::index::OPTIMIZE_MERGE, _isOptimizeMerge);
    return std::make_pair(Status::OK(), opDesc);
}

std::unique_ptr<framework::IndexOperationDescription>
NormalTableMergeDescriptionCreator::CreateInitMergeOperationDescription(const std::shared_ptr<MergePlan>& mergePlan,
                                                                        framework::IndexOperationId operationId)
{
    return CreateOpLog2PatchOperationDescription(_schema, operationId);
}

std::pair<framework::IndexOperationId, std::vector<std::unique_ptr<framework::IndexOperationDescription>>>
NormalTableMergeDescriptionCreator::CreateBeforeMergeIndexOperationDescriptions(
    framework::IndexOperationId operationId, framework::IndexOperationDescription* initOpDesc, size_t mergePlanIdx)
{
    std::vector<std::unique_ptr<framework::IndexOperationDescription>> indexOperationDescs;
    auto reclaimMapOpDesc =
        std::make_unique<framework::IndexOperationDescription>(operationId, ReclaimMapOperation::OPERATION_TYPE);
    assert(reclaimMapOpDesc != nullptr);
    reclaimMapOpDesc->AddParameter(NORMAL_TABLE_COMPACTION_TYPE, _compactionType);
    reclaimMapOpDesc->AddParameter(MERGE_PLAN_INDEX, std::to_string(mergePlanIdx));

    if (initOpDesc != nullptr) {
        reclaimMapOpDesc->AddDepend(initOpDesc->GetId());
        reclaimMapOpDesc->AddParameter(DEPENDENT_OPERATION_ID, std::to_string(initOpDesc->GetId()));
    }

    framework::IndexOperationId reclaimMapOpId = reclaimMapOpDesc->GetId();
    indexOperationDescs.emplace_back(std::move(reclaimMapOpDesc));
    bool needBucketMap = false;
    auto indexConfigs = _schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
        if (invertedIndexConfig->HasTruncate()) {
            needBucketMap = true;
            break;
        }
    }
    if (needBucketMap) {
        auto bucketMapOpDesc =
            std::make_unique<framework::IndexOperationDescription>(++operationId, BucketMapOperation::OPERATION_TYPE);
        bucketMapOpDesc->AddDepend(reclaimMapOpId);
        bucketMapOpDesc->AddParameter(MERGE_PLAN_INDEX, std::to_string(mergePlanIdx));
        bucketMapOpDesc->AddParameter(index::DocMapper::GetDocMapperType(),
                                      NormalTableResourceCreator::GetReclaimMapName(mergePlanIdx, _isSortedMerge));
        _bucketMapOpId = bucketMapOpDesc->GetId();
        indexOperationDescs.emplace_back(std::move(bucketMapOpDesc));
    }
    return {reclaimMapOpId, std::move(indexOperationDescs)};
}

std::unique_ptr<framework::IndexOperationDescription>
NormalTableMergeDescriptionCreator::CreateOpLog2PatchOperationDescription(
    const std::shared_ptr<config::ITabletSchema>& schema, framework::IndexOperationId operationId)
{
    if (!schema->GetIndexConfig(indexlib::index::OPERATION_LOG_INDEX_TYPE_STR,
                                indexlib::index::OPERATION_LOG_INDEX_NAME)) {
        return nullptr;
    }
    return std::unique_ptr<framework::IndexOperationDescription>(
        new framework::IndexOperationDescription(operationId, OpLog2PatchOperation::OPERATION_TYPE));
}

std::vector<std::shared_ptr<config::IIndexConfig>> NormalTableMergeDescriptionCreator::GetSupportConfigs()
{
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs = _schema->GetIndexConfigs();
    std::vector<std::shared_ptr<config::IIndexConfig>> allNeededIndexConfigs;
    for (const auto& indexConfig : indexConfigs) {
        if (indexConfig->GetIndexType() == indexlib::index::INVERTED_INDEX_TYPE_STR) {
            const auto& shardIndexConfig =
                std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
            if (shardIndexConfig && shardIndexConfig->GetShardingType() ==
                                        indexlibv2::config::InvertedIndexConfig::IndexShardingType::IST_NEED_SHARDING) {
                const auto& shardingIndexConfigs = shardIndexConfig->GetShardingIndexConfigs();
                allNeededIndexConfigs.insert(allNeededIndexConfigs.end(), shardingIndexConfigs.begin(),
                                             shardingIndexConfigs.end());
            }
        }
        const auto& attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(indexConfig);
        // TODO: by yijie.zhang support unique attribute slice
        if (MergeStrategyDefine::OPTIMIZE_MERGE_STRATEGY_NAME == _mergeStrategy && attributeConfig &&
            attributeConfig->GetSliceCount() > 1 && !attributeConfig->IsUniqEncode()) {
            auto sliceAttributeConfigs = attributeConfig->CreateSliceAttributeConfigs(attributeConfig->GetSliceCount());
            allNeededIndexConfigs.insert(allNeededIndexConfigs.end(), sliceAttributeConfigs.begin(),
                                         sliceAttributeConfigs.end());
            continue;
        }
        allNeededIndexConfigs.emplace_back(indexConfig);
    }

    // add slice attribute
    return allNeededIndexConfigs;
}

} // namespace indexlibv2::table
