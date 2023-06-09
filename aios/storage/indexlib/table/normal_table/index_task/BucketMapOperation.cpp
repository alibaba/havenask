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
#include "indexlib/table/normal_table/index_task/BucketMapOperation.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/truncate/BucketMapCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeUtil.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/NormalTableResourceCreator.h"
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, BucketMapOperation);

BucketMapOperation::BucketMapOperation(const framework::IndexOperationDescription& description)
    : framework::IndexOperation(description.GetId(), description.UseOpFenceDir())
    , _description(description)
{
}

Status BucketMapOperation::Execute(const framework::IndexTaskContext& context)
{
    auto [st, segmentMergeInfos] = PrepareSegmentMergeInfos(context);
    RETURN_IF_STATUS_ERROR(st, "prepare segment merge infos failed");

    int64_t idx = 0;
    if (!_description.GetParameter(MERGE_PLAN_INDEX, idx)) {
        AUTIL_LOG(ERROR, "get merge plan idx from desc [%s] failed", ToJsonString(_description, true).c_str());
        return Status::Corruption("reclaim map op,get merge plan idx from desc [%s] failed",
                                  autil::legacy::ToJsonString(_description, true).c_str());
    }
    auto resourceManager = context.GetResourceManager();
    auto [status, bucketMaps] = CreateBucketMaps(context, segmentMergeInfos);
    RETURN_IF_STATUS_ERROR(status, "create bucket maps failed.");
    for (auto& [truncateProfileName, bucketMap] : bucketMaps) {
        const std::string bucketMapName = indexlib::index::BucketMapCreator::GetBucketMapName(idx, truncateProfileName);
        std::shared_ptr<indexlib::index::BucketMap> toCommitBucketMap = bucketMap;
        // in case of failover, try load resource first
        status = resourceManager->LoadResource(bucketMapName, indexlib::index::BucketMap::GetBucketMapType(),
                                               toCommitBucketMap);
        if (status.IsNoEntry()) {
            status = resourceManager->CreateResource(bucketMapName, indexlib::index::BucketMap::GetBucketMapType(),
                                                     toCommitBucketMap);
            *toCommitBucketMap = std::move(*bucketMap);
            RETURN_IF_STATUS_ERROR(status, "create bucket map resource failed");
            status = resourceManager->CommitResource(bucketMapName);
        }
        RETURN_IF_STATUS_ERROR(status, "commit bucket map failed");
    }
    AUTIL_LOG(INFO, "execute bucket map operation success");
    return Status::OK();
}

std::pair<Status, indexlib::index::BucketMaps>
BucketMapOperation::CreateBucketMaps(const framework::IndexTaskContext& context,
                                     const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos) const
{
    indexlib::index::BucketMaps bucketMaps;
    std::string docMapperName;
    if (!_description.GetParameter(index::DocMapper::GetDocMapperType(), docMapperName)) {
        AUTIL_LOG(ERROR, "could't find doc mapper, type [%s]", index::DocMapper::GetDocMapperType().c_str());
        return {Status::Corruption(), bucketMaps};
    }
    std::shared_ptr<index::DocMapper> docMapper;
    auto resourceManager = context.GetResourceManager();
    auto st = resourceManager->LoadResource(docMapperName, index::DocMapper::GetDocMapperType(), docMapper);
    RETURN2_IF_STATUS_ERROR(st, bucketMaps, "load doc mapper failed");

    auto schema = context.GetTabletSchema();
    auto [status, truncateProfileConfigs] =
        schema->GetSetting<std::vector<indexlibv2::config::TruncateProfileConfig>>("truncate_profiles");
    RETURN2_IF_STATUS_ERROR(status, bucketMaps, "get truncate profile settings failed.");

    auto option = context.GetTabletOptions();
    auto truncateOptionConfig =
        std::make_shared<indexlibv2::config::TruncateOptionConfig>(option->GetMergeConfig().GetTruncateStrategys());
    truncateOptionConfig->Init(schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR),
                               truncateProfileConfigs);

    if (truncateOptionConfig->GetTruncateProfiles().empty()) {
        AUTIL_LOG(INFO, "no need create bucket maps for truncate-profiles is empty");
        return {Status::OK(), bucketMaps};
    }
    int64_t idx = 0;
    if (!_description.GetParameter(MERGE_PLAN_INDEX, idx)) {
        AUTIL_LOG(ERROR, "get merge plan idx from desc [%s] failed",
                  autil::legacy::ToJsonString(_description, true).c_str());
        return {Status::Corruption("get merge plan idx failed"), bucketMaps};
    }
    indexlib::index::TruncateAttributeReaderCreator attrReaderCreator(schema, segmentMergeInfos, docMapper);
    return indexlib::index::BucketMapCreator::CreateBucketMaps(truncateOptionConfig, schema, docMapper,
                                                               &attrReaderCreator, idx);
}

std::pair<Status, indexlibv2::index::IIndexMerger::SegmentMergeInfos>
BucketMapOperation::PrepareSegmentMergeInfos(const framework::IndexTaskContext& context) const
{
    auto tabletData = context.GetTabletData();
    assert(tabletData);
    SegmentMergePlan segMergePlan;
    auto status = GetSegmentMergePlan(context, segMergePlan);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get segment merge plan failed.");
        return std::make_pair(status, index::IIndexMerger::SegmentMergeInfos());
    }
    indexlibv2::index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    segMergeInfos.relocatableGlobalRoot = context.GetGlobalRelocatableFolder();
    for (size_t i = 0; i < segMergePlan.GetSrcSegmentCount(); ++i) {
        auto sourceSegment = MergeUtil::GetSourceSegment(segMergePlan.GetSrcSegmentId(i), tabletData);
        segMergeInfos.srcSegments.emplace_back(std::move(sourceSegment));
    }
    return std::make_pair(Status::OK(), segMergeInfos);
}

Status BucketMapOperation::GetSegmentMergePlan(const framework::IndexTaskContext& context,
                                               SegmentMergePlan& segMergePlan) const
{
    auto resourceManager = context.GetResourceManager();
    std::shared_ptr<MergePlan> mergePlan;
    auto status = resourceManager->LoadResource<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN, mergePlan);
    RETURN_IF_STATUS_ERROR(status, "load merge plan resource failed");
    int64_t idx = 0;
    if (!_description.GetParameter(MERGE_PLAN_INDEX, idx)) {
        AUTIL_LOG(ERROR, "get merge plan idx from desc [%s] failed",
                  autil::legacy::ToJsonString(_description, true).c_str());
        return Status::Corruption("get merge plan idx failed");
    }
    segMergePlan = mergePlan->GetSegmentMergePlan(idx);
    return Status::OK();
}

} // namespace indexlibv2::table
