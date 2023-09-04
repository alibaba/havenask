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
#include "indexlib/table/kv_table/index_task/KeyValueOptimizeMergeStrategy.h"

#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentTopologyInfo.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/SegmentMergePlan.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KeyValueOptimizeMergeStrategy);

std::pair<Status, std::shared_ptr<MergePlan>>
KeyValueOptimizeMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto tabletData = context->GetTabletData();
    auto levelInfo = tabletData->GetOnDiskVersion().GetSegmentDescriptions()->GetLevelInfo();
    bool needMerge = false;
    if (!CheckLevelInfo(levelInfo, needMerge)) {
        return {Status::InvalidArgs(), nullptr};
    }
    if (!needMerge) {
        return {Status::OK(), std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN)};
    }
    std::vector<SegmentMergePlan> segMergePlans(levelInfo->GetShardCount());
    FillSrcSegments(levelInfo, segMergePlans);
    FillTargetSegments(context->GetMaxMergedSegmentId(), tabletData, segMergePlans);

    std::map<segmentid_t, framework::SegmentTopologyInfo> topoInfos;
    FillTopologyInfo(segMergePlans, levelInfo, topoInfos);

    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*IndexTaskResourceType=*/MERGE_PLAN);
    FillMergePlan(context, segMergePlans, levelInfo, topoInfos, mergePlan);
    return std::make_pair(Status::OK(), mergePlan);
}

void KeyValueOptimizeMergeStrategy::FillSrcSegments(const std::shared_ptr<framework::LevelInfo>& levelInfo,
                                                    std::vector<SegmentMergePlan>& plans)
{
    for (const auto& levelMeta : levelInfo->levelMetas) {
        if (levelMeta.topology == framework::topo_sequence) {
            for (auto segmentId : levelMeta.segments) {
                if (segmentId == INVALID_SEGMENTID) {
                    continue;
                }
                for (size_t i = 0; i < plans.size(); i++) {
                    auto& plan = plans[i];
                    plan.AddSrcSegment(segmentId);
                }
            }
        } else if (levelMeta.topology == framework::topo_hash_mod) {
            assert(plans.size() == levelMeta.segments.size());
            for (size_t i = 0; i < levelMeta.segments.size(); ++i) {
                segmentid_t segmentId = levelMeta.segments[i];
                if (segmentId == INVALID_SEGMENTID) {
                    continue;
                }
                plans[i].AddSrcSegment(segmentId);
            }
        } else {
            assert(false);
        }
    }
}

void KeyValueOptimizeMergeStrategy::FillTargetSegments(segmentid_t baseId,
                                                       const std::shared_ptr<framework::TabletData>& tabletData,
                                                       std::vector<SegmentMergePlan>& plans)
{
    for (size_t i = 0; i < plans.size(); i++) {
        if (plans[i].GetSrcSegmentCount() == 0) {
            continue;
        }
        const auto& segmentIds = plans[i].GetSrcSegments();
        framework::SegmentInfo currentSegmentInfo;
        if (tabletData) {
            for (auto segmentId : segmentIds) {
                SegmentMergePlan::UpdateTargetSegmentInfo(segmentId, tabletData, &currentSegmentInfo);
            }
        }
        currentSegmentInfo.mergedSegment = true;
        currentSegmentInfo.shardId = i;
        segmentid_t targetSegmentId = ++baseId;
        plans[i].AddTargetSegment(targetSegmentId, currentSegmentInfo);
    }
}

void KeyValueOptimizeMergeStrategy::FillTopologyInfo(const std::vector<SegmentMergePlan>& plans,
                                                     const std::shared_ptr<framework::LevelInfo>& levelInfo,
                                                     std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos)
{
    for (size_t i = 0; i < plans.size(); i++) {
        if (plans[i].GetSrcSegmentCount() == 0) {
            continue;
        }
        const auto& targetSegments = plans[i].GetTargetSegments();
        for (const auto& targetSegment : targetSegments) {
            segmentid_t targetSegmentId = targetSegment.first;
            framework::SegmentTopologyInfo targetTopology;
            targetTopology.levelTopology = levelInfo->GetTopology();
            targetTopology.levelIdx = levelInfo->GetLevelCount() - 1;
            targetTopology.isBottomLevel = true;
            targetTopology.shardCount = levelInfo->GetShardCount();
            targetTopology.shardIdx = i;
            topoInfos[targetSegmentId] = targetTopology;
        }
    }
}

void KeyValueOptimizeMergeStrategy::FillMergePlan(
    const framework::IndexTaskContext* context, const std::vector<SegmentMergePlan>& plans,
    const std::shared_ptr<framework::LevelInfo>& levelInfo,
    const std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos, std::shared_ptr<MergePlan>& mergePlan)
{
    for (const auto& plan : plans) {
        mergePlan->AddMergePlan(plan);
    }
    framework::Version targetVersion = MergePlan::CreateNewVersion(mergePlan, context);
    CollectSegmentDescriptions(levelInfo, topoInfos, mergePlan, targetVersion);
    mergePlan->SetTargetVersion(std::move(targetVersion));
}

bool KeyValueOptimizeMergeStrategy::CheckLevelInfo(const std::shared_ptr<framework::LevelInfo>& levelInfo,
                                                   bool& needMerge) const
{
    if (levelInfo == nullptr) {
        AUTIL_LOG(ERROR, "levelInfo is nullptr");
        return false;
    }
    if (levelInfo->GetShardCount() == 0 || levelInfo->GetLevelCount() == 0) {
        AUTIL_LOG(ERROR, "invalid levelInfo shardCount[%lu] levelCount[%lu]", levelInfo->GetShardCount(),
                  levelInfo->GetLevelCount());
        return false;
    }
    if (levelInfo->GetTopology() != framework::topo_hash_mod) {
        AUTIL_LOG(ERROR,
                  "kv and kkv table optimize merge strategy only support framework::topo_hash_mod,"
                  " [%s] is unsupported",
                  framework::LevelMeta::TopologyToStr(levelInfo->GetTopology()).c_str());
        return false;
    }
    const auto& levelMetas = levelInfo->levelMetas;
    for (size_t i = 0; i < levelMetas.size(); i++) {
        const auto& topology = levelMetas[i].topology;
        if (topology != framework::topo_hash_mod && (i != 0 || topology != framework::topo_sequence)) {
            AUTIL_LOG(ERROR, "level[%lu] topology[%s] is not valid for KeyValueOptimizeMergeStrategy", i,
                      framework::LevelMeta::TopologyToStr(topology).c_str());
            return false;
        }

        size_t maxLevel = levelInfo->GetLevelCount() - 1;
        bool hasValidSegment = false;
        for (segmentid_t segmentId : levelMetas[i].segments) {
            hasValidSegment |= (segmentId != INVALID_SEGMENTID);
        }
        if (i != maxLevel && hasValidSegment) {
            needMerge = true;
        }
    }
    if (!needMerge) {
        AUTIL_LOG(WARN, "no segments need to be merged.");
    }
    return true;
}

void KeyValueOptimizeMergeStrategy::CollectSegmentDescriptions(
    const std::shared_ptr<framework::LevelInfo>& originalLevelInfo,
    const std::map<segmentid_t, framework::SegmentTopologyInfo>& topoInfos, const std::shared_ptr<MergePlan>& mergePlan,
    framework::Version& targetVersion)
{
    auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
    for (auto iter = topoInfos.begin(); iter != topoInfos.end(); iter++) {
        auto segId = iter->first;
        auto topoInfo = iter->second;
        framework::LevelMeta& levelMeta = levelInfo->levelMetas[topoInfo.levelIdx];
        assert(topoInfo.levelTopology == framework::topo_hash_mod);
        assert(topoInfo.shardIdx < levelMeta.segments.size());
        levelMeta.segments[topoInfo.shardIdx] = segId;
    }
    for (size_t i = 0; i < mergePlan->Size(); i++) {
        const auto& segmentMergePlan = mergePlan->GetSegmentMergePlan(i);
        uint32_t minLevel = std::numeric_limits<uint32_t>::max();
        uint32_t maxLevel = 0;
        for (size_t j = 0; j < segmentMergePlan.GetSrcSegmentCount(); j++) {
            uint32_t levelIdx = 0;
            uint32_t inLevelIdx = 0;
            bool ret = originalLevelInfo->FindPosition(segmentMergePlan.GetSrcSegmentId(j), levelIdx, inLevelIdx);
            (void)ret;
            assert(ret);
            minLevel = std::min(minLevel, levelIdx);
            maxLevel = std::max(maxLevel, levelIdx);
        }
        for (uint32_t levelIdx = minLevel; levelIdx < maxLevel; ++levelIdx) {
            if (levelIdx > 0 && levelIdx < levelInfo->GetLevelCount()) {
                auto& levelMeta = levelInfo->levelMetas[levelIdx];
                levelMeta.cursor = (levelMeta.cursor + 1) % levelMeta.segments.size();
            }
        }
    }
}

} // namespace indexlibv2::table
