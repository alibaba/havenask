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
#include "indexlib/table/index_task/merger/ShardBasedMergeStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlibv2::config;
using namespace indexlibv2::framework;
using namespace indexlib::file_system;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, ShardBasedMergeStrategy);

std::pair<Status, std::shared_ptr<MergePlan>>
ShardBasedMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto tabletData = context->GetTabletData();
    const auto& version = tabletData->GetOnDiskVersion();
    if (version.GetSegmentCount() == 0) {
        return std::make_pair(Status::OK(), std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN));
    }
    auto segDescs = version.GetSegmentDescriptions();
    auto levelInfo = segDescs->GetLevelInfo();
    if (!levelInfo) {
        AUTIL_LOG(ERROR, "no level info, create merge plan failed");
        return std::make_pair(Status::Corruption("create merge plan failed"), nullptr);
    }
    LevelTopology topology = levelInfo->GetTopology();
    size_t levelNum = levelInfo->GetLevelCount();
    if (topology != framework::topo_hash_mod || levelNum < 2) {
        AUTIL_LOG(ERROR,
                  "kv and kkv table shard_based merge strategy only support topo_hash_mod,"
                  " [%s], levelNum [%ld] is unsupported",
                  LevelMeta::TopologyToStr(topology).c_str(), levelNum);
        return std::make_pair(Status::ConfigError(), nullptr);
    }

    auto mergeConfig = context->GetMergeConfig();
    assert(mergeConfig.GetMergeStrategyStr() == GetName());
    SetParameter(mergeConfig.GetMergeStrategyParameter());
    auto [status, mergeTag] = GenerateMergeTag(levelInfo, tabletData);
    if (!status.IsOK()) {
        return std::make_pair(status, nullptr);
    }
    return DoCreateMergePlan(context, mergeTag);
}

std::pair<Status, std::shared_ptr<MergePlan>>
ShardBasedMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context,
                                           const std::vector<std::vector<bool>>& mergeTag)
{
    segmentid_t lastMergeSegmentId = context->GetMaxMergedSegmentId();
    auto tabletData = context->GetTabletData();
    const auto& version = tabletData->GetOnDiskVersion();
    auto segDescs = version.GetSegmentDescriptions();
    auto levelInfo = segDescs->GetLevelInfo();
    assert(levelInfo);
    auto mergePlan = std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN);
    SegmentMergePlan plan;
    framework::SegmentInfo segmentInfo;
    segmentInfo.mergedSegment = true;
    for (auto segmentId : levelInfo->levelMetas[0].segments) {
        if (segmentId != INVALID_SEGMENTID) {
            plan.AddSrcSegment(segmentId);
            SegmentMergePlan::UpdateTargetSegmentInfo(segmentId, tabletData, &segmentInfo);
        }
    }

    size_t shardCount = levelInfo->GetShardCount();
    size_t levelNum = levelInfo->GetLevelCount();
    std::map<segmentid_t, SegmentTopologyInfo> topoInfos;
    for (size_t i = 0; i < shardCount; ++i) {
        auto currentPlan = plan;
        auto currentSegmentInfo = segmentInfo;
        for (size_t levelIdx = 1; levelIdx < levelNum; ++levelIdx) {
            if (!mergeTag[levelIdx][i] && currentPlan.GetSrcSegmentCount() == 0) {
                continue;
            }
            segmentid_t segId = levelInfo->levelMetas[levelIdx].segments[i];
            if (segId != INVALID_SEGMENTID) {
                currentPlan.AddSrcSegment(segId);
                SegmentMergePlan::UpdateTargetSegmentInfo(segId, tabletData, &currentSegmentInfo);
            }

            if (!mergeTag[levelIdx][i]) {
                segmentid_t targetSegId = ++lastMergeSegmentId;
                SegmentTopologyInfo targetTopology;
                targetTopology.levelTopology = framework::topo_hash_mod;
                targetTopology.levelIdx = levelIdx;
                targetTopology.isBottomLevel = (levelIdx == (levelNum - 1));
                targetTopology.shardCount = shardCount;
                targetTopology.shardIdx = i;

                currentSegmentInfo.shardId = i;
                currentSegmentInfo.shardCount = shardCount;
                topoInfos[targetSegId] = targetTopology;
                currentPlan.AddTargetSegment(targetSegId, currentSegmentInfo);
                mergePlan->AddMergePlan(currentPlan);

                AUTIL_LOG(INFO, "Merge Plan generated [%s]", currentPlan.ToString().c_str());

                currentPlan = SegmentMergePlan();
            }
        }
    }
    framework::Version targetVersion = MergePlan::CreateNewVersion(mergePlan, context);
    CollectSegmentDescriptions(levelInfo, topoInfos, mergePlan, targetVersion);
    mergePlan->SetTargetVersion(std::move(targetVersion));
    return std::make_pair(Status::OK(), mergePlan);
}

void ShardBasedMergeStrategy::CollectSegmentDescriptions(
    const std::shared_ptr<indexlibv2::framework::LevelInfo>& originalLevelInfo,
    const std::map<segmentid_t, SegmentTopologyInfo>& topoInfos, const std::shared_ptr<MergePlan>& mergePlan,
    framework::Version& targetVersion)
{
    auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
    for (auto iter = topoInfos.begin(); iter != topoInfos.end(); iter++) {
        auto segId = iter->first;
        auto topoInfo = iter->second;
        LevelMeta& levelMeta = levelInfo->levelMetas[topoInfo.levelIdx];
        assert(topoInfo.levelTopology == framework::topo_hash_mod);
        assert(topoInfo.shardIdx < levelMeta.segments.size());
        levelMeta.segments[topoInfo.shardIdx] = segId;
    }
    for (size_t i = 0; i < mergePlan->Size(); i++) {
        const auto& segmentMergePlan = mergePlan->GetSegmentMergePlan(i);
        uint32_t minLevel = numeric_limits<uint32_t>::max();
        uint32_t maxLevel = 0;
        for (size_t j = 0; j < segmentMergePlan.GetSrcSegmentCount(); j++) {
            uint32_t levelIdx = 0;
            uint32_t inLevelIdx = 0;
            bool ret = originalLevelInfo->FindPosition(segmentMergePlan.GetSrcSegmentId(j), levelIdx, inLevelIdx);
            assert(ret);
            (void)ret;
            minLevel = min(minLevel, levelIdx);
            maxLevel = max(maxLevel, levelIdx);
        }
        for (uint32_t levelIdx = minLevel; levelIdx < maxLevel; ++levelIdx) {
            if (levelIdx > 0 && levelIdx < levelInfo->GetLevelCount()) {
                auto& levelMeta = levelInfo->levelMetas[levelIdx];
                levelMeta.cursor = (levelMeta.cursor + 1) % levelMeta.segments.size();
            }
        }
    }
}

std::pair<Status, vector<vector<bool>>>
ShardBasedMergeStrategy::GenerateMergeTag(const std::shared_ptr<indexlibv2::framework::LevelInfo>& levelInfo,
                                          const std::shared_ptr<framework::TabletData>& tabletData)
{
    size_t levelNum = levelInfo->GetLevelCount();
    assert(levelNum >= 2);
    vector<vector<size_t>> segmentsSize;
    vector<size_t> levelsSize;
    auto status = GetLevelSizeInfo(levelInfo, tabletData, segmentsSize, levelsSize);
    vector<vector<bool>> mergeTag(levelNum);
    if (!status.IsOK()) {
        return std::make_pair(status, mergeTag);
    }
    vector<size_t> levelsThreshold;
    GetLevelThreshold(levelNum, *levelsSize.rbegin(), levelsThreshold);

    size_t shardCount = levelInfo->GetShardCount();
    for (size_t i = 0; i < shardCount; ++i) {
        size_t incSize = levelsSize[0] / shardCount;
        segmentsSize[1][i] += incSize;
        levelsSize[1] += incSize;
    }

    for (size_t levelIdx = 1; levelIdx < levelNum - 1; ++levelIdx) {
        mergeTag[levelIdx].resize(shardCount, false);
        if (_disableLastLevelMerge && levelIdx == levelNum - 2) {
            break;
        }
        size_t cursor = levelInfo->levelMetas[levelIdx].cursor;
        while (levelsSize[levelIdx] > levelsThreshold[levelIdx]) {
            levelsSize[levelIdx] -= segmentsSize[levelIdx][cursor];
            levelsSize[levelIdx + 1] += segmentsSize[levelIdx][cursor];
            segmentsSize[levelIdx + 1][cursor] += segmentsSize[levelIdx][cursor];
            mergeTag[levelIdx][cursor] = true;
            cursor = (cursor + 1) % shardCount;
        }
    }
    mergeTag.rbegin()->resize(shardCount, false);
    return std::make_pair(Status::OK(), mergeTag);
}

void ShardBasedMergeStrategy::GetLevelThreshold(uint32_t levelNum, size_t bottomLevelSize,
                                                vector<size_t>& levelsThreshold)
{
    assert(levelsThreshold.empty());

    uint32_t levelPercent = CalculateLevelPercent(_spaceAmplification, levelNum);
    levelsThreshold.resize(levelNum, 0);
    *levelsThreshold.rbegin() = bottomLevelSize;
    for (int64_t i = levelNum - 2; i >= 0; --i) {
        levelsThreshold[i] = levelsThreshold[i + 1] * (double)levelPercent / 100;
        AUTIL_LOG(INFO, "level [%ld], Threshold [%lu]", i, levelsThreshold[i]);
    }
}

uint32_t ShardBasedMergeStrategy::CalculateLevelPercent(double spaceAmplification, uint32_t levelNum)
{
    uint32_t begin = 0;
    uint32_t end = 100;

    while (begin < end - 1) {
        uint32_t mid = (begin + end) / 2;
        if (CalculateSpaceAmplification(mid, levelNum) > spaceAmplification) {
            end = mid;
        } else {
            begin = mid;
        }
    }
    AUTIL_LOG(INFO, "level percent [%u]", begin);
    return begin;
}

double ShardBasedMergeStrategy::CalculateSpaceAmplification(uint32_t percent, uint32_t levelNum)
{
    double spaceAmplification = 0;
    double currentLevelSize = 1;
    for (size_t i = 0; i < levelNum; ++i) {
        spaceAmplification += currentLevelSize;
        currentLevelSize *= (double)percent / 100;
    }
    return spaceAmplification;
}

Status ShardBasedMergeStrategy::GetLevelSizeInfo(const std::shared_ptr<indexlibv2::framework::LevelInfo>& levelInfo,
                                                 const std::shared_ptr<framework::TabletData>& tabletData,
                                                 vector<vector<size_t>>& segmentsSize, vector<size_t>& actualLevelSize)
{
    assert(segmentsSize.empty());
    assert(actualLevelSize.empty());

    size_t levelNum = levelInfo->GetLevelCount();

    segmentsSize.resize(levelNum, {});
    actualLevelSize.resize(levelNum, 0);
    for (size_t levelIdx = 0; levelIdx < levelNum; ++levelIdx) {
        size_t levelSize = 0;
        vector<size_t>& curLevelSizes = segmentsSize[levelIdx];
        size_t docCount = 0;
        for (auto segmentId : levelInfo->levelMetas[levelIdx].segments) {
            if (segmentId == INVALID_SEGMENTID) {
                curLevelSizes.push_back(0);
                continue;
            }
            auto segment = tabletData->GetSegment(segmentId);
            auto [status, segmentSize] =
                segment->GetSegmentDirectory()->GetIDirectory()->GetDirectorySize("").StatusWith();
            RETURN_IF_STATUS_ERROR(status, "get segment size failed");
            curLevelSizes.push_back(segmentSize);
            levelSize += segmentSize;
            docCount += segment->GetSegmentInfo()->docCount;
        }
        actualLevelSize[levelIdx] = levelSize;
        AUTIL_LOG(INFO, "level [%lu], actualSize [%lu], docCount [%lu]", levelIdx, actualLevelSize[levelIdx], docCount);
    }
    return Status::OK();
}

void ShardBasedMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    std::map<std::string, std::string> kvMap;

    std::vector<std::string> configItems;
    autil::StringUtil::fromString(param.GetStrategyConditions(), configItems, ";");
    for (const auto& item : configItems) {
        std::vector<std::string> kvPairs = autil::StringUtil::split(item, '=');
        if (kvPairs.size() != 2) {
            AUTIL_LOG(WARN, "invalid config item[%s]", item.c_str());
            continue;
        }
        kvMap[kvPairs[0]] = kvPairs[1];
    }

    for (const auto& [k, v] : kvMap) {
        if (k == "space_amplification") {
            double value = 0.0;
            if (autil::StringUtil::fromString(v, value)) {
                _spaceAmplification = value;
            }
        } else if (k == "disable_last_level_merge") {
            bool value = false;
            if (autil::StringUtil::fromString(v, value)) {
                _disableLastLevelMerge = value;
            }
        }
    }

    AUTIL_LOG(INFO, "space amplification [%lf], disable last level merge[%s]", _spaceAmplification,
              _disableLastLevelMerge ? "true" : "false");
}

} // namespace indexlibv2::table
