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
#include "indexlib/merger/merge_strategy/shard_based_merge_strategy.h"

#include "autil/StringUtil.h"
#include "indexlib/merger/merge_strategy/key_value_optimize_merge_strategy.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, ShardBasedMergeStrategy);

ShardBasedMergeStrategy::ShardBasedMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                 const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mSpaceAmplification(1.5)
{
}

ShardBasedMergeStrategy::~ShardBasedMergeStrategy() {}

void ShardBasedMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    vector<string> kvPairs;
    StringUtil::fromString(param.strategyConditions, kvPairs, "=");
    assert(kvPairs.size() == 2);
    mSpaceAmplification = 1.5;
    if (kvPairs.size() == 2 && kvPairs[0] == "space_amplification") {
        double value = 0.0;
        if (StringUtil::fromString(kvPairs[1], value)) {
            mSpaceAmplification = value;
        }
    }
    IE_LOG(INFO, "space amplification [%lf]", mSpaceAmplification);
}

string ShardBasedMergeStrategy::GetParameter() const
{
    assert(false);
    return "";
}

MergeTask ShardBasedMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                   const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    size_t levelNum = levelInfo.GetLevelCount();
    if (topology != indexlibv2::framework::topo_hash_mod || levelNum < 2) {
        INDEXLIB_FATAL_ERROR(
            UnSupported,
            "kv and kkv table shard_based merge strategy only support indexlibv2::framework::topo_hash_mod,"
            " [%s] is unsupported",
            indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    map<segmentid_t, SegmentMergeInfo> segMergeInfoMap;
    for (const auto& segMergeInfo : segMergeInfos) {
        segMergeInfoMap[segMergeInfo.segmentId] = segMergeInfo;
    }
    vector<vector<bool>> mergeTag = GenerateMergeTag(levelInfo, segMergeInfoMap);
    return GenerateMergeTask(levelInfo, segMergeInfoMap, mergeTag);
}

MergeTask ShardBasedMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                              const indexlibv2::framework::LevelInfo& levelInfo)
{
    KeyValueOptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}

uint32_t ShardBasedMergeStrategy::CalculateLevelPercent(double spaceAmplification, uint32_t levelNum) const
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
    IE_LOG(INFO, "level percent [%u]", begin);
    return begin;
}

double ShardBasedMergeStrategy::CalculateSpaceAmplification(uint32_t percent, uint32_t levelNum) const
{
    double spaceAmplification = 0;
    double currentLevelSize = 1;
    for (size_t i = 0; i < levelNum; ++i) {
        spaceAmplification += currentLevelSize;
        currentLevelSize *= (double)percent / 100;
    }
    return spaceAmplification;
}

void ShardBasedMergeStrategy::GetLevelSizeInfo(const indexlibv2::framework::LevelInfo& levelInfo,
                                               const map<segmentid_t, SegmentMergeInfo>& segMergeInfoMap,
                                               vector<vector<size_t>>& segmentsSize, vector<size_t>& actualLevelSize)
{
    assert(segmentsSize.empty());
    assert(actualLevelSize.empty());

    size_t levelNum = levelInfo.GetLevelCount();

    segmentsSize.resize(levelNum, {});
    actualLevelSize.resize(levelNum, 0);
    for (size_t levelIdx = 0; levelIdx < levelNum; ++levelIdx) {
        size_t levelSize = 0;
        vector<size_t>& curLevelSizes = segmentsSize[levelIdx];
        size_t docCount = 0;
        for (auto segmentId : levelInfo.levelMetas[levelIdx].segments) {
            if (segmentId == INVALID_SEGMENTID) {
                curLevelSizes.push_back(0);
                continue;
            }
            auto iter = segMergeInfoMap.find(segmentId);
            assert(iter != segMergeInfoMap.cend());
            curLevelSizes.push_back(iter->second.segmentSize);
            levelSize += iter->second.segmentSize;
            docCount += iter->second.segmentInfo.docCount;
        }
        actualLevelSize[levelIdx] = levelSize;
        IE_LOG(INFO, "level [%lu], actualSize [%lu], docCount [%lu]", levelIdx, actualLevelSize[levelIdx], docCount);
    }
}

void ShardBasedMergeStrategy::GetLevelThreshold(uint32_t levelNum, size_t bottomLevelSize,
                                                vector<size_t>& levelsThreshold)
{
    assert(levelsThreshold.empty());

    uint32_t levelPercent = CalculateLevelPercent(mSpaceAmplification, levelNum);
    levelsThreshold.resize(levelNum, 0);
    *levelsThreshold.rbegin() = bottomLevelSize;
    for (int64_t i = levelNum - 2; i >= 0; --i) {
        levelsThreshold[i] = levelsThreshold[i + 1] * (double)levelPercent / 100;
        IE_LOG(INFO, "level [%ld], Threshold [%lu]", i, levelsThreshold[i]);
    }
}

vector<vector<bool>>
ShardBasedMergeStrategy::GenerateMergeTag(const indexlibv2::framework::LevelInfo& levelInfo,
                                          const map<segmentid_t, SegmentMergeInfo>& segMergeInfoMap)
{
    size_t levelNum = levelInfo.GetLevelCount();
    assert(levelNum >= 2);
    vector<vector<size_t>> segmentsSize;
    vector<size_t> levelsSize;
    GetLevelSizeInfo(levelInfo, segMergeInfoMap, segmentsSize, levelsSize);
    vector<size_t> levelsThreshold;
    GetLevelThreshold(levelNum, *levelsSize.rbegin(), levelsThreshold);

    size_t columnCount = levelInfo.GetShardCount();
    for (size_t i = 0; i < columnCount; ++i) {
        size_t incSize = levelsSize[0] / columnCount;
        segmentsSize[1][i] += incSize;
        levelsSize[1] += incSize;
    }

    vector<vector<bool>> mergeTag(levelNum);
    for (size_t levelIdx = 1; levelIdx < levelNum - 1; ++levelIdx) {
        mergeTag[levelIdx].resize(columnCount, false);
        size_t cursor = levelInfo.levelMetas[levelIdx].cursor;
        while (levelsSize[levelIdx] > levelsThreshold[levelIdx]) {
            levelsSize[levelIdx] -= segmentsSize[levelIdx][cursor];
            levelsSize[levelIdx + 1] += segmentsSize[levelIdx][cursor];
            segmentsSize[levelIdx + 1][cursor] += segmentsSize[levelIdx][cursor];
            mergeTag[levelIdx][cursor] = true;
            cursor = (cursor + 1) % columnCount;
        }
    }
    mergeTag.rbegin()->resize(columnCount, false);
    return mergeTag;
}

MergeTask ShardBasedMergeStrategy::GenerateMergeTask(const indexlibv2::framework::LevelInfo& levelInfo,
                                                     const map<segmentid_t, SegmentMergeInfo>& segMergeInfoMap,
                                                     const vector<vector<bool>>& mergeTag)
{
    MergeTask task;
    MergePlan plan;
    for (auto segmentId : levelInfo.levelMetas[0].segments) {
        auto iter = segMergeInfoMap.find(segmentId);
        assert(iter != segMergeInfoMap.cend());
        plan.AddSegment(iter->second);
    }

    size_t columnCount = levelInfo.GetShardCount();
    size_t levelNum = levelInfo.GetLevelCount();
    for (size_t i = 0; i < columnCount; ++i) {
        MergePlan currentPlan = plan;
        for (size_t levelIdx = 1; levelIdx < levelNum; ++levelIdx) {
            if (!mergeTag[levelIdx][i] && currentPlan.Empty()) {
                continue;
            }

            segmentid_t segId = levelInfo.levelMetas[levelIdx].segments[i];

            if (segId != INVALID_SEGMENTID) {
                auto iter = segMergeInfoMap.find(segId);
                assert(iter != segMergeInfoMap.cend());
                currentPlan.AddSegment(iter->second);
            }

            if (!mergeTag[levelIdx][i]) {
                SegmentTopologyInfo targetTopology;
                targetTopology.levelTopology = indexlibv2::framework::topo_hash_mod;
                targetTopology.levelIdx = levelIdx;
                targetTopology.isBottomLevel = (levelIdx == (levelNum - 1));
                targetTopology.columnCount = columnCount;
                targetTopology.columnIdx = i;
                currentPlan.SetTargetTopologyInfo(0, targetTopology);
                SegmentInfo& segInfo = currentPlan.GetTargetSegmentInfo(0);
                segInfo.shardId = i;
                task.AddPlan(currentPlan);

                IE_LOG(INFO, "Merge Plan generated [%s]", currentPlan.ToString().c_str());

                currentPlan = MergePlan();
            }
        }
    }
    return task;
}
}} // namespace indexlib::merger
