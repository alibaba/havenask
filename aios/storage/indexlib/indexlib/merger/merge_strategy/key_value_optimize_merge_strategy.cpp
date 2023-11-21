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
#include "indexlib/merger/merge_strategy/key_value_optimize_merge_strategy.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <memory>
#include <stdint.h>
#include <vector>

#include "alog/Logger.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, KeyValueOptimizeMergeStrategy);

KeyValueOptimizeMergeStrategy::KeyValueOptimizeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                             const config::IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

KeyValueOptimizeMergeStrategy::~KeyValueOptimizeMergeStrategy() {}

void KeyValueOptimizeMergeStrategy::SetParameter(const config::MergeStrategyParameter& param) {}

std::string KeyValueOptimizeMergeStrategy::GetParameter() const { return ""; }

MergeTask KeyValueOptimizeMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                         const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_hash_mod) {
        INDEXLIB_FATAL_ERROR(
            UnSupported,
            "kv and kkv table optimize merge strategy only support indexlibv2::framework::topo_hash_mod,"
            " [%s] is unsupported",
            indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }
    if (!NeedMerge(segMergeInfos, levelInfo)) {
        return MergeTask();
    }

    map<segmentid_t, size_t> segMergeInfoMap;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        segMergeInfoMap[segMergeInfos[i].segmentId] = i;
    }
    uint32_t columnCount = segMergeInfos[0].segmentInfo.shardCount;
    vector<MergePlan> mergePlans(columnCount);
    for (const auto& levelMeta : levelInfo.levelMetas) {
        if (levelMeta.topology == indexlibv2::framework::topo_sequence) {
            for (auto segmentId : levelMeta.segments) {
                if (segmentId == INVALID_SEGMENTID) {
                    continue;
                }
                size_t segIdx = segMergeInfoMap[segmentId];
                for (auto& mergePlan : mergePlans) {
                    mergePlan.AddSegment(segMergeInfos[segIdx]);
                }
            }
        } else if (levelMeta.topology == indexlibv2::framework::topo_hash_mod) {
            assert(mergePlans.size() == levelMeta.segments.size());
            for (size_t i = 0; i < levelMeta.segments.size(); ++i) {
                segmentid_t segmentId = levelMeta.segments[i];
                if (segmentId == INVALID_SEGMENTID) {
                    continue;
                }
                size_t segIdx = segMergeInfoMap[segmentId];
                mergePlans[i].AddSegment(segMergeInfos[segIdx]);
            }
        } else {
            assert(false);
        }
    }
    MergeTask mergeTask;
    SegmentTopologyInfo segTopoInfo;
    segTopoInfo.levelTopology = topology;
    segTopoInfo.levelIdx = levelInfo.GetLevelCount() - 1;
    segTopoInfo.isBottomLevel = true;
    segTopoInfo.columnCount = columnCount;
    for (size_t i = 0; i < mergePlans.size(); ++i) {
        auto& mergePlan = mergePlans[i];
        if (mergePlan.GetSegmentCount() != 0) {
            segTopoInfo.columnIdx = i;
            mergePlan.SetTargetTopologyInfo(0, segTopoInfo);
            SegmentInfo& segInfo = mergePlan.GetTargetSegmentInfo(0);
            segInfo.shardId = i;
            mergeTask.AddPlan(mergePlan);
        }
    }
    return mergeTask;
}

MergeTask KeyValueOptimizeMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                                    const indexlibv2::framework::LevelInfo& levelInfo)
{
    return CreateMergeTask(segMergeInfos, levelInfo);
}

bool KeyValueOptimizeMergeStrategy::NeedMerge(const SegmentMergeInfos& segMergeInfos,
                                              const indexlibv2::framework::LevelInfo& levelInfo) const
{
    for (const auto& segMergeInfo : segMergeInfos) {
        if (segMergeInfo.levelIdx != levelInfo.GetLevelCount() - 1) {
            return true;
        }
    }
    return false;
}
}} // namespace indexlib::merger
