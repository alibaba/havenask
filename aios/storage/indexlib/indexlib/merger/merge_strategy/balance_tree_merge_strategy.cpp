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
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, BalanceTreeMergeStrategy);

BalanceTreeMergeStrategy::BalanceTreeMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                                   const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
    , mBaseDocCount(0)
    , mMaxDocCount(0)
    , mConflictSegNum(0)
    , mConflictDelPercent(100)
    , mMaxValidDocCount((uint32_t)-1)
{
}

BalanceTreeMergeStrategy::~BalanceTreeMergeStrategy() {}

void BalanceTreeMergeStrategy::SetParameter(const MergeStrategyParameter& param)
{
    string mergeParam = param.GetLegacyString();
    StringTokenizer st(mergeParam, ";", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() < 3) {
        INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", mergeParam.c_str());
    }
    for (uint32_t i = 0; i < st.getNumTokens(); i++) {
        StringTokenizer st2(st[i], "=", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (st2.getNumTokens() != 2) {
            INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", st[i].c_str());
        }

        if (st2[0] == "base-doc-count") {
            uint32_t baseDocCount;
            if (!StringUtil::strToUInt32(st2[1].c_str(), baseDocCount)) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mBaseDocCount = baseDocCount;
        } else if (st2[0] == "max-doc-count") {
            uint32_t maxDocCount;
            if (!StringUtil::strToUInt32(st2[1].c_str(), maxDocCount)) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mMaxDocCount = maxDocCount;
        } else if (st2[0] == "conflict-segment-number") {
            uint32_t conflictSegNum;
            if (!StringUtil::strToUInt32(st2[1].c_str(), conflictSegNum)) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mConflictSegNum = conflictSegNum;
        } else if (st2[0] == "conflict-delete-percent") {
            uint32_t conflictDelPercent;
            if (!StringUtil::strToUInt32(st2[1].c_str(), conflictDelPercent) && (conflictDelPercent > 100)) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mConflictDelPercent = conflictDelPercent;
        } else if (st2[0] == "max-valid-doc-count") {
            uint32_t maxValidDcoCount;
            if (!StringUtil::strToUInt32(st2[1].c_str(), maxValidDcoCount)) {
                INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]",
                               st2[1].c_str());
            }
            mMaxValidDocCount = maxValidDcoCount;
        } else {
            INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for merge strategy: [%s]", +st2[0].c_str());
        }
    }
}

string BalanceTreeMergeStrategy::GetParameter() const
{
    stringstream ss;
    ss << "base-doc-count=" << mBaseDocCount << ";"
       << "max-doc-count=" << mMaxDocCount << ";"
       << "conflict-segment-number=" << mConflictSegNum << ";"
       << "conflict-delete-percent=" << mConflictDelPercent << ";"
       << "max-valid-doc-count=" << mMaxValidDocCount;
    return ss.str();
}

MergeTask BalanceTreeMergeStrategy::CreateMergeTask(const SegmentMergeInfos& segMergeInfos,
                                                    const indexlibv2::framework::LevelInfo& levelInfo)
{
    indexlibv2::framework::LevelTopology topology = levelInfo.GetTopology();
    if (topology != indexlibv2::framework::topo_sequence) {
        INDEXLIB_FATAL_ERROR(UnSupported, " [%s] is unsupported, only support sequence",
                             indexlibv2::framework::LevelMeta::TopologyToStr(topology).c_str());
    }

    MergePlan mergePlan;
    vector<SegmentQueue> balanceTree;

    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        int32_t layerIdx = GetLayer(segMergeInfos[i].segmentInfo.docCount);
        if (layerIdx != -1) {
            if (balanceTree.size() < (size_t)(layerIdx + 1)) {
                balanceTree.resize(layerIdx + 1);
            }
            balanceTree[layerIdx].push(segMergeInfos[i]);
        }
    }

    bool isConflictDelPercent = false;
    for (uint32_t i = 0; i < balanceTree.size(); i++) {
        uint32_t currentLayer = i;
        while (true) {
            if (HasLargeDelPercentSegment(balanceTree[currentLayer])) {
                isConflictDelPercent = true;
            } else if (!(balanceTree[currentLayer].size() >= mConflictSegNum)) {
                break;
            }

            SegmentMergeInfo mergedSegInfo;
            uint32_t segNum = std::min((size_t)mConflictSegNum, balanceTree[currentLayer].size());
            for (uint32_t j = 0; j < segNum; j++) {
                SegmentMergeInfo node = balanceTree[currentLayer].top();
                uint32_t docCount = node.segmentInfo.docCount - node.deletedDocCount;
                mergedSegInfo.segmentInfo.docCount = mergedSegInfo.segmentInfo.docCount + docCount;
                balanceTree[currentLayer].pop();

                if (node.segmentId != INVALID_SEGMENTID) {
                    mergePlan.AddSegment(node);
                }
            }

            int32_t layerIdx = GetLayer(mergedSegInfo.segmentInfo.docCount);
            if (layerIdx != -1) {
                if (balanceTree.size() < (size_t)(layerIdx + 1)) {
                    balanceTree.resize(layerIdx + 1);
                }
                balanceTree[layerIdx].push(mergedSegInfo);

                if ((uint32_t)layerIdx < i) {
                    i = layerIdx - 1;
                }
            }
        }
    }

    MergeTask mergeTask;
    if (mergePlan.GetSegmentCount() > 1 || (mergePlan.GetSegmentCount() == 1 && isConflictDelPercent)) {
        IE_LOG(INFO, "Merge plan generated [%s] by BalanceTree", mergePlan.ToString().c_str());
        mergeTask.AddPlan(mergePlan);
    }
    CreateTopLayerMergePlan(mergeTask, segMergeInfos);

    return mergeTask;
}

void BalanceTreeMergeStrategy::CreateTopLayerMergePlan(MergeTask& mergeTask, const SegmentMergeInfos& segMergeInfos)
{
    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        int32_t layerIdx = GetLayer(segMergeInfos[i].segmentInfo.docCount);
        if (-1 == layerIdx && LargerThanDelPercent(segMergeInfos[i])) {
            uint32_t curValidDocCount = segMergeInfos[i].segmentInfo.docCount - segMergeInfos[i].deletedDocCount;
            if (curValidDocCount > mMaxValidDocCount) {
                continue;
            }
            MergePlan mergePlan;
            mergePlan.AddSegment(segMergeInfos[i]);
            mergeTask.AddPlan(mergePlan);
            IE_LOG(INFO, "Merge plan generated [%s] by BalanceTree", mergePlan.ToString().c_str());
            return;
        }
    }
}

int32_t BalanceTreeMergeStrategy::GetLayer(uint64_t docCount)
{
    if (docCount > mMaxDocCount) {
        return -1;
    }

    int32_t layerIdx = 0;
    uint64_t layerMaxDocCount = mBaseDocCount;
    while (true) {
        if (docCount <= layerMaxDocCount) {
            IE_LOG(DEBUG, "docCount: [%lu], layer: [%d]", docCount, layerIdx);
            return layerIdx;
        }
        layerIdx++;
        layerMaxDocCount *= mConflictSegNum;
    }
}

bool BalanceTreeMergeStrategy::HasLargeDelPercentSegment(SegmentQueue& segQueue)
{
    vector<SegmentMergeInfo> segmentMergeInfos;
    while (!segQueue.empty()) {
        if (LargerThanDelPercent(segQueue.top())) {
            for (size_t i = 0; i < segmentMergeInfos.size(); ++i) {
                segQueue.push(segmentMergeInfos[i]);
            }
            return true;
        }
        segmentMergeInfos.push_back(segQueue.top());
        segQueue.pop();
    }
    for (size_t i = 0; i < segmentMergeInfos.size(); ++i) {
        segQueue.push(segmentMergeInfos[i]);
    }
    return false;
}

bool BalanceTreeMergeStrategy::LargerThanDelPercent(const SegmentMergeInfo& segMergeInfo)
{
    if (0 == segMergeInfo.segmentInfo.docCount) {
        return false;
    }
    uint32_t curPercent = double(segMergeInfo.deletedDocCount) / segMergeInfo.segmentInfo.docCount * 100;
    return curPercent > mConflictDelPercent;
}

MergeTask BalanceTreeMergeStrategy::CreateMergeTaskForOptimize(const SegmentMergeInfos& segMergeInfos,
                                                               const indexlibv2::framework::LevelInfo& levelInfo)
{
    OptimizeMergeStrategy strategy(mSegDir, mSchema);
    return strategy.CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
