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
#include "indexlib/table/normal_table/index_task/merger/BalanceTreeMergeStrategy.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"

namespace indexlibv2::table {

namespace {
using indexlibv2::framework::Segment;
using indexlibv2::framework::TabletData;
} // namespace

AUTIL_LOG_SETUP(indexlib.table, BalanceTreeMergeStrategy);

std::pair<Status, BalanceTreeMergeParams>
BalanceTreeMergeStrategy::ExtractParams(const config::MergeStrategyParameter& param) const
{
    BalanceTreeMergeParams params;
    std::string mergeParam = param.GetLegacyString();
    autil::StringUtil::trim(mergeParam);
    if (mergeParam.empty()) {
        AUTIL_LOG(INFO, "no specified merge param");
        return {Status::OK(), params};
    }

    autil::StringTokenizer st(mergeParam, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < st.getNumTokens(); i++) {
        autil::StringTokenizer kvStr(st[i], "=",
                                     autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (kvStr.getNumTokens() != 2) {
            auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return {status, params};
        }
        if (kvStr[0] == "max-doc-count") {
            uint32_t maxDocCount;
            if (!autil::StringUtil::strToUInt32(kvStr[1].c_str(), maxDocCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.maxDocCount = maxDocCount;
        } else if (kvStr[0] == "base-doc-count") {
            uint32_t baseDocCount;
            if (!autil::StringUtil::strToUInt32(kvStr[1].c_str(), baseDocCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.baseDocCount = baseDocCount;
        } else if (kvStr[0] == "conflict-segment-number") {
            uint32_t conflictSegmentNumber;
            if (!autil::StringUtil::strToUInt32(kvStr[1].c_str(), conflictSegmentNumber)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.conflictSegmentNum = conflictSegmentNumber;
        } else if (kvStr[0] == "conflict-delete-percent") {
            uint32_t conflictDelPercent;
            if (!autil::StringUtil::strToUInt32(kvStr[1].c_str(), conflictDelPercent)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.conflictDelPercent = conflictDelPercent;
        } else if (kvStr[0] == "max-valid-doc-count") {
            uint32_t maxValidDocCount;
            if (!autil::StringUtil::strToUInt32(kvStr[1].c_str(), maxValidDocCount)) {
                auto status = Status::InvalidArgs("invalid parameter [%s] for merge strategy", mergeParam.c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.maxValidDocCount = maxValidDocCount;
        }
        AUTIL_LOG(INFO, "Skipping parameter not intended for for balance tree merge strategy: [%s]:[%s]",
                  kvStr[0].c_str(), kvStr[1].c_str());
    }
    return {Status::OK(), params};
}

std::pair<Status, std::shared_ptr<MergePlan>>
BalanceTreeMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto [status, mergePlan] = DoCreateMergePlan(context);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "Create merge plan failed");
    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    AUTIL_LOG(INFO, "Create merge plan %s", autil::legacy::ToJsonString(mergePlan, true).c_str());
    return {Status::OK(), mergePlan};
}

void BalanceTreeMergeStrategy::InitBalanceTree(const framework::IndexTaskContext* context,
                                               std::vector<SegmentMergeInfo>& segmentMergeInfos,
                                               std::vector<SegmentQueue>& balanceTree) const
{
    auto tabletData = context->GetTabletData();
    segmentMergeInfos = CollectSegmentMergeInfos(tabletData);
    for (const auto& segmentMergeInfo : segmentMergeInfos) {
        TryInsertBalanceTree(segmentMergeInfo, balanceTree);
    }
}

Status BalanceTreeMergeStrategy::CheckParam(const framework::IndexTaskContext* context)
{
    auto tabletData = context->GetTabletData();
    const auto& version = tabletData->GetOnDiskVersion();
    auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
    if (levelInfo != nullptr && levelInfo->GetTopology() != framework::topo_sequence) {
        AUTIL_LOG(ERROR, "[%s] is not supported, only support sequence topology",
                  framework::LevelMeta::TopologyToStr(levelInfo->GetTopology()).c_str());
        return Status::InvalidArgs();
    }
    auto mergeConfig = context->GetMergeConfig();
    assert(mergeConfig.GetMergeStrategyStr() == GetName() or
           mergeConfig.GetMergeStrategyStr() == MergeStrategyDefine::COMBINED_MERGE_STRATEGY_NAME);
    auto [st, params] = ExtractParams(mergeConfig.GetMergeStrategyParameter());
    if (!st.IsOK()) {
        return st;
    }
    _params = std::move(params);
    return Status::OK();
}

std::pair<Status, std::shared_ptr<MergePlan>>
BalanceTreeMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context)
{
    const auto& version = context->GetTabletData()->GetOnDiskVersion();
    if (version.GetSegmentCount() == 0) {
        AUTIL_LOG(INFO, "empty version, no need create merge plan")
        return std::make_pair(Status::OK(), std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN));
    }
    auto st = CheckParam(context);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "invalid params.");
        return {st, nullptr};
    }
    std::vector<SegmentMergeInfo> segmentMergeInfos;
    std::vector<SegmentQueue> balanceTree;
    InitBalanceTree(context, segmentMergeInfos, balanceTree);

    auto mergePlan = std::make_shared<MergePlan>(/*name=*/MERGE_PLAN, /*type=*/MERGE_PLAN);
    SegmentMergePlan segmentMergePlan;
    bool isConflictDelPercent = false;
    for (uint32_t i = 0; i < balanceTree.size(); ++i) {
        uint32_t currentLayer = i;
        int32_t minLayer = balanceTree.size();
        while (true) {
            if (HasLargeDelPercentSegment(balanceTree[currentLayer])) {
                isConflictDelPercent = true;
            } else if (!(balanceTree[currentLayer].size() >= _params.conflictSegmentNum)) {
                break;
            }

            SegmentMergeInfo mergedSegInfo;
            uint32_t segNum = std::min((size_t)_params.conflictSegmentNum, balanceTree[currentLayer].size());
            for (uint32_t j = 0; j < segNum; j++) {
                SegmentMergeInfo node = balanceTree[currentLayer].top();
                uint32_t docCount = node.docCount - node.deletedDocCount;
                mergedSegInfo.docCount += docCount;
                balanceTree[currentLayer].pop();

                if (node.segmentId != INVALID_SEGMENTID) {
                    segmentMergePlan.AddSrcSegment(node.segmentId);
                }
            }

            int32_t layerIdx = TryInsertBalanceTree(mergedSegInfo, balanceTree);
            i = TrySetNextTreeLevel(layerIdx, i, minLayer);
        }
    }
    TryAddSegmentMergePlan(mergePlan, segmentMergePlan, isConflictDelPercent);
    CreateTopLayerMergePlan(mergePlan, segmentMergeInfos);
    return {Status::OK(), mergePlan};
}

void BalanceTreeMergeStrategy::TryAddSegmentMergePlan(const std::shared_ptr<MergePlan>& mergePlan,
                                                      const SegmentMergePlan& segmentMergePlan,
                                                      bool isConflictDelPercent) const
{
    if (segmentMergePlan.GetSrcSegmentCount() > 1 ||
        (segmentMergePlan.GetSrcSegmentCount() == 1 && isConflictDelPercent)) {
        AUTIL_LOG(INFO, "Merge plan generated [%s] by BalanceTree", segmentMergePlan.ToString().c_str());
        mergePlan->AddMergePlan(segmentMergePlan);
    }
    return;
}

std::vector<BalanceTreeMergeStrategy::SegmentMergeInfo>
BalanceTreeMergeStrategy::CollectSegmentMergeInfos(const std::shared_ptr<framework::TabletData>& tabletData) const
{
    std::vector<BalanceTreeMergeStrategy::SegmentMergeInfo> segmentMergeInfos;
    auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segment = *iter;
        if (!segment->GetSegmentInfo()->mergedSegment) {
            continue;
        }
        auto result = NormalTableMergeStrategyUtil::GetDeleteDocCount(segment.get());
        assert(result.first.IsOK());
        segmentMergeInfos.push_back(
            {segment->GetSegmentId(), segment->GetSegmentInfo()->docCount, static_cast<uint64_t>(result.second)});
    }
    return segmentMergeInfos;
}

void BalanceTreeMergeStrategy::CreateTopLayerMergePlan(const std::shared_ptr<MergePlan>& mergePlan,
                                                       const std::vector<SegmentMergeInfo>& segmentMergeInfos) const
{
    for (const auto& segmentMergeInfo : segmentMergeInfos) {
        int32_t layerIdx = GetLayer(segmentMergeInfo.docCount);
        if (-1 == layerIdx && LargerThanDelPercent(segmentMergeInfo)) {
            uint32_t curValidDocCount = segmentMergeInfo.docCount - segmentMergeInfo.deletedDocCount;
            if (curValidDocCount > _params.maxValidDocCount) {
                continue;
            }
            SegmentMergePlan segmentMergePlan;
            segmentMergePlan.AddSrcSegment(segmentMergeInfo.segmentId);
            mergePlan->AddMergePlan(segmentMergePlan);
            AUTIL_LOG(INFO, "Merge plan generated [%s] by BalanceTree",
                      autil::legacy::ToJsonString(mergePlan, true).c_str());
            // only add one merge plan in old repo coded in 2011... why?
            return;
        }
    }
}

int32_t BalanceTreeMergeStrategy::GetLayer(uint64_t docCount) const
{
    if (docCount > _params.maxDocCount) {
        return -1;
    }

    int32_t layerIdx = 0;
    uint64_t layerMaxDocCount = _params.baseDocCount;
    while (true) {
        if (docCount <= layerMaxDocCount) {
            AUTIL_LOG(DEBUG, "docCount: [%lu], layer: [%d]", docCount, layerIdx);
            return layerIdx;
        }
        layerIdx++;
        layerMaxDocCount *= _params.conflictSegmentNum;
    }
}
int32_t BalanceTreeMergeStrategy::TryInsertBalanceTree(const SegmentMergeInfo& segmentMergeInfo,
                                                       std::vector<SegmentQueue>& balanceTree) const
{
    int32_t layerIdx = GetLayer(segmentMergeInfo.docCount);
    if (layerIdx != -1) {
        if (balanceTree.size() < (size_t)(layerIdx + 1)) {
            balanceTree.resize(layerIdx + 1);
        }
        balanceTree[layerIdx].push(segmentMergeInfo);
    }
    return layerIdx;
}

int32_t BalanceTreeMergeStrategy::TrySetNextTreeLevel(int32_t mergedLayerId, uint32_t currentId,
                                                      int32_t& minLayerId) const
{
    if (mergedLayerId == -1) {
        // the doc cnt in mergedSegmentInfo is greater than _params.maxDocCount,
        // no need change tree level
        return currentId;
    }
    if (mergedLayerId < currentId) {
        // the mergedSegmentInfo will be inserted to upper level in balance tree
        // so forward the tree level
        minLayerId = std::min(mergedLayerId, minLayerId);
        return minLayerId - 1;
    }
    // the mergedSegmentInfo will be inserted to lower level in balance tree
    // no need change tree level
    return currentId;
}

bool BalanceTreeMergeStrategy::HasLargeDelPercentSegment(SegmentQueue& segQueue) const
{
    std::vector<SegmentMergeInfo> segmentMergeInfos;
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

bool BalanceTreeMergeStrategy::LargerThanDelPercent(const SegmentMergeInfo& segmentMergeInfo) const
{
    if (0 == segmentMergeInfo.docCount) {
        return false;
    }
    uint32_t curPercent = double(segmentMergeInfo.deletedDocCount) / segmentMergeInfo.docCount * 100;
    return curPercent > _params.conflictDelPercent;
}

std::pair<Status, BalanceTreeMergeParams> BalanceTreeMergeStrategy::TEST_ExtractParams(const std::string& paramStr)
{
    config::MergeStrategyParameter param;
    param.SetLegacyString(paramStr);
    return ExtractParams(param);
}

} // namespace indexlibv2::table
