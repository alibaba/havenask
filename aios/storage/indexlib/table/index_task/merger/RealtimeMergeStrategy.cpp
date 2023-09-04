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
#include "indexlib/table/index_task/merger/RealtimeMergeStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2 { namespace table {
namespace {
using indexlibv2::framework::Segment;
} // namespace

AUTIL_LOG_SETUP(indexlib.table, RealtimeMergeStrategy);

std::pair<Status, std::shared_ptr<MergePlan>>
RealtimeMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto mergeConfig = context->GetMergeConfig();
    auto [status, mergePlan] = DoCreateMergePlan(context, mergeConfig.GetMergeStrategyParameter());
    RETURN2_IF_STATUS_ERROR(status, nullptr, "Create merge plan failed");
    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    return {Status::OK(), mergePlan};
}

std::pair<Status, std::shared_ptr<MergePlan>>
RealtimeMergeStrategy::DoCreateMergePlan(const framework::IndexTaskContext* context,
                                         const config::MergeStrategyParameter& param)
{
    auto mergePlan = std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN);
    const auto& version = context->GetTabletData()->GetOnDiskVersion();
    if (0 == version.GetSegmentCount()) {
        return {Status::OK(), mergePlan};
    }
    auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
    // TODO: (by yijie.zhang) check topology from version
    if (levelInfo != nullptr and levelInfo->GetTopology() != framework::topo_sequence) {
        auto status = Status::Corruption("Topology [%s] not supported, only support sequence topology",
                                         framework::LevelMeta::TopologyToStr(levelInfo->GetTopology()).c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, nullptr};
    }

    auto [status, params] = ExtractParams(param);
    if (!status.IsOK()) {
        return {status, nullptr};
    }
    _params = std::move(params);

    std::vector<std::shared_ptr<Segment>> srcSegments = CollectSrcSegments(context->GetTabletData());
    size_t totalSize = 0;
    size_t maxInPlanSegmentCount = std::min(_params.maxSegmentCount, srcSegments.size());
    SegmentMergePlan segmentMergePlan;
    for (size_t i = 0; i < maxInPlanSegmentCount and totalSize < _params.maxTotalMergeSize; ++i) {
        segmentMergePlan.AddSrcSegment(srcSegments[i]->GetSegmentId());

        std::shared_ptr<Segment> srcSegment = srcSegments[i];
        auto ret = srcSegment->GetSegmentDirectory()->GetIDirectory()->GetDirectorySize(/*path=*/"");
        auto status = ret.Status();
        RETURN2_IF_STATUS_ERROR(status, nullptr, "get directory size fail, dir[%s]",
                                srcSegment->GetSegmentDirectory()->GetLogicalPath().c_str());
        auto segmentSize = ret.result;
        totalSize += segmentSize;
    }
    if (segmentMergePlan.GetSrcSegmentCount() > 0) {
        mergePlan->AddMergePlan(segmentMergePlan);
    }
    AUTIL_LOG(INFO, "Segment merge plans created.");
    return std::make_pair(Status::OK(), mergePlan);
}

std::pair<Status, struct RealtimeMergeParams>
RealtimeMergeStrategy::ExtractParams(const config::MergeStrategyParameter& param)
{
    struct RealtimeMergeParams params;
    std::string mergeParam = param.GetLegacyString();
    autil::StringTokenizer st(mergeParam, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (uint32_t i = 0; i < st.getNumTokens(); i++) {
        autil::StringTokenizer innerSt(st[i], "=",
                                       autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (innerSt.getNumTokens() != 2) {
            auto status = Status::InvalidArgs("Invalid parameter for merge strategy: [%s]", st[i].c_str());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return {status, params};
        }

        if (innerSt[0] == "rt-max-segment-count") {
            size_t maxSegmentCount;
            if (!autil::StringUtil::fromString(innerSt[1].c_str(), maxSegmentCount)) {
                auto status = Status::InvalidArgs("Invalid parameter for merge strategy: [%s]", st[i].c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.maxSegmentCount = maxSegmentCount;
            continue;
        } else if (innerSt[0] == "rt-max-total-merge-size") {
            size_t maxTotalMergeSize;
            if (!autil::StringUtil::fromString(innerSt[1].c_str(), maxTotalMergeSize)) {
                auto status = Status::InvalidArgs("Invalid parameter for merge strategy: [%s]", st[i].c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, params};
            }
            params.maxTotalMergeSize = maxTotalMergeSize << 20;
            continue;
        }
        AUTIL_LOG(INFO, "Skipping parameter not intended for for realtime merge strategy: [%s]:[%s]",
                  innerSt[0].c_str(), innerSt[1].c_str());
    }
    return {Status::OK(), params};
}

std::vector<std::shared_ptr<Segment>>
RealtimeMergeStrategy::CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData)
{
    std::vector<std::shared_ptr<Segment>> segments;
    auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_BUILT);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        auto segment = *iter;
        if (!segment->GetSegmentInfo()->mergedSegment) {
            segments.push_back(segment);
        }
    }
    return segments;
}

}} // namespace indexlibv2::table
