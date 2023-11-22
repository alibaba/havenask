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
#include "indexlib/table/index_task/merger/SpecificSegmentsMergeStrategy.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, SpecificSegmentsMergeStrategy);

std::pair<Status, std::shared_ptr<MergePlan>>
SpecificSegmentsMergeStrategy::CreateMergePlan(const framework::IndexTaskContext* context)
{
    auto mergeConfig = context->GetMergeConfig();
    auto [status, segmentIds] = ExtractParams(mergeConfig.GetMergeStrategyParameter());
    RETURN2_IF_STATUS_ERROR(status, nullptr, "extract param SpecificSegmentsMergeStrategy failed");

    const auto& version = context->GetTabletData()->GetOnDiskVersion();
    auto mergePlan = std::make_shared<MergePlan>(MERGE_PLAN, MERGE_PLAN);
    SegmentMergePlan segmentMergePlan;
    for (segmentid_t segmentId : segmentIds) {
        if (unlikely(not version.HasSegment(segmentId))) {
            std::vector<segmentid_t> versionSegmentIds;
            for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
                versionSegmentIds.push_back(version[i]);
            }
            std::string versionSegmentIdsStr = autil::StringUtil::toString(versionSegmentIds, ",");
            AUTIL_LOG(ERROR, "can't find segment [%d] in version [%d] with segments [%s]", segmentId,
                      version.GetVersionId(), versionSegmentIdsStr.c_str())
            return {Status::ConfigError("can't find segment [%d] in version [%d] with segments [%s]", segmentId,
                                        version.GetVersionId(), versionSegmentIdsStr.c_str()),
                    mergePlan};
        }
        segmentMergePlan.AddSrcSegment(segmentId);
    }
    if (segmentMergePlan.GetSrcSegmentCount() > 0) {
        mergePlan->AddMergePlan(segmentMergePlan);
    }
    AUTIL_LOG(INFO, "Segment merge plans created, source segment [%s]",
              autil::StringUtil::toString(segmentIds).c_str());

    RETURN2_IF_STATUS_ERROR(MergeStrategy::FillMergePlanTargetInfo(context, mergePlan), mergePlan,
                            "fill merge plan target info failed");
    return {Status::OK(), mergePlan};
}

std::pair<Status, std::vector<segmentid_t>>
SpecificSegmentsMergeStrategy::ExtractParams(const config::MergeStrategyParameter& param)
{
    std::string mergeParam = param.GetLegacyString();
    autil::StringUtil::trim(mergeParam);
    if (mergeParam.empty()) {
        return {Status::OK(), {}};
    }
    autil::StringTokenizer st(mergeParam, "=",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2 || st[0] != "merge_segments") {
        return {Status::ConfigError("Invalid parameter for merge strategy: [%s]", mergeParam.c_str()), {}};
    }
    std::vector<segmentid_t> segmentIds;
    autil::StringUtil::fromString(st[1], segmentIds, ",");
    return {Status::OK(), segmentIds};
}

} // namespace indexlibv2::table
