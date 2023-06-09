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
#include "indexlib/table/index_task/merger/MergeUtil.h"

#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MergeUtil);

index::IIndexMerger::SourceSegment MergeUtil::GetSourceSegment(segmentid_t srcSegmentId,
                                                               const std::shared_ptr<framework::TabletData>& tabletData)
{
    docid_t baseDocId = 0;
    index::IIndexMerger::SourceSegment sourceSegment;
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_UNSPECIFY);
    for (auto iter = slice.begin(); iter != slice.end(); iter++) {
        if (srcSegmentId == (*iter)->GetSegmentId()) {
            sourceSegment.baseDocid = baseDocId;
            sourceSegment.segment = *iter;
            break;
        }
        baseDocId += (*iter)->GetSegmentInfo()->docCount;
    }
    return sourceSegment;
}

Status MergeUtil::RewriteMergeConfig(const framework::IndexTaskContext& context)
{
    auto& mergeConfig = context.GetTabletOptions()->MutableMergeConfig();
    AUTIL_LOG(INFO, "before rewrite get package config list from merge config: %s",
              autil::legacy::ToJsonString(mergeConfig.GetPackageFileTagConfigList()).c_str());
    auto designateTask = context.GetDesignateTask();
    if (designateTask == std::nullopt) {
        AUTIL_LOG(ERROR, "DesignateTask not set in context, use default merge config");
        return Status::OK();
    }
    auto [taskType, taskName] = designateTask.value();
    auto taskConfig = context.GetTabletOptions()->GetIndexTaskConfig(taskType, taskName);
    if (taskConfig == std::nullopt) {
        AUTIL_LOG(INFO, "target IndexTaskConfig for taskName [%s] not exist, use default merge config.",
                  taskName.c_str());
        return Status::OK();
    }
    auto [status, taskMergeConfig] = taskConfig.value().GetSetting<indexlibv2::config::MergeConfig>("merge_config");
    if (!status.IsOK()) {
        if (status.IsNotFound()) {
            AUTIL_LOG(
                ERROR,
                "target IndexTaskConfig for taskName [%s] does not contian merge config, use default merge config.",
                taskName.c_str());
            return Status::OK();
        }
        AUTIL_LOG(ERROR, "get merge config from IndexTaskConfig fail. [taskType:%s,taskName:%s]", taskType.c_str(),
                  taskName.c_str());
        return status;
    }
    AUTIL_LOG(INFO, "rewrite merge config in context from IndexTaskConfig [taskType:%s,taskName:%s]", taskType.c_str(),
              taskName.c_str());
    mergeConfig = taskMergeConfig;
    AUTIL_LOG(INFO, "after rewrite get package config list from merge config: %s",
              autil::legacy::ToJsonString(mergeConfig.GetPackageFileTagConfigList()).c_str());
    return Status::OK();
}

}} // namespace indexlibv2::table