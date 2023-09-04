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
#include "indexlib/table/normal_table/index_task/NormalTableReclaimPlanCreator.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/index/deletionmap/DeletionMapPatchFileFinder.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchFileFinder.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/table/index_task/merger/PathMoveOperation.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/NormalTableReclaimOperation.h"
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"
#include "indexlib/util/Clock.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableReclaimPlanCreator);

const std::string NormalTableReclaimPlanCreator::TASK_TYPE = RECLAIM_TASK_TYPE;
const std::string NormalTableReclaimPlanCreator::DEFAULT_TASK_NAME = "__default_reclaim__";
const std::string NormalTableReclaimPlanCreator::DEFAULT_TASK_PERIOD = "period=18000"; // 5h

NormalTableReclaimPlanCreator::NormalTableReclaimPlanCreator(const std::string& taskName,
                                                             const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, params)
{
}

NormalTableReclaimPlanCreator::~NormalTableReclaimPlanCreator() {}

std::pair<Status, bool>
NormalTableReclaimPlanCreator::IsSegmentHasValidPatch(const std::shared_ptr<framework::Segment>& segment,
                                                      const framework::IndexTaskContext* taskContext)
{
    auto findPatchFiles =
        [taskContext, segment](index::PatchFileFinder* finder,
                               const std::shared_ptr<config::IIndexConfig>& indexConfig) -> std::pair<Status, bool> {
        index::PatchInfos patchInfos;
        auto status = finder->FindAllPatchFiles({segment}, indexConfig, &patchInfos);
        RETURN2_IF_STATUS_ERROR(status, false, "get [%s] patch for segment [%d] failed",
                                indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        for (const auto& [_, fileInfos] : patchInfos) {
            for (const auto& patchInfo : fileInfos.GetPatchFileInfos()) {
                segmentid_t destSegmentId = patchInfo.destSegment;
                if (taskContext->GetTabletData()->GetSegment(destSegmentId) != nullptr &&
                    destSegmentId != segment->GetSegmentId()) {
                    return {Status::OK(), true};
                }
            }
        }
        return {Status::OK(), false};
    };

    auto tabletSchema = taskContext->GetTabletSchema();
    std::vector<std::pair<std::string, std::shared_ptr<index::PatchFileFinder>>> patchFinders = {
        {index::ATTRIBUTE_INDEX_TYPE_STR, std::make_shared<index::AttributePatchFileFinder>()},
        {indexlib::index::INVERTED_INDEX_TYPE_STR, std::make_shared<indexlib::index::InvertedIndexPatchFileFinder>()},
        {index::DELETION_MAP_INDEX_TYPE_STR, std::make_shared<index::DeletionMapPatchFileFinder>()}};

    for (auto [indexType, finder] : patchFinders) {
        for (const auto& indexConfig : tabletSchema->GetIndexConfigs(indexType)) {
            auto [status, isFound] = findPatchFiles(finder.get(), indexConfig);
            RETURN2_STATUS_DIRECTLY_IF_ERROR(status, false);
            if (isFound) {
                return {Status::OK(), true};
            }
        }
    }
    return {Status::OK(), false};
}

std::tuple<Status, std::vector<framework::IndexOperationDescription>, framework::Version>
NormalTableReclaimPlanCreator::CreateOperationDescriptions(const framework::IndexTaskContext* taskContext)
{
    std::vector<framework::IndexOperationDescription> opDescs;
    indexlibv2::framework::IndexOperationId opId = 0;

    auto currentTimeInSeconds = taskContext->GetClock()->NowInSeconds();
    std::string docReclaimSource;
    std::optional<indexlibv2::framework::IndexOperationId> prepareIndexReclaimParamOp;
    auto taskConfig = taskContext->GetTabletOptions()->GetIndexTaskConfig(TASK_TYPE, _taskName);
    bool needReclaim = false;
    if (taskConfig != std::nullopt) {
        auto [status, _] = taskConfig.value().GetSetting<autil::legacy::Any>("doc_reclaim_source");
        if (status.IsOK()) {
            needReclaim = true;
            prepareIndexReclaimParamOp = opId++;
            framework::IndexOperationDescription opDesc(prepareIndexReclaimParamOp.value(),
                                                        PrepareIndexReclaimParamOperation::OPERATION_TYPE);
            opDesc.AddParameter(PrepareIndexReclaimParamOperation::TASK_STOP_TS_IN_SECONDS, currentTimeInSeconds);
            opDescs.push_back(opDesc);
        }
    }

    auto [status, enableTTL] = taskContext->GetTabletSchema()->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    if (status.IsOK() && enableTTL) {
        needReclaim = true;
    }

    if (status.IsNotFound()) {
        enableTTL = false;
    }

    if (!status.IsOK() && !status.IsNotFound()) {
        return {status, {}, framework::Version()};
    }

    // read all merged segments, load params and create deletionmap
    bool hasMergeSegment = false;
    auto tabletData = taskContext->GetTabletData();
    const auto& version = tabletData->GetOnDiskVersion();
    segmentid_t maxMergedSegmentId = INVALID_SEGMENTID;

    std::vector<segmentid_t> removedSegments;
    auto segments = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);

    for (auto segment : segments) {
        if (framework::Segment::IsMergedSegmentId(segment->GetSegmentId())) {
            auto segmentInfo = segment->GetSegmentInfo();
            if (enableTTL && segmentInfo->GetMaxTTL() < currentTimeInSeconds && segmentInfo->docCount != 0) {
                // segment maxTTL only consider add doc. For patch, we can't remove entire segment
                auto [status, patched] = IsSegmentHasValidPatch(segment, taskContext);
                if (!status.IsOK()) {
                    return {status, {}, framework::Version()};
                }
                if (!patched) {
                    removedSegments.push_back(segment->GetSegmentId());
                    AUTIL_LOG(INFO, "reclaim entire segment [%d] with max ttl [%d]", segment->GetSegmentId(),
                              segmentInfo->GetMaxTTL());
                }
            }
            hasMergeSegment = true;
            maxMergedSegmentId = std::max(maxMergedSegmentId, segment->GetSegmentId());
        }
    }

    if (!hasMergeSegment || !needReclaim) {
        AUTIL_LOG(INFO, "no need for reclaim needReclaim [%d], hasMergeSegment [%d]", needReclaim, hasMergeSegment);
        return {Status::OK(), {}, framework::Version()};
    }
    assert(maxMergedSegmentId != INVALID_SEGMENTID);

    auto reclaimOpId = opId++;
    framework::Version targetVersion = version.Clone("");
    segmentid_t targetSegmentId = taskContext->GetMaxMergedSegmentId() + 1;
    assert(targetSegmentId >= maxMergedSegmentId + 1);

    targetVersion.SetVersionId(taskContext->GetMaxMergedVersionId() + 1);
    targetVersion.AddSegment(targetSegmentId);
    targetVersion.SetSchemaId(taskContext->GetTabletSchema()->GetSchemaId());
    for (auto segmentId : removedSegments) {
        targetVersion.RemoveSegment(segmentId);
    }

    if (removedSegments.size() > 0) {
        REGISTER_METRIC_WITH_INDEXLIB_PREFIX(taskContext->GetMetricsManager()->GetMetricsReporter(),
                                             TTLReclaimSegmentCount, "docReclaim/TTLReclaimSegmentCount",
                                             kmonitor::GAUGE);
        kmonitor::MetricsTags tags;
        tags.AddTag("reclaim_segments", autil::StringUtil::toString(removedSegments, "_"));
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, TTLReclaimSegmentCount, removedSegments.size());
    }

    auto segDirName = targetVersion.GetSegmentDirName(targetSegmentId);
    auto reclaimOpDesc = NormalTableReclaimOperation::CreateOperationDescription(reclaimOpId, segDirName,
                                                                                 targetSegmentId, removedSegments);
    if (prepareIndexReclaimParamOp != std::nullopt) {
        reclaimOpDesc.AddDepend(prepareIndexReclaimParamOp.value());
    }
    opDescs.push_back(reclaimOpDesc);

    std::map<std::string, framework::IndexOperationId> movePaths;
    movePaths[segDirName] = reclaimOpId;

    auto moveOpId = opId++;
    auto moveOpDesc = PathMoveOperation::CreateOperationDescription(moveOpId, movePaths);
    moveOpDesc.AddDepend(reclaimOpId);
    opDescs.push_back(moveOpDesc);

    // commit version

    if (!CommitTaskLogToVersion(taskContext, targetVersion).IsOK()) {
        return {Status::Unknown(), {}, framework::Version()};
    }

    auto commitOpId = opId++;
    auto commitOpDesc =
        MergedVersionCommitOperation::CreateOperationDescription(commitOpId, targetVersion,
                                                                 /*patchIndexDir*/ "", /*dropIndexes*/ {});
    commitOpDesc.AddDepend(moveOpId);
    opDescs.push_back(commitOpDesc);

    // clean fence
    auto deleteOp = opId++;
    auto delOpDesc = FenceDirDeleteOperation::CreateOperationDescription(deleteOp);
    delOpDesc.AddDepend(commitOpId);
    opDescs.push_back(delOpDesc);

    return {Status::OK(), opDescs, targetVersion};
}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
NormalTableReclaimPlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    auto [status, opDescs, targetVersion] = CreateOperationDescriptions(taskContext);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create operation descriptions failed");
    if (opDescs.empty()) {
        return {Status::OK(), nullptr};
    }

    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    for (auto opDesc : opDescs) {
        indexTaskPlan->AddOperation(opDesc);
    }
    framework::IndexOperationDescription endTaskOpDesc(/*id*/ opDescs.size(),
                                                       /*type*/ framework::END_TASK_OPERATION);
    endTaskOpDesc.AddParameter(PARAM_TARGET_VERSION, targetVersion.ToString());
    indexTaskPlan->SetEndTaskOperation(endTaskOpDesc);
    return {Status::OK(), std::move(indexTaskPlan)};
}

std::string NormalTableReclaimPlanCreator::ConstructLogTaskType() const
{
    std::string logType = TASK_TYPE;
    if (!_taskName.empty()) {
        logType += "@";
        logType += _taskName;
    }
    return logType;
}

std::string NormalTableReclaimPlanCreator::ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                                              const framework::Version& targetVersion) const
{
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    std::string taskId = "version_";
    taskId += autil::StringUtil::toString(baseVersion.GetVersionId());
    taskId += "_to_";
    taskId += autil::StringUtil::toString(targetVersion.GetVersionId());
    return taskId;
}

Status NormalTableReclaimPlanCreator::AppendDefaultConfigIfNecessary(const framework::IndexTaskContext* context,
                                                                     std::vector<config::IndexTaskConfig>* configs)
{
    auto [status, enableTTL] = context->GetTabletSchema()->GetRuntimeSettings().GetValue<bool>("enable_ttl");
    if (!status.IsOK() && !status.IsNotFound()) {
        return status;
    }
    if (status.IsOK() && enableTTL) {
        for (const auto& config : *configs) {
            if (config.GetTaskType() == TASK_TYPE) {
                return Status::OK();
            }
        }
        auto defaultReclaimConfig = config::IndexTaskConfig(DEFAULT_TASK_NAME, TASK_TYPE, DEFAULT_TASK_PERIOD);
        configs->push_back(defaultReclaimConfig);
        AUTIL_LOG(INFO, "append default reclaim task config name [%s], period [%s] to task config",
                  DEFAULT_TASK_NAME.c_str(), DEFAULT_TASK_PERIOD.c_str());
        auto tabletData = context->GetTabletData();
        if (tabletData) {
            tabletData->DeclareTaskConfig(defaultReclaimConfig.GetTaskName(), defaultReclaimConfig.GetTaskType());
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::table
