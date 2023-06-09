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
#include "indexlib/table/index_task/merger/MergePlan.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MergePlan);

MergePlan::MergePlan(std::string name, framework::IndexTaskResourceType type) : IndexTaskResource(std::move(name), type)
{
}

MergePlan::~MergePlan() {}

framework::Version MergePlan::CreateNewVersion(const std::shared_ptr<MergePlan>& mergePlan,
                                               const framework::IndexTaskContext* taskContext)
{
    const auto& sourceVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    // For merged version, fence name is empty, use root dir instead.
    framework::Version targetVersion = sourceVersion.Clone("");

    // this addSegments is for full merger && split to multi segment, without it may cause remove should keep segment
    set<segmentid_t> addSegments;
    for (uint32_t i = 0; i < mergePlan->Size(); ++i) {
        const auto& plan = mergePlan->GetSegmentMergePlan(i);
        for (size_t i = 0; i < plan.GetSrcSegmentCount(); i++) {
            auto segmentId = plan.GetSrcSegmentId(i);
            if (addSegments.find(segmentId) == addSegments.end()) {
                targetVersion.RemoveSegment(segmentId);
            }
        }

        for (size_t j = 0; j < plan.GetTargetSegmentCount(); ++j) {
            auto targetSegmentId = plan.GetTargetSegmentId(j);
            targetVersion.AddSegment(targetSegmentId);
            addSegments.insert(targetSegmentId);
        }
    }

    auto targetVersionId = taskContext->GetMaxMergedVersionId() + 1;
    targetVersion.SetVersionId(targetVersionId);
    targetVersion.SetSchemaId(taskContext->GetTabletSchema()->GetSchemaId());
    // TODO: (by yijie.zhang) support when alter field add in
    // targetVersion.SetOngoingModifyOperations(taskContext->tabletOptions->GetOngoingModifyOperationIds());
    // commitVersion.SetDescriptions(mOptions.GetVersionDescriptions());
    // commitVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());
    return targetVersion;
}

Status MergePlan::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    auto status = resourceDirectory->GetIDirectory()->RemoveFile(MERGE_PLAN, removeOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "remove file [%s] in directory [%s] failed", MERGE_PLAN,
                  resourceDirectory->DebugString().c_str());
        return status;
    }

    std::string content = ToJsonString(*this);
    indexlib::file_system::WriterOption writerOption = indexlib::file_system::WriterOption::AtomicDump();
    writerOption.notInPackage = true;

    status = resourceDirectory->GetIDirectory()->Store(MERGE_PLAN, content, writerOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "store file [%s] in directory [%s] failed", MERGE_PLAN,
                  resourceDirectory->DebugString().c_str());
        return status;
    }

    return Status::OK();
}

Status MergePlan::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    std::string content;
    auto status =
        resourceDirectory->GetIDirectory()
            ->Load(MERGE_PLAN, indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM),
                   content)
            .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "segment info not exist in dir [%s]", resourceDirectory->DebugString().c_str());
        return status;
    }

    try {
        autil::legacy::FromJsonString(*this, content);
    } catch (const autil::legacy::ParameterInvalidException& e) {
        auto status =
            Status::ConfigError("Invalid json str [%s] exception [%s]", content.c_str(), e.GetMessage().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    return Status::OK();
}

void MergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("segment_merge_plans", _mergePlan, _mergePlan);
    json.Jsonize("target_version", _targetVersion);
}

void MergePlan::AddTaskLogInTargetVersion(const std::string& taskType, const framework::IndexTaskContext* taskContext)
{
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    std::string taskId = "merge_version_";
    taskId += autil::StringUtil::toString(baseVersion.GetVersionId());
    taskId += "_to_";
    taskId += autil::StringUtil::toString(_targetVersion.GetVersionId());
    auto taskLog = std::make_shared<framework::IndexTaskLog>(
        taskId, baseVersion.GetVersionId(), _targetVersion.GetVersionId(), autil::TimeUtility::currentTimeInSeconds(),
        taskContext->GetAllParameters());
    _targetVersion.AddIndexTaskLog(taskType, taskLog);
}

}} // namespace indexlibv2::table
