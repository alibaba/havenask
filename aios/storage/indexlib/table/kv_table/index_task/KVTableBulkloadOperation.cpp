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
#include "indexlib/table/kv_table/index_task/KVTableBulkloadOperation.h"

#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/IndexTaskQueue.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/VersionResource.h"
#include "indexlib/table/kv_table/KVTabletOptions.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreatePlan.h"
#include "indexlib/table/kv_table/index_task/KVTableExternalFileImportJob.h"
#include "indexlib/util/JsonUtil.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableBulkloadOperation);

KVTableBulkloadOperation::KVTableBulkloadOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

std::pair<Status, framework::IndexOperationDescription>
KVTableBulkloadOperation::CreateOperationDescription(framework::IndexOperationId opId,
                                                     const std::map<std::string, std::string>& params,
                                                     const std::string& taskName, versionid_t targetVersionId)
{
    auto getParameter = [&](const std::string& key, std::string* value) -> bool {
        auto iter = params.find(key);
        if (iter == params.end()) {
            AUTIL_LOG(ERROR, "parameter [%s] not found.", key.c_str());
            return false;
        }
        if (iter->second.empty()) {
            AUTIL_LOG(ERROR, "parameter [%s] value is empty.", key.c_str());
            return false;
        }
        (*value) = iter->second;
        return true;
    };
    std::string bulkloadId = taskName;
    std::string externalFilesStr;
    std::string importExternalFileOptionsStr;
    std::string lastSequenceNumberStr;
    if (!getParameter(framework::PARAM_EXTERNAL_FILES, &externalFilesStr) ||
        !getParameter(framework::PARAM_IMPORT_EXTERNAL_FILE_OPTIONS, &importExternalFileOptionsStr) ||
        !getParameter(framework::PARAM_LAST_SEQUENCE_NUMBER, &lastSequenceNumberStr)) {
        return {Status::InternalError(), framework::IndexOperationDescription()};
    }
    framework::IndexOperationDescription opDesc(opId, OPERATION_TYPE);
    opDesc.AddParameter(framework::PARAM_BULKLOAD_ID, bulkloadId);
    opDesc.AddParameter(framework::PARAM_EXTERNAL_FILES, externalFilesStr);
    opDesc.AddParameter(framework::PARAM_IMPORT_EXTERNAL_FILE_OPTIONS, importExternalFileOptionsStr);
    opDesc.AddParameter(framework::PARAM_LAST_SEQUENCE_NUMBER, lastSequenceNumberStr);
    opDesc.AddParameter(PARAM_TARGET_VERSION_ID, targetVersionId);
    return {Status::OK(), opDesc};
}

Status KVTableBulkloadOperation::Execute(const framework::IndexTaskContext& context)
{
    Status status;
    std::string bulkloadId;
    std::string externalFilesStr;
    std::string lastSequenceNumberStr;
    std::string importExternalFileOptionsStr;

    if (!_desc.GetParameter(framework::PARAM_BULKLOAD_ID, bulkloadId) ||
        !_desc.GetParameter(framework::PARAM_EXTERNAL_FILES, externalFilesStr) ||
        !_desc.GetParameter(framework::PARAM_LAST_SEQUENCE_NUMBER, lastSequenceNumberStr) ||
        !_desc.GetParameter(framework::PARAM_IMPORT_EXTERNAL_FILE_OPTIONS, importExternalFileOptionsStr)) {
        status = Status::InternalError("failed to get params for bulkload");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    std::vector<std::string> externalFiles;
    framework::ImportExternalFileOptions options;
    status = indexlib::util::JsonUtil::FromString(externalFilesStr, &externalFiles);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    status = indexlib::util::JsonUtil::FromString(importExternalFileOptionsStr, &options);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    uint64_t lastSequenceNumber = 0;
    if (!autil::StringUtil::fromString(lastSequenceNumberStr, lastSequenceNumber)) {
        status =
            Status::InternalError("convert last sequence number str [%s] to num failed", lastSequenceNumberStr.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto resourceManager = context.GetResourceManager();
    versionid_t versionId;
    if (resourceManager == nullptr) {
        status = Status::InternalError("failed to get resource manager");
    } else if (!_desc.GetParameter(PARAM_TARGET_VERSION_ID, versionId)) {
        status = Status::InternalError("failed to get target version id");
    }
    auto targetVersion = context.GetTabletData()->GetOnDiskVersion().Clone("");
    auto indexTask = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    if (indexTask == nullptr) {
        status = Status::InternalError("current bulkload task [%s] not found in base version [%d]", bulkloadId.c_str(),
                                       context.GetTabletData()->GetOnDiskVersion().GetVersionId());
    }
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    std::shared_ptr<indexlib::file_system::IDirectory> opDir;
    std::tie(status, opDir) = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get op fence root failed opId[%ld], status %s", _desc.GetId(), status.ToString().c_str());
        return status;
    }

    targetVersion.SetVersionId(versionId);
    auto tabletOptions = context.GetTabletOptions();
    if (tabletOptions == nullptr) {
        status = Status::InternalError("invalid bulkload result, internal file meta empty");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    KVTabletOptions kvTabletOptions(std::move(tabletOptions));
    auto levelInfo = targetVersion.GetSegmentDescriptions()->GetLevelInfo();
    if (levelInfo == nullptr) {
        levelInfo = std::make_shared<indexlibv2::framework::LevelInfo>();
        levelInfo->Init(framework::topo_hash_mod, kvTabletOptions.GetLevelNum(), kvTabletOptions.GetShardNum());
        targetVersion.GetSegmentDescriptions()->SetLevelInfo(levelInfo);
    }

    KVTableExternalFileImportJob job(opDir->GetOutputPath(), options, kvTabletOptions.GetShardNum());
    status = job.Prepare(externalFiles);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "failed to prepare external files");
        return status;
    }
    auto internalFiles = job.GetInternalFiles();
    if (internalFiles.empty()) {
        status = Status::InternalError("invalid bulkload result, internal files empty");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    std::shared_ptr<KVTableBulkloadSegmentCreatePlan> bulkloadSegmentCreatePlan;
    status = resourceManager->LoadResource(/*name=*/BULKLOAD_SEGMENT_CREATE_PLAN, /*type=*/BULKLOAD_SEGMENT_CREATE_PLAN,
                                           bulkloadSegmentCreatePlan);
    if (status.IsNotFound()) {
        status = resourceManager->CreateResource(/*name=*/BULKLOAD_SEGMENT_CREATE_PLAN,
                                                 /*type=*/BULKLOAD_SEGMENT_CREATE_PLAN, bulkloadSegmentCreatePlan);
    }
    RETURN_IF_STATUS_ERROR(status, "load or create [%s] failed", BULKLOAD_SEGMENT_CREATE_PLAN);

    std::map<uint64_t, std::vector<InternalFileInfo>> internalFileGroups;
    for (const auto& internalFile : internalFiles) {
        internalFileGroups[internalFile.sequenceNumber].push_back(internalFile);
    }
    segmentid_t currentMaxMergedSegmentId = context.GetMaxMergedSegmentId();
    std::vector<segmentid_t> bulkloadSegIds;
    for (auto& [_, fileGroup] : internalFileGroups) {
        segmentid_t segId = ++currentMaxMergedSegmentId;
        targetVersion.AddSegment(segId);
        bulkloadSegIds.push_back(segId);
        for (auto& file : fileGroup) {
            file.targetSegmentId = segId;
            file.targetLevelIdx = 0;
            bulkloadSegmentCreatePlan->AddInternalFileInfo(file);
        }
    }

    // bulkload reserved a built seg id, and << 24 as sequence number
    // use this seg id to position
    segmentid_t reservedSegmentId = lastSequenceNumber >> 24;
    auto iter = levelInfo->levelMetas[0].segments.begin();
    for (; iter != levelInfo->levelMetas[0].segments.end(); ++iter) {
        if (*iter > reservedSegmentId) {
            break;
        }
    }
    levelInfo->levelMetas[0].segments.insert(iter, bulkloadSegIds.begin(), bulkloadSegIds.end());

    RETURN_IF_STATUS_ERROR(resourceManager->CommitResource(bulkloadSegmentCreatePlan->GetName()),
                           "commit resource failed, name[%s], type[%s].", bulkloadSegmentCreatePlan->GetName().c_str(),
                           bulkloadSegmentCreatePlan->GetType().c_str());

    std::shared_ptr<VersionResource> versionResource;
    status = resourceManager->LoadResource(targetVersion.GetVersionFileName(), VERSION_RESOURCE, versionResource);
    if (status.IsNotFound()) {
        status = resourceManager->CreateResource(targetVersion.GetVersionFileName(), VERSION_RESOURCE, versionResource);
    }
    RETURN_IF_STATUS_ERROR(status, "load or create [%s] failed", VERSION_RESOURCE);
    auto indexTaskQueue = targetVersion.GetIndexTaskQueue();
    if (!indexTaskQueue->Done(framework::BULKLOAD_TASK_TYPE, bulkloadId)) {
        return Status::InternalError();
    }
    versionResource->SetVersion(targetVersion);
    RETURN_IF_STATUS_ERROR(resourceManager->CommitResource(versionResource->GetName()),
                           "commit resource failed, name[%s], type[%s].", versionResource->GetName().c_str(),
                           versionResource->GetType().c_str());
    return status;
}

} // namespace indexlibv2::table
