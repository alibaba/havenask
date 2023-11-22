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
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreateOperation.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreatePlan.h"
#include "indexlib/util/PathUtil.h"

using PathUtil = indexlib::util::PathUtil;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableBulkloadSegmentCreateOperation);

KVTableBulkloadSegmentCreateOperation::KVTableBulkloadSegmentCreateOperation(
    const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

framework::IndexOperationDescription
KVTableBulkloadSegmentCreateOperation::CreateOperationDescription(framework::IndexOperationId opId)
{
    return framework::IndexOperationDescription(opId, OPERATION_TYPE);
}

Status KVTableBulkloadSegmentCreateOperation::Execute(const framework::IndexTaskContext& context)
{
    Status status;
    auto resourceManager = context.GetResourceManager();
    if (resourceManager == nullptr) {
        status = Status::Corruption("get resource manager failed");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    std::shared_ptr<KVTableBulkloadSegmentCreatePlan> plan;
    status = resourceManager->LoadResource(BULKLOAD_SEGMENT_CREATE_PLAN, BULKLOAD_SEGMENT_CREATE_PLAN, plan);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load resource [%s] fail", BULKLOAD_SEGMENT_CREATE_PLAN);
        return status;
    }
    indexlib::file_system::DirectoryOption directoryOption;
    directoryOption.recursive = true;
    const auto& internalFileInfos = plan->GetInternalFileInfos();
    for (const auto& internalFile : internalFileInfos) {
        std::shared_ptr<indexlib::file_system::IDirectory> targetSegmentDirectory;
        std::string segmentRelativePath = framework::SegmentDescriptions::GetSegmentDirName(
            internalFile.targetSegmentId, internalFile.targetLevelIdx);

        RETURN_IF_STATUS_ERROR(GetOrMakeOutputDirectory(context, segmentRelativePath, &targetSegmentDirectory),
                               "get or make output dir failed");

        std::shared_ptr<indexlib::file_system::IDirectory> targetDir;
        std::tie(status, targetDir) =
            targetSegmentDirectory->MakeDirectory(internalFile.targetRelativePathInSegment, directoryOption)
                .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make directory %s failed, status %s", internalFile.targetRelativePathInSegment.c_str(),
                      status.ToString().c_str());
            return status;
        }
        if (internalFile.isDir) {
            continue;
        }
        if (internalFile.targetFileName.empty()) {
            status = Status::InvalidArgs("invalid internal file, file path is not dir and file name is empty");
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        if (internalFile.targetFileName == indexlib::SEGMETN_METRICS_FILE_NAME) {
            indexlib::framework::SegmentMetrics segmentMetrics;
            status = segmentMetrics.Load(internalFile.internalFilePath);
            RETURN_IF_STATUS_ERROR(status, "load %s failed, src path %s", internalFile.targetFileName.c_str(),
                                   internalFile.internalFilePath.c_str());
            status = Publish(context, PathUtil::JoinPath(segmentRelativePath, internalFile.targetRelativePathInSegment),
                             indexlib::SEGMETN_METRICS_FILE_NAME, segmentMetrics.ToString());
            RETURN_IF_STATUS_ERROR(status, "publish %s failed.", indexlib::SEGMETN_METRICS_FILE_NAME);
        } else if (internalFile.targetFileName == COUNTER_FILE_NAME) {
            status = Publish(context, PathUtil::JoinPath(segmentRelativePath, internalFile.targetRelativePathInSegment),
                             COUNTER_FILE_NAME, "");
            RETURN_IF_STATUS_ERROR(status, "publish %s failed.", COUNTER_FILE_NAME.c_str());
        } else if (internalFile.targetFileName == SEGMENT_INFO_FILE_NAME) {
            auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
            framework::SegmentInfo segInfo;
            status = segInfo.Load(internalFile.internalFilePath);
            RETURN_IF_STATUS_ERROR(status, "load %s failed, src path %s", internalFile.targetFileName.c_str(),
                                   internalFile.internalFilePath.c_str());
            // bulkload segment locator should be reset to default.
            segInfo.SetLocator(framework::Locator());
            std::string segmentInfoContent;
            std::tie(status, segmentInfoContent) = indexlib::file_system::JsonUtil::ToString(segInfo).StatusWith();
            RETURN_IF_STATUS_ERROR(status, "%s to str failed.", SEGMENT_INFO_FILE_NAME.c_str());
            status = Publish(context, PathUtil::JoinPath(segmentRelativePath, internalFile.targetRelativePathInSegment),
                             SEGMENT_INFO_FILE_NAME, segmentInfoContent);
            RETURN_IF_STATUS_ERROR(status, "publish %s failed.", SEGMENT_INFO_FILE_NAME.c_str());
        } else {
            std::string targetFilePath = PathUtil::JoinPath(targetDir->GetOutputPath(), internalFile.targetFileName);
            RETURN_IF_STATUS_ERROR(
                indexlib::file_system::FslibWrapper::Rename(internalFile.internalFilePath, targetFilePath).Status(),
                "mv from [%s] to [%s] failed", internalFile.internalFilePath.c_str(), targetFilePath.c_str());
        }
    }
    return status;
}

Status KVTableBulkloadSegmentCreateOperation::GetOrMakeOutputDirectory(
    const framework::IndexTaskContext& context, const std::string& targetPath,
    std::shared_ptr<indexlib::file_system::IDirectory>* outputDir)
{
    indexlib::file_system::DirectoryOption directoryOption;
    directoryOption.recursive = true;
    auto [status, targetDir] =
        context.GetIndexRoot()->GetIDirectory()->MakeDirectory(targetPath, directoryOption).StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    *outputDir = targetDir;
    return Status::OK();
}

} // namespace indexlibv2::table
