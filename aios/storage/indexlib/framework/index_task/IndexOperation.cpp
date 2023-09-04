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
#include "indexlib/framework/index_task/IndexOperation.h"

#include "autil/LoopThread.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/relocatable/Relocator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexOperation);

Status IndexOperation::ExecuteWithLog(const IndexTaskContext& context)
{
    AUTIL_LOG(INFO, "start execute op[%ld][%s]", _opId, GetDebugString().c_str());
    auto manager = context.GetMetricsManager();
    if (manager) {
        // general task will call manager->ReportMetrics() in every workLoop
        kmonitor::MetricsTags tags;
        tags.AddTag("opId", std::to_string(_opId));
        tags.AddTag("opName", indexlib::util::KmonitorTagvNormalizer::GetInstance()->Normalize(GetDebugString()));

        const std::string identifier = "INDEX_OPERATION_METRICS_" + GetDebugString() + "_" + std::to_string(_opId);
        const auto creatorFunc = [&]() -> std::shared_ptr<framework::IMetrics> {
            return std::make_shared<IndexOperationMetrics>(manager->GetMetricsReporter(), tags);
        };
        _operationMetrics =
            std::dynamic_pointer_cast<IndexOperationMetrics>(manager->CreateMetrics(identifier, creatorFunc));
        assert(_operationMetrics);
    }

    context.Log();
    auto [status, fenceRoot] = context.CreateOpFenceRoot(_opId, _useOpFenceDir);
    RETURN_IF_STATUS_ERROR(status, "create op fence root failed");
    if (fenceRoot) {
        std::string folderWorkRoot = IndexTaskContext::GetRelocatableFolderWorkRoot(fenceRoot->GetOutputPath(), _opId);
        auto [folderStatus, folder] =
            indexlib::file_system::Relocator::CreateFolder(folderWorkRoot, context.GetMetricProvider());
        RETURN_IF_STATUS_ERROR(folderStatus, "create relocatable global folder failed for op[%ld]", _opId);
        context.SetGlobalRelocatableFolder(folder);
    }

    status = Execute(context);
    if (status.IsOK()) {
        auto folder = context.GetGlobalRelocatableFolder();
        if (folder) {
            RETURN_IF_STATUS_ERROR(indexlib::file_system::Relocator::SealFolder(folder), "%s",
                                   "seal relocatable global folder failed");
        }
    }

    int64_t doneTimeUs = -1;
    if (_operationMetrics) {
        doneTimeUs = _operationMetrics->GetTimerDoneUs();
        _operationMetrics.reset();
    }
    AUTIL_LOG(INFO, "end execute op[%ld][%s], time[%ld]seconds, status[%s]", _opId, GetDebugString().c_str(),
              doneTimeUs == -1 ? -1 : doneTimeUs / (1000 * 1000), status.ToString().c_str());
    return status;
}

Status IndexOperation::Publish(const IndexTaskContext& context, const std::string& targetRelativePath,
                               const std::string& fileName, const std::string& content) const
{
    indexlib::file_system::DirectoryOption directoryOption;
    // ignore exist error if recursive = true
    directoryOption.recursive = true;
    auto [status, targetDir] =
        context.GetIndexRoot()->GetIDirectory()->MakeDirectory(targetRelativePath, directoryOption).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get target dir failed, opId[%ld], status [%s]", _opId, status.ToString().c_str());
        return status;
    }
    bool isExist = false;
    std::tie(status, isExist) = targetDir->IsExist(fileName).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "check file [%s] exist in target dir[%s] failed, opId[%ld], status [%s]", fileName.c_str(),
                  targetDir->GetOutputPath().c_str(), _opId, status.ToString().c_str());
        return status;
    }
    if (isExist) {
        AUTIL_LOG(INFO, "file[%s] already exist in target dir[%s], skip this file, opId[%ld]", fileName.c_str(),
                  targetDir->GetOutputPath().c_str(), _opId);
        return status;
    }
    std::shared_ptr<indexlib::file_system::IDirectory> opDir;
    std::tie(status, opDir) = context.GetOpFenceRoot(_opId, _useOpFenceDir);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "get op fence dir failed, opId[%ld], status %s", _opId, status.ToString().c_str());
        return status;
    }
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    status = opDir->RemoveFile(fileName, removeOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "remove old %s failed, opId[%ld], status %s", fileName.c_str(), _opId,
                  status.ToString().c_str());
        return status;
    }
    status = opDir->Store(fileName, content, indexlib::file_system::WriterOption::AtomicDump()).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "store file[%s] at [%s] failed opId[%ld], status %s", fileName.c_str(),
                  opDir->GetOutputPath().c_str(), _opId, status.ToString().c_str());
        return status;
    }
    std::string srcFilePath = indexlib::util::PathUtil::JoinPath(opDir->GetOutputPath(), fileName);
    std::string targetFilePath = indexlib::util::PathUtil::JoinPath(targetDir->GetOutputPath(), fileName);
    status = indexlib::file_system::FslibWrapper::Rename(srcFilePath, targetFilePath).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "mv from [%s] to [%s] failed, opId[%ld], status %s", srcFilePath.c_str(),
                  targetFilePath.c_str(), _opId, status.ToString().c_str());
        return status;
    }
    auto fs = targetDir->GetFileSystem();
    if (fs == nullptr) {
        status = Status::InternalError("get file system failed.");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    status = fs->MountFile(targetDir->GetPhysicalPath(""), fileName,
                           indexlib::util::PathUtil::JoinPath(targetRelativePath, fileName),
                           indexlib::file_system::FSMT_READ_ONLY, content.size(), /*mayNonExist=*/false)
                 .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "mount file [%s] failed, opId[%ld], status %s", srcFilePath.c_str(), _opId,
                  status.ToString().c_str());
        return status;
    }

    return Status::OK();
}

} // namespace indexlibv2::framework
