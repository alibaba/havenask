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
#include "indexlib/file_system/FileSystemCreator.h"

#include <memory>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemCreator);

FSResult<std::shared_ptr<IFileSystem>>
FileSystemCreator::Create(const std::string& name, const std::string& rootPath, const FileSystemOptions& fsOptions,
                          const std::shared_ptr<util::MetricProvider>& metricProvider, bool isOverride,
                          FenceContext* fenceContext) noexcept
{
    if (isOverride) {
        auto [ec, isExist] = FslibWrapper::IsExist(rootPath);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "IsExist failed, path[%s]", rootPath.c_str());
        if (isExist) {
            ec = FslibWrapper::DeleteDir(rootPath, DeleteOption::Fence(fenceContext, false)).Code();
            RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "delete failed, path[%s]", rootPath.c_str());
        }
        ec = FslibWrapper::MkDir(rootPath, true).Code();
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "mkdir failed, path[%s]", rootPath.c_str());
    }

    return DoCreate(name, rootPath, fsOptions, metricProvider);
}

FSResult<std::shared_ptr<IFileSystem>>
FileSystemCreator::CreateForRead(const std::string& name, const std::string& outputRoot,
                                 const FileSystemOptions& fsOptions,
                                 const std::shared_ptr<util::MetricProvider>& metricProvider) noexcept
{
    return DoCreate(name, outputRoot, fsOptions, metricProvider);
}

FSResult<std::shared_ptr<IFileSystem>>
FileSystemCreator::CreateForWrite(const std::string& name, const std::string& outputRoot,
                                  const FileSystemOptions& fsOptions,
                                  const std::shared_ptr<util::MetricProvider>& metricProvider, bool isOverride) noexcept
{
    // prepare directory
    auto [ec, isExist] = FslibWrapper::IsExist(outputRoot);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "IsExist failed, path[%s]", outputRoot.c_str());
    if (isExist && isOverride) {
        ec = FslibWrapper::DeleteDir(outputRoot, DeleteOption::NoFence(false)).Code();
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "delete failed, path[%s]", outputRoot.c_str());
        isExist = false;
    }
    if (!isExist) {
        ec = FslibWrapper::MkDir(outputRoot, true).Code();
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<IFileSystem>(), "mkdir failed, path[%s]", outputRoot.c_str());
    }
    return DoCreate(name, outputRoot, fsOptions, metricProvider);
}

FSResult<std::shared_ptr<IFileSystem>>
FileSystemCreator::DoCreate(const std::string& name, const std::string& outputRoot, const FileSystemOptions& fsOptions,
                            const std::shared_ptr<util::MetricProvider>& metricProvider) noexcept
{
    // new fs
    std::shared_ptr<IFileSystem> fs(new LogicalFileSystem(name, outputRoot, metricProvider));
    if (fsOptions.memoryQuotaController) {
        RETURN2_IF_FS_ERROR(fs->Init(fsOptions), std::shared_ptr<IFileSystem>(), "Init FileSystem[%s] failed",
                            name.c_str());
    } else {
        FileSystemOptions newOptions = fsOptions;
        newOptions.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        RETURN2_IF_FS_ERROR(fs->Init(newOptions), std::shared_ptr<IFileSystem>(), "Init FileSystem[%s] failed",
                            name.c_str());
    }
    return {FSEC_OK, fs};
}
}} // namespace indexlib::file_system
