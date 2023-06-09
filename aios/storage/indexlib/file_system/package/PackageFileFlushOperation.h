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
#pragma once

#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"
#include "indexlib/file_system/package/PackageFileMeta.h"

namespace indexlib { namespace file_system {
class FileNode;
struct WriterOption;

class PackageFileFlushOperation : public FileFlushOperation
{
public:
    PackageFileFlushOperation(const FlushRetryStrategy& flushRetryStrategy, const std::string& packageFilePath,
                              const PackageFileMetaPtr& packageFileMeta,
                              const std::vector<std::shared_ptr<FileNode>>& fileNodeVec,
                              const std::vector<WriterOption>& writerOptionVec);

    ~PackageFileFlushOperation();

public:
    ErrorCode DoExecute() noexcept override;
    const std::string& GetDestPath() const noexcept override { return _packageFilePath; }
    void GetFileNodePaths(FileList& fileList) const noexcept override;

protected:
    void CleanDirtyFile() noexcept override;

private:
    void Init(const std::vector<std::shared_ptr<FileNode>>& fileNodeVec,
              const std::vector<WriterOption>& writerOptionVec);
    ErrorCode StoreDataFile() noexcept;
    ErrorCode StoreMetaFile() noexcept;
    void MakeFlushedFileNotDirty() noexcept;
    std::string GetDataFilePath();
    std::string GetMetaFilePath();

private:
    std::string _packageDirPath;
    std::string _packageFilePath;
    PackageFileMetaPtr _packageFileMeta;
    std::vector<std::shared_ptr<FileNode>> _innerFileNodes;
    std::vector<std::shared_ptr<FileNode>> _flushFileNodes;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileFlushOperation> PackageFileFlushOperationPtr;
}} // namespace indexlib::file_system
