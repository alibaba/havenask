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

#include "autil/Log.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/flush/FileFlushOperation.h"

namespace indexlib { namespace file_system {

class SingleFileFlushOperation : public FileFlushOperation
{
public:
    SingleFileFlushOperation(const FlushRetryStrategy& flushRetryStrategy, const std::shared_ptr<FileNode>& fileNode,
                             const WriterOption& writerOption = WriterOption());
    ~SingleFileFlushOperation();

public:
    ErrorCode DoExecute() noexcept override;
    const std::string& GetDestPath() const noexcept override { return _destPath; }
    void GetFileNodePaths(FileList& fileList) const noexcept override;

protected:
    void CleanDirtyFile() noexcept override;

private:
    FSResult<void> AtomicStore(const std::string& filePath) noexcept;
    FSResult<void> Store(const std::string& filePath) noexcept;

private:
    std::string _destPath;
    std::shared_ptr<FileNode> _fileNode;
    std::shared_ptr<FileNode> _flushFileNode;
    WriterOption _writerOption;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SingleFileFlushOperation> SingleFileFlushOperationPtr;
}} // namespace indexlib::file_system
