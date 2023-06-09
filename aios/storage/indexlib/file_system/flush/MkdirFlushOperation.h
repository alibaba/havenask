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
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/file/FileNode.h"

namespace indexlib { namespace file_system {

class MkdirFlushOperation
{
public:
    MkdirFlushOperation(const FlushRetryStrategy& flushRetryStrategy, const std::shared_ptr<FileNode>& fileNode);
    ~MkdirFlushOperation();

public:
    ErrorCode Execute() noexcept;
    const std::string& GetDestPath() noexcept { return _destPath; }

private:
    FlushRetryStrategy _flushRetryStrategy;
    std::string _destPath;
    std::shared_ptr<FileNode> _fileNode;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MkdirFlushOperation> MkdirFlushOperationPtr;
}} // namespace indexlib::file_system
