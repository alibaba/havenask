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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"

namespace indexlib { namespace file_system {

class FileFlushOperation
{
public:
    FileFlushOperation(const FlushRetryStrategy& flushRetryStrategy)
        : _flushRetryStrategy(flushRetryStrategy)
        , _flushMemoryUse(0)
    {
    }

    virtual ~FileFlushOperation() {}
    int64_t GetFlushMemoryUse() const noexcept { return _flushMemoryUse; }

public:
    ErrorCode Execute() noexcept;
    virtual ErrorCode DoExecute() noexcept = 0;
    virtual const std::string& GetDestPath() const noexcept = 0;
    virtual void GetFileNodePaths(FileList& fileList) const noexcept = 0;

protected:
    static const uint32_t DEFAULT_FLUSH_BUF_SIZE = 2 * 1024 * 1024; // 2 MB
    virtual void CleanDirtyFile() noexcept = 0;
    ErrorCode SplitWrite(const std::unique_ptr<FslibFileWrapper>& file, const void* buffer, size_t length) noexcept;

protected:
    FlushRetryStrategy _flushRetryStrategy;
    int64_t _flushMemoryUse;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileFlushOperation> FileFlushOperationPtr;

}} // namespace indexlib::file_system
