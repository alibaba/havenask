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

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriter.h"

namespace indexlib::file_system {

class FileWriterImpl : public FileWriter
{
public:
    using UpdateFileSizeFunc = std::function<void(int64_t)>;

public:
    FileWriterImpl() noexcept;
    FileWriterImpl(const UpdateFileSizeFunc&& updateFileSizeFunc) noexcept;
    FileWriterImpl(const WriterOption& writerOption, const UpdateFileSizeFunc&& updateFileSizeFunc) noexcept;
    virtual ~FileWriterImpl() noexcept;

public:
    FSResult<void> Open(const std::string& logicalPath, const std::string& physicalPath) noexcept override final;
    FSResult<void> Close() noexcept override final;

    const std::string& GetLogicalPath() const noexcept override final { return _logicalPath; }
    const std::string& GetPhysicalPath() const noexcept override final { return _physicalPath; }
    const WriterOption& GetWriterOption() const noexcept override final { return _writerOption; }

protected:
    virtual ErrorCode DoOpen() noexcept = 0;
    virtual ErrorCode DoClose() noexcept = 0;

protected:
    WriterOption _writerOption;
    UpdateFileSizeFunc _updateFileSizeFunc;
    std::string _logicalPath;
    std::string _physicalPath;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
