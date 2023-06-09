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
class TempFileWriter : public FileWriter
{
public:
    using CloseFunc = std::function<void(const std::string&)>;
    TempFileWriter(const std::shared_ptr<FileWriter>& writer, CloseFunc&& closeFunc) noexcept;
    ~TempFileWriter() noexcept;

    TempFileWriter(const TempFileWriter&) = delete;
    TempFileWriter& operator=(const TempFileWriter&) = delete;
    TempFileWriter(TempFileWriter&&) = delete;
    TempFileWriter& operator=(TempFileWriter&&) = delete;

public:
    FSResult<void> Open(const std::string& logicalPath, const std::string& physicalPath) noexcept override;
    FSResult<void> Close() noexcept override;
    const std::string& GetLogicalPath() const noexcept override;
    const std::string& GetPhysicalPath() const noexcept override;
    const WriterOption& GetWriterOption() const noexcept override;

public:
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override;
    FSResult<void> Truncate(size_t truncateSize) noexcept override;
    void* GetBaseAddress() noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;

private:
    bool _isClosed;
    std::shared_ptr<FileWriter> _writer;
    CloseFunc _closeFunc;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TempFileWriter> TempFileWriterPtr;
} // namespace indexlib::file_system
