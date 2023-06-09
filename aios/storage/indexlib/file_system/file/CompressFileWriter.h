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

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriterImpl.h"

namespace indexlib { namespace file_system {
class CompressDataDumper;
class FileSystemMetricsReporter;

class CompressFileWriter : public FileWriterImpl
{
public:
    CompressFileWriter(FileSystemMetricsReporter* reporter) noexcept;
    ~CompressFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override
    {
        assert(false);
        return FSEC_OK;
    }

public:
    FSResult<void> Init(const std::shared_ptr<FileWriter>& fileWriter,
                        const std::shared_ptr<FileWriter>& infoFileWriter,
                        const std::shared_ptr<FileWriter>& metaFileWriter, const std::string& compressorName,
                        size_t bufferSize, const KeyValueMap& compressorParam) noexcept;

    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;
    size_t GetLogicLength() const noexcept override { return GetUncompressLength(); }

    ErrorCode DoClose() noexcept override;
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override;
    FSResult<void> Truncate(size_t truncateSize) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    void* GetBaseAddress() noexcept override
    {
        assert(false);
        return NULL;
    }

    size_t GetUncompressLength() const noexcept;
    std::shared_ptr<FileWriter> GetDataFileWriter() const noexcept;
    const std::string& GetCompressorName() const noexcept { return _compressorName; }

private:
    std::shared_ptr<CompressDataDumper> _dumper;
    std::string _compressorName;
    FileSystemMetricsReporter* _reporter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressFileWriter> CompressFileWriterPtr;
}} // namespace indexlib::file_system
