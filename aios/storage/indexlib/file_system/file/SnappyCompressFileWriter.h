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

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

namespace indexlib { namespace file_system {
class CompressFileMeta;

// only for attribute patch compress use
class SnappyCompressFileWriter : public FileWriterImpl
{
public:
    SnappyCompressFileWriter() noexcept;
    ~SnappyCompressFileWriter() noexcept;

public:
    static const size_t DEFAULT_COMPRESS_BUFF_SIZE = 10 * 1024 * 1024; // 10MB

public:
    ErrorCode DoOpen() noexcept override
    {
        assert(false);
        return FSEC_OK;
    }

public:
    void Init(const std::shared_ptr<FileWriter>& fileWriter, size_t bufferSize = DEFAULT_COMPRESS_BUFF_SIZE) noexcept;

    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override
    {
        if (_dataWriter) {
            return _dataWriter->GetLength();
        }
        return 0;
    }

    size_t GetLogicLength() const noexcept override { return GetUncompressLength(); }

    ErrorCode DoClose() noexcept override;

    FSResult<void> ReserveFile(size_t reserveSize) noexcept override
    {
        if (_dataWriter) {
            return _dataWriter->ReserveFile(reserveSize);
        }
        return FSEC_OK;
    }
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

    size_t GetUncompressLength() const noexcept { return _writeLength; }
    std::shared_ptr<FileWriter> GetDataFileWriter() const noexcept { return _dataWriter; }

private:
    void DumpCompressData() noexcept(false);
    void DumpCompressMeta() noexcept(false);

private:
    size_t _bufferSize;
    size_t _writeLength;
    std::shared_ptr<FileWriter> _dataWriter;
    std::shared_ptr<CompressFileMeta> _compressMeta;
    util::SnappyCompressor _compressor;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SnappyCompressFileWriter> SnappyCompressFileWriterPtr;
}} // namespace indexlib::file_system
