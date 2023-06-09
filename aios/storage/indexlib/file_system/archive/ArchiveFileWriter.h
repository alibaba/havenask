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

#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/file/FileWriterImpl.h"

namespace indexlib { namespace file_system {

class ArchiveFileWriter : public FileWriterImpl
{
public:
    ArchiveFileWriter(const ArchiveFilePtr& archiveFile) noexcept : _archiveFile(archiveFile), _length(0) {}
    ErrorCode DoOpen() noexcept override
    {
        assert(false);
        return FSEC_OK;
    };
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override
    {
        _length += length;
        return _archiveFile->Write(buffer, length);
    }
    size_t GetLength() const noexcept override { return _length; }
    ErrorCode DoClose() noexcept override
    {
        auto ec = _archiveFile->Close();
        assert(_length == _archiveFile->GetFileLength());
        return ec;
    }
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override { return FSEC_OK; }
    FSResult<void> Truncate(size_t truncateSize) noexcept override { return FSEC_OK; }
    void* GetBaseAddress() noexcept override
    {
        assert(false);
        return NULL;
    }

private:
    ArchiveFilePtr _archiveFile;
    size_t _length;
};
typedef std::shared_ptr<ArchiveFileWriter> ArchiveFileWriterPtr;

}} // namespace indexlib::file_system
