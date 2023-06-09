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
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"

namespace indexlib { namespace util {
class ByteSliceList;
}} // namespace indexlib::util

namespace indexlib { namespace file_system {

class ArchiveFileReader : public FileReader
{
public:
    ArchiveFileReader(const ArchiveFilePtr& archiveFile) noexcept : _archiveFile(archiveFile) {}
    FSResult<void> Open() noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset,
                          ReadOption option = ReadOption()) noexcept override
    {
        return _archiveFile->PRead(buffer, length, offset);
    }
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option = ReadOption()) noexcept override
    {
        return _archiveFile->Read(buffer, length);
    }
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset,
                                             ReadOption option = ReadOption()) noexcept override
    {
        assert(false);
        return nullptr;
    }
    void* GetBaseAddress() const noexcept override
    {
        assert(false);
        return nullptr;
    }
    size_t GetLength() const noexcept override { return _archiveFile->GetFileLength(); }
    FSResult<void> Close() noexcept override { return _archiveFile->Close(); }
    FSOpenType GetOpenType() const noexcept override
    {
        assert(false);
        return FSOT_UNKNOWN;
    }
    FSFileType GetType() const noexcept override
    {
        assert(false);
        return FSFT_UNKNOWN;
    }
    const std::string& GetLogicalPath() const noexcept override
    {
        static std::string path = "";
        assert(false);
        return path;
    }
    const std::string& GetPhysicalPath() const noexcept override
    {
        static std::string path = "";
        assert(false);
        return path;
    }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override
    {
        assert(false);
        return nullptr;
    }

private:
    ArchiveFilePtr _archiveFile;
};

typedef std::shared_ptr<ArchiveFileReader> ArchiveFileReaderPtr;
}} // namespace indexlib::file_system
