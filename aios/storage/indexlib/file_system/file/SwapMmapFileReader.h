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
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/file/SwapMmapFileNode.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {

class SwapMmapFileReader : public FileReader
{
public:
    SwapMmapFileReader(const std::shared_ptr<SwapMmapFileNode>& fileNode) noexcept;
    ~SwapMmapFileReader() noexcept;

public:
    FSResult<void> Open() noexcept override
    {
        assert(_fileNode);
        return FSEC_OK;
    }

    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override
    {
        return _fileNode ? _fileNode->Read(buffer, length, offset, option) : FSResult<size_t> {FSEC_OK, 0};
    }

    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override
    {
        assert(false);
        return {FSEC_NOTSUP, 0};
    }

    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override
    {
        return _fileNode ? _fileNode->ReadToByteSliceList(length, offset, option) : NULL;
    }

    void* GetBaseAddress() const noexcept override { return _fileNode ? _fileNode->GetBaseAddress() : NULL; }

    size_t GetLength() const noexcept override { return _fileNode ? _fileNode->GetLength() : 0; }

    const std::string& GetLogicalPath() const noexcept override
    {
        if (!_fileNode) {
            static std::string tmp;
            return tmp;
        }
        return _fileNode->GetLogicalPath();
    }

    const std::string& GetPhysicalPath() const noexcept override
    {
        if (!_fileNode) {
            static std::string tmp;
            return tmp;
        }
        return _fileNode->GetPhysicalPath();
    }

    FSResult<void> Close() noexcept override
    {
        _fileNode.reset();
        return FSEC_OK;
    }

    FSOpenType GetOpenType() const noexcept override { return _fileNode ? _fileNode->GetOpenType() : FSOT_UNKNOWN; }

    FSFileType GetType() const noexcept override { return _fileNode ? _fileNode->GetType() : FSFT_UNKNOWN; }

private:
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return _fileNode; }

private:
    std::shared_ptr<SwapMmapFileNode> _fileNode;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SwapMmapFileReader> SwapMmapFileReaderPtr;
}} // namespace indexlib::file_system
