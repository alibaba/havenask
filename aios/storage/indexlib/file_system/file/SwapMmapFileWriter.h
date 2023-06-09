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
#include "indexlib/file_system/file/SwapMmapFileNode.h"

namespace indexlib { namespace file_system {
class Storage;

class SwapMmapFileWriter : public FileWriterImpl
{
public:
    SwapMmapFileWriter(const std::shared_ptr<SwapMmapFileNode>& fileNode, Storage* storage,
                       FileWriterImpl::UpdateFileSizeFunc&& updateFileSizeFunc) noexcept;
    ~SwapMmapFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;
    ErrorCode DoClose() noexcept override;
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    FSResult<void> Truncate(size_t truncateSize) noexcept override;
    void* GetBaseAddress() noexcept override { return _fileNode->GetBaseAddress(); }

public:
    ErrorCode WarmUp() noexcept { return _fileNode->WarmUp(); }
    void SetDirty(bool dirty) noexcept
    {
        if (_fileNode) {
            _fileNode->SetDirty(dirty);
        }
    }

private:
    void TEST_Sync() noexcept(false);

private:
    std::shared_ptr<SwapMmapFileNode> _fileNode;
    Storage* _storage;
    size_t _length = 0;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SwapMmapFileWriter> SwapMmapFileWriterPtr;
}} // namespace indexlib::file_system
