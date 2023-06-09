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
#include "indexlib/file_system/file/SliceFileNode.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/slice_array/BytesAlignedSliceArray.h"

namespace indexlib { namespace file_system {
struct ReadOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class SliceFileReader : public FileReader
{
public:
    SliceFileReader(const std::shared_ptr<SliceFileNode>& sliceFileNode) noexcept;
    ~SliceFileReader() noexcept;

public:
    FSResult<void> Open() noexcept override { return FSEC_OK; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    void* GetBaseAddress() const noexcept override;
    size_t GetLength() const noexcept override;
    const std::string& GetLogicalPath() const noexcept override;
    const std::string& GetPhysicalPath() const noexcept override;
    FSOpenType GetOpenType() const noexcept override;
    FSFileType GetType() const noexcept override;
    FSResult<void> Close() noexcept override;
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return _sliceFileNode; }

public:
    const util::BytesAlignedSliceArrayPtr& GetBytesAlignedSliceArray() const noexcept
    {
        assert(_sliceFileNode);
        return _sliceFileNode->GetBytesAlignedSliceArray();
    }

private:
    std::shared_ptr<SliceFileNode> _sliceFileNode;
    size_t _offset;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SliceFileReader> SliceFileReaderPtr;
}} // namespace indexlib::file_system
