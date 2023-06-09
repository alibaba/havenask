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

#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class MemFileWriter : public FileWriterImpl
{
private:
    typedef util::AlignedSliceArray<char> SliceArray;

public:
    // for not packaged file
    MemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController, Storage* storage,
                  const WriterOption& writerOption, FileWriterImpl::UpdateFileSizeFunc&& updateFileSizeFunc) noexcept;
    ~MemFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;
    ErrorCode DoClose() noexcept override;

    // todo: add ut for useBuffer = false
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override;
    FSResult<void> Truncate(size_t truncateSize) noexcept override;
    void* GetBaseAddress() noexcept override;

protected:
    FSResult<std::shared_ptr<MemFileNode>> CreateFileNodeFromSliceArray() noexcept;

private:
    const static uint64_t SLICE_LEN = 32 * 1024 * 1024;
    const static uint32_t SLICE_NUM = 32 * 1024;
    const static bool NEED_SKIP_MMAP;

protected:
    Storage* _storage;
    SliceArray* _sliceArray;
    std::shared_ptr<MemFileNode> _memFileNode;
    util::BlockMemoryQuotaControllerPtr _memController;
    size_t _length;
    bool _isClosed;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemFileWriter> MemFileWriterPtr;
}} // namespace indexlib::file_system
