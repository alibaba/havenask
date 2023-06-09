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

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriterImpl.h"

namespace indexlib { namespace file_system {
class SliceFileNode;

class SliceFileWriter : public FileWriterImpl
{
public:
    SliceFileWriter(const std::shared_ptr<SliceFileNode>& sliceFileNode,
                    UpdateFileSizeFunc&& updateFileSizeFunc) noexcept;
    ~SliceFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override;
    ErrorCode DoClose() noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override;
    FSResult<void> Truncate(size_t truncateSize) noexcept override;
    void* GetBaseAddress() noexcept override;

private:
    std::shared_ptr<SliceFileNode> _sliceFileNode;
    size_t _length;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SliceFileWriter> SliceFileWriterPtr;
}} // namespace indexlib::file_system
