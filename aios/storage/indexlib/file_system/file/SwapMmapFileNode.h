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
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

namespace indexlib {
namespace file_system {
class PackageOpenMeta;
struct ReadOption;
} // namespace file_system
namespace util {
class ByteSliceList;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {

class SwapMmapFileNode : public MmapFileNode
{
public:
    SwapMmapFileNode(size_t fileSize, const util::BlockMemoryQuotaControllerPtr& memController) noexcept;
    ~SwapMmapFileNode() noexcept;

public:
    size_t GetLength() const noexcept override { return _cursor; }

    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;

    FSResult<void> OpenForRead(const std::string& logicalPath, const std::string& physicalPath,
                               FSOpenType openType) noexcept;
    FSResult<void> OpenForWrite(const std::string& logicalPath, const std::string& physicalPath,
                                FSOpenType openType) noexcept;
    bool ReadOnly() const noexcept override { return _readOnly; }

public:
    ErrorCode WarmUp() noexcept;
    void Resize(size_t size) noexcept;
    size_t GetCapacity() const noexcept { return _length; }
    FSResult<void> Sync() noexcept;
    void SetRemainFile() noexcept { _remainFile = true; }
    void SetCleanCallback(std::function<void()>&& cleanFunc) noexcept { _cleanFunc = std::move(cleanFunc); }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

private:
    std::function<void()> _cleanFunc;
    size_t _cursor;
    volatile bool _remainFile;
    bool _readOnly;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SwapMmapFileNode> SwapMmapFileNodePtr;
}} // namespace indexlib::file_system
