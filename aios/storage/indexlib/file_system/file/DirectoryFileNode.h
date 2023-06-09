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
#include "indexlib/file_system/file/FileNode.h"

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

class DirectoryFileNode : public FileNode
{
public:
    DirectoryFileNode() noexcept;
    ~DirectoryFileNode() noexcept;

public:
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;
    bool ReadOnly() const noexcept override { return true; }

private:
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DirectoryFileNode> DirectoryFileNodePtr;
}} // namespace indexlib::file_system
