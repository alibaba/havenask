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
#include <sys/types.h>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/SessionFileCache.h"

namespace indexlib {
namespace file_system {
class DataFlushController;
class PackageOpenMeta;
struct ReadOption;
} // namespace file_system
namespace util {
class ByteSliceList;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {

class BufferedFileNode : public FileNode
{
public:
    BufferedFileNode(fslib::Flag mode = fslib::READ,
                     const SessionFileCachePtr& fileCache = SessionFileCachePtr()) noexcept;
    ~BufferedFileNode() noexcept;

public:
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;
    bool ReadOnly() const noexcept override { return _mode == fslib::READ; }
    bool ForceFlush() noexcept;

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

private:
    FSResult<void> CreateFile(const std::string& fileName, bool useDirectIO, ssize_t physicalFileLength,
                              size_t beginOffset, ssize_t fileLength) noexcept;
    void IOCtlPrefetch(ssize_t blockSize, size_t beginOffset, ssize_t fileLength) noexcept;
    void CheckError() noexcept;

private:
    static const uint32_t DEFAULT_READ_WRITE_LENGTH = 64 * 1024 * 1024;

private:
    size_t _length = 0;
    size_t _fileBeginOffset = 0;
    fslib::Flag _mode = fslib::READ;
    std::string _originalFileName;
    fslib::fs::FilePtr _file;
    SessionFileCachePtr _fileCache;
    DataFlushController* _flushController = nullptr;

private:
    friend class BufferedFileWriterTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BufferedFileNode> BufferedFileNodePtr;
}} // namespace indexlib::file_system
