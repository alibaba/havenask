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

#include <atomic>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <sys/types.h>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"

namespace fslib { namespace fs {
class MMapFile;
}} // namespace fslib::fs
namespace indexlib {
namespace file_system {
class LoadConfig;
class PackageOpenMeta;
struct ReadOption;
} // namespace file_system
namespace util {
class ByteSliceList;
template <typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
class BlockMemoryQuotaController;
} // namespace util
} // namespace indexlib

AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::file_system, MmapLoadStrategy);

namespace indexlib { namespace file_system {

class MmapFileNode : public FileNode
{
public:
    MmapFileNode(const LoadConfig& loadConfig, const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                 bool readOnly) noexcept;
    ~MmapFileNode() noexcept;

public:
    FSResult<void> Populate() noexcept override;
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;
    future_lite::Future<FSResult<uint32_t>> ReadVUInt32Async(size_t offset, ReadOption option) noexcept override;
    bool ReadOnly() const noexcept override { return _readOnly; };
    bool MatchType(FSOpenType type, FSFileType fileType, bool needWrite) const noexcept override;

protected:
    ErrorCode LoadData() noexcept;
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;

private:
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

private:
    ErrorCode DoOpenMmapFile(const std::string& path, size_t offset, size_t length, FSOpenType openType,
                             ssize_t fileLength) noexcept;
    // multiple file share one mmaped shared package file
    ErrorCode DoOpenInSharedFile(const std::string& path, size_t offset, size_t length,
                                 const std::shared_ptr<fslib::fs::MMapFile>& sharedFile, FSOpenType openType) noexcept;
    uint8_t WarmUp(const char* addr, int64_t len) noexcept;

protected:
    mutable autil::ThreadMutex _lock;
    std::shared_ptr<fslib::fs::MMapFile> _file;
    std::shared_ptr<util::SimpleMemoryQuotaController> _memController;
    std::shared_ptr<MmapLoadStrategy> _loadStrategy;
    std::shared_ptr<FileNode> _dependFileNode; // dcache, for hold shared package file
    void* _data;
    size_t _length;
    FSFileType _type;
    bool _warmup;
    bool _populated;
    bool _readOnly;

private:
    AUTIL_LOG_DECLARE();
    friend class MmapFileNodeTest;
};

typedef std::shared_ptr<MmapFileNode> MmapFileNodePtr;
}} // namespace indexlib::file_system
