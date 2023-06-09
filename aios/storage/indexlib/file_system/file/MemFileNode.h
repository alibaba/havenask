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
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/types.h>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"

namespace autil { namespace mem_pool {
class PoolBase;
}} // namespace autil::mem_pool
namespace indexlib {
namespace file_system {
class LoadConfig;
class PackageOpenMeta;
class SessionFileCache;
class MemLoadStrategy;
class FslibFileWrapper;
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

namespace indexlib { namespace file_system {

class MemFileNode : public FileNode
{
public:
    MemFileNode(size_t reservedSize, bool readFromDisk, const LoadConfig& loadConfig,
                const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                const std::shared_ptr<SessionFileCache>& fileCache = std::shared_ptr<SessionFileCache>(),
                bool skipMmap = false) noexcept;
    MemFileNode(const MemFileNode& other) noexcept;
    ~MemFileNode() noexcept;

public:
    FSResult<void> Populate() noexcept override;
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;

    MemFileNode* Clone() const noexcept override;

    void Resize(size_t size) noexcept;
    bool ReadOnly() const noexcept override { return false; }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

    ErrorCode DoOpenFromDisk(const std::string& path, size_t offset, size_t length, ssize_t fileLength) noexcept;
    size_t CalculateSize(size_t needLength) noexcept;
    void Extend(size_t extendSize) noexcept;

    void AllocateMemQuota(size_t newLength) noexcept;
    ErrorCode LoadData(const std::shared_ptr<FslibFileWrapper>& file, int64_t offset, int64_t length) noexcept;

private:
    mutable autil::ThreadMutex _lock;

    size_t _length = 0;
    size_t _memPeak = 0;
    size_t _capacity = 0;
    size_t _dataFileOffset = 0;
    ssize_t _dataFileLength = -1;
    bool _readFromDisk = false;
    bool _populated = false;

    std::unique_ptr<autil::mem_pool::PoolBase> _pool;
    uint8_t* _data = nullptr;

    std::string _dataFilePath;
    std::shared_ptr<util::SimpleMemoryQuotaController> _memController;
    std::shared_ptr<MemLoadStrategy> _loadStrategy;
    std::shared_ptr<SessionFileCache> _fileCache;

private:
    AUTIL_LOG_DECLARE();
    friend class InMemFileTest;
};

typedef std::shared_ptr<MemFileNode> MemFileNodePtr;
//////////////////////////////////////////////////////////////////

}} // namespace indexlib::file_system
