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

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "future_lite/Common.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Helper.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"

namespace indexlib {
namespace file_system {
class PackageOpenMeta;
} // namespace file_system
namespace util {
class ByteSliceList;
class StatusMetricReporter;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {
class FileSystemMetricsReporter;

class FileNode
{
public:
    FileNode() noexcept;
    FileNode(const FileNode& other) noexcept;
    virtual ~FileNode() noexcept;

public:
    // fileLength is a hint, -1 by default.
    FSResult<void> Open(const std::string& logicalPath, const std::string& physicalPath, FSOpenType openType,
                        int64_t fileLength) noexcept;
    // for inner File open in package file
    FSResult<void> Open(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept;

public:
    virtual FSResult<void> Close() noexcept = 0;

    // load file into memory
    virtual FSResult<void> Populate() noexcept { return FSEC_OK; }
    virtual FSResult<size_t> Prefetch(size_t length, size_t offset, ReadOption option) noexcept;

    virtual void* GetBaseAddress() const noexcept = 0;
    virtual FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept = 0;
    virtual util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept = 0;
    virtual FSResult<size_t> Write(const void* buffer, size_t length) noexcept = 0;
    virtual FileNode* Clone() const noexcept;
    virtual FSFileType GetType() const noexcept = 0;
    virtual size_t GetLength() const noexcept = 0;
    virtual bool ReadOnly() const noexcept = 0;

    // future_lite
    virtual future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                                  ReadOption option) noexcept(false);
    virtual future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) noexcept(false);
    virtual future_lite::Future<size_t> PrefetchAsync(size_t length, size_t offset, ReadOption option) noexcept(false);
    virtual future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false);

    // FL_LAZY
    virtual FL_LAZY(FSResult<size_t>)
        ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept;
    virtual FL_LAZY(FSResult<uint32_t>) ReadUInt32AsyncCoro(size_t offset, ReadOption option) noexcept;
    virtual FL_LAZY(FSResult<size_t>) PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept;
    virtual FL_LAZY(FSResult<uint32_t>) ReadVUInt32AsyncCoro(size_t offset, ReadOption option) noexcept;
    virtual future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                                    ReadOption option) noexcept;

    virtual bool MatchType(FSOpenType type, FSFileType fileType, bool needWrite) const noexcept;
    virtual void InitMetricReporter(FileSystemMetricsReporter* reporter) noexcept { UpdateFileLengthMetric(reporter); }

public:
    const std::string& GetLogicalPath() const noexcept { return _logicalPath; }
    const std::string& GetPhysicalPath() const noexcept { return _physicalPath; }
    FSOpenType GetOpenType() const noexcept { return _openType; }
    void SetDirty(bool isDirty) noexcept { _dirty = isDirty; }
    bool IsDirty() const noexcept { return _dirty; }
    void SetInPackage(bool inPackage) noexcept { _inPackage = inPackage; }
    bool IsInPackage() const noexcept { return _inPackage; }
    void SetOriginalOpenType(FSOpenType type) noexcept { _originalOpenType = type; }
    FSOpenType GetOriginalOpenType() const noexcept { return _originalOpenType; }
    void SetMetricGroup(FSMetricGroup metricGroup) noexcept { _metricGroup = metricGroup; }
    FSMetricGroup GetMetricGroup() const noexcept { return _metricGroup; }
    std::string DebugString() const noexcept;
    void UpdateFileLengthMetric(FileSystemMetricsReporter* reporter) noexcept;
    void UpdateFileNodeUseCount(uint32_t count) noexcept;

private:
    virtual ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept = 0;
    virtual ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept = 0;

protected:
    std::string _logicalPath;
    std::string _physicalPath;
    FSOpenType _openType;
    FSOpenType _originalOpenType;
    FSMetricGroup _metricGroup;
    volatile bool _dirty;
    volatile bool _inPackage;
    std::shared_ptr<util::StatusMetricReporter> _fileLenReporter;
    std::shared_ptr<util::StatusMetricReporter> _fileNodeUseCountReporter;
    util::StatusMetricReporter* _fileNodeUseCountReporterPtr = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
inline future_lite::coro::Lazy<std::vector<FSResult<size_t>>> FileNode::BatchReadOrdered(const BatchIO& batchIO,
                                                                                         ReadOption option) noexcept
{
    assert(std::is_sorted(batchIO.begin(), batchIO.end()));
    assert(batchIO.empty() || (batchIO.rbegin()->len + batchIO.rbegin()->offset <= GetLength()));
    std::vector<FSResult<size_t>> result(batchIO.size());
    for (size_t i = 0; i < batchIO.size(); ++i) {
        auto& singleIO = batchIO[i];
        result[i] = Read(singleIO.buffer, singleIO.len, singleIO.offset, option);
    }
    co_return result;
}

inline future_lite::Future<size_t> FileNode::PrefetchAsync(size_t length, size_t offset,
                                                           ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture<size_t>(0);
}
inline FL_LAZY(FSResult<size_t>) FileNode::PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept
{
    FL_CORETURN FSResult<size_t> {FSEC_OK, 0ul};
}

inline future_lite::Future<size_t> FileNode::ReadAsync(void* buffer, size_t length, size_t offset,
                                                       ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture(Read(buffer, length, offset, option).GetOrThrow());
}
inline FL_LAZY(FSResult<size_t>) FileNode::ReadAsyncCoro(void* buffer, size_t length, size_t offset,
                                                         ReadOption option) noexcept
{
    FL_CORETURN Read(buffer, length, offset, option);
}

inline future_lite::Future<uint32_t> FileNode::ReadUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    uint32_t buffer = 0;
    auto readSize = Read(static_cast<void*>(&buffer), sizeof(buffer), offset, option).GetOrThrow();
    assert(readSize == sizeof(buffer));
    (void)readSize;
    return future_lite::makeReadyFuture<uint32_t>(buffer);
}
inline FL_LAZY(FSResult<uint32_t>) FileNode::ReadUInt32AsyncCoro(size_t offset, ReadOption option) noexcept
{
    uint32_t buffer = 0;
    auto [ec, readSize] = Read(static_cast<void*>(&buffer), sizeof(buffer), offset, option);
    FL_CORETURN2_IF_FS_ERROR(ec, buffer, "Read failed, offset[%lu]", offset);
    assert(readSize == sizeof(buffer));
    (void)readSize;
    FL_CORETURN FSResult<uint32_t> {FSEC_OK, buffer};
}

inline FL_LAZY(FSResult<uint32_t>) FileNode::ReadVUInt32AsyncCoro(size_t offset, ReadOption option) noexcept
{
    auto fileLen = GetLength();
    if (unlikely(offset >= fileLen)) {
        AUTIL_LOG(ERROR, "read file out of range, offset: [%lu], file length: [%lu]", offset, fileLen);
        FL_CORETURN FSResult<uint32_t> {FSEC_BADARGS, 0u};
    }
    auto bufferPtr = std::make_unique<uint64_t>(0);
    auto buffer = static_cast<void*>(bufferPtr.get());
    size_t bufferSize = std::min(sizeof(uint64_t), fileLen - offset);
    auto [ec, _] = FL_COAWAIT ReadAsyncCoro(buffer, bufferSize, offset, option);
    FL_CORETURN2_IF_FS_ERROR(ec, 0u, "ReadAsyncCoro failed, offset[%lu]", offset);
    uint8_t* byte = reinterpret_cast<uint8_t*>(bufferPtr.get());
    uint32_t value = (*byte) & 0x7f;
    int shift = 7;
    while ((*byte) & 0x80) {
        ++byte;
        value |= ((*byte & 0x7F) << shift);
        shift += 7;
    }
    FL_CORETURN FSResult<uint32_t> {FSEC_OK, value};
}

inline bool FileNode::MatchType(FSOpenType type, FSFileType fileType, bool needWrite) const noexcept
{
    if (needWrite && ReadOnly()) {
        return false;
    }
    return (_openType == type) || (_originalOpenType == type);
}

typedef std::shared_ptr<FileNode> FileNodePtr;
}} // namespace indexlib::file_system
