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
#include "indexlib/file_system/file/MemFileNode.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/file/SessionFileCache.h"
#include "indexlib/file_system/fslib/FslibCommonFileWrapper.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/MemLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MmapPool.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemFileNode);

MemFileNode::MemFileNode(size_t reservedSize, bool readFromDisk, const LoadConfig& loadConfig,
                         const std::shared_ptr<util::BlockMemoryQuotaController>& memController,
                         const SessionFileCachePtr& fileCache, bool skipMmap) noexcept
    : _capacity(reservedSize)
    , _readFromDisk(readFromDisk)
    , _fileCache(fileCache)
{
    if (skipMmap) {
        _pool = std::make_unique<autil::mem_pool::Pool>();
    } else {
        _pool = std::make_unique<MmapPool>();
    }
    _memController.reset(new util::SimpleMemoryQuotaController(memController));
    auto mmapLoadStrategy = std::dynamic_pointer_cast<MmapLoadStrategy>(loadConfig.GetLoadStrategy());
    if (mmapLoadStrategy) {
        _loadStrategy.reset(mmapLoadStrategy->CreateMemLoadStrategy());
    }
}

MemFileNode::MemFileNode(const MemFileNode& other) noexcept
    : FileNode(other)
    , _length(other._length)
    , _capacity(other._length)
    , _readFromDisk(other._readFromDisk)
    , _populated(other._populated)
{
    if (dynamic_cast<MmapPool*>(other._pool.get())) {
        _pool = std::make_unique<MmapPool>();
    } else {
        _pool = std::make_unique<autil::mem_pool::Pool>();
    }
    _memController.reset(new util::SimpleMemoryQuotaController(other._memController->GetBlockMemoryController()));
    if (_length > 0 && other._data) {
        _data = (uint8_t*)_pool->allocate(_length);
        memcpy(_data, other._data, _length);
        AllocateMemQuota(_length);
    }
}

MemFileNode::~MemFileNode() noexcept
{
    [[maybe_unused]] auto ret = Close();
    _data = nullptr;
    assert(_pool);
    _pool.reset();
}

ErrorCode MemFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    assert(_pool);
    assert(_data == NULL);

    if (_readFromDisk) {
        _dataFilePath = packageOpenMeta.GetPhysicalFilePath();
        _length = packageOpenMeta.GetLength();
        _dataFileOffset = packageOpenMeta.GetOffset();
        _dataFileLength = packageOpenMeta.GetPhysicalFileLength();
    } else {
        AUTIL_LOG(ERROR, "MemFileNode of inner file only supports read from disk.");
        return FSEC_NOTSUP;
    }
    return FSEC_OK;
}

ErrorCode MemFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    assert(_pool);
    assert(_data == NULL);

    if (_readFromDisk) {
        _dataFilePath = path;
        if (fileLength < 0) {
            auto [ec, length] = FslibWrapper::GetFileLength(path);
            RETURN_IF_FS_ERROR(ec, "DoOpen failed when GetFileLength path [%s]", path.c_str());
            _length = length;
        } else {
            _length = fileLength;
        }
        _dataFileOffset = 0;
        _dataFileLength = _length;
    } else {
        assert(fileLength < 0);
        if (_capacity > 0) {
            _data = (uint8_t*)_pool->allocate(_capacity);
        }
    }
    return FSEC_OK;
}

FSResult<void> MemFileNode::Populate() noexcept
{
    ScopedLock lock(_lock);
    if (_populated) {
        return FSEC_OK;
    }

    if (_readFromDisk) {
        RETURN_IF_FS_ERROR(DoOpenFromDisk(_dataFilePath, _dataFileOffset, _length, _dataFileLength),
                           "DoOpenFromDisk [%s] failed", DebugString().c_str());
    }

    _populated = true;
    return FSEC_OK;
}

ErrorCode MemFileNode::DoOpenFromDisk(const string& path, size_t offset, size_t length, ssize_t fileLength) noexcept
{
    assert(_pool);

    fslib::fs::File* file = NULL;
    std::shared_ptr<FslibFileWrapper> fileWrapper;
    if (_inPackage && _fileCache) {
        auto [ec, retFile] = _fileCache->Get(path, fileLength);
        RETURN_IF_FS_ERROR(ec, "OpenFile [%s] failed", path.c_str());
        file = retFile;
        fileWrapper.reset(new FslibCommonFileWrapper(file, false, false));
    } else {
        auto [ec, retFileWrapper] = FslibWrapper::OpenFile(path, fslib::READ, false, fileLength);
        RETURN_IF_FS_ERROR(ec, "OpenFile [%s] failed", path.c_str());
        fileWrapper = std::move(retFileWrapper);
    }

    // if length <= 0, mmap will return MAP_FAILED(-1)
    // and set errno = 12, means Cannot allocate memory.
    ErrorCode ec = FSEC_OK;
    if (length > 0) {
        ec = LoadData(fileWrapper, offset, length);
    }
    if (_inPackage && _fileCache) {
        _fileCache->Put(file, path);
    }
    RETURN_IF_FS_ERROR(fileWrapper->Close(), "close file wrapper [%s] failed", path.c_str());
    RETURN_IF_FS_ERROR(ec, "LoadData [%s] failed", path.c_str());
    _capacity = length;
    _length = length;
    AllocateMemQuota(_length);
    return FSEC_OK;
}

void MemFileNode::AllocateMemQuota(size_t newLength) noexcept
{
    assert(_memController);
    if (newLength > _memPeak) {
        _memController->Allocate(newLength - _memPeak);
        _memPeak = newLength;
    }
}

ErrorCode MemFileNode::LoadData(const std::shared_ptr<FslibFileWrapper>& file, int64_t offset, int64_t length) noexcept
{
    _data = (uint8_t*)_pool->allocate(length);
    if (!_loadStrategy || _loadStrategy->GetInterval() == 0) {
        size_t realLength = 0;
        RETURN_IF_FS_ERROR(file->PRead(_data, length, offset, realLength),
                           "PRead file[%s] FAILED, length[%lu] offset[%ld]", file->GetFileName(), length, offset);

    } else {
        for (int64_t offset = 0; offset < length; offset += _loadStrategy->GetSlice()) {
            int64_t readLen = min(length - offset, (int64_t)_loadStrategy->GetSlice());
            size_t realLength = 0;
            RETURN_IF_FS_ERROR(file->PRead(_data + offset, readLen, offset, realLength),
                               "PRead file[%s] FAILED, length[%lu] offset[%ld]", file->GetFileName(), readLen, offset);
            usleep(_loadStrategy->GetInterval() * 1000);
        }
    }
    return FSEC_OK;
}

FSFileType MemFileNode::GetType() const noexcept { return FSFT_MEM; }

size_t MemFileNode::GetLength() const noexcept { return _length; }

void* MemFileNode::GetBaseAddress() const noexcept
{
    assert(!_readFromDisk || (_readFromDisk && _populated));
    return _data;
}

FSResult<size_t> MemFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(buffer);
    assert(!_readFromDisk || (_readFromDisk && _populated));

    if ((offset + length) <= _length) {
        assert((_length == 0) || (_data != NULL));
        memcpy(buffer, (void*)(_data + offset), length);
        return {FSEC_OK, length};
    }

    AUTIL_LOG(ERROR, "read file out of range, offset: [%lu], read length: [%lu], file length: [%lu]", offset, length,
              _length);
    return {FSEC_BADARGS, 0};
}

ByteSliceList* MemFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    assert(!_readFromDisk || (_readFromDisk && _populated));

    if ((offset + length) <= _length) {
        assert((_length == 0) || (_data != NULL));
        ByteSlice* slice = ByteSlice::CreateObject(0);
        slice->size = length;
        slice->data = (uint8_t*)_data + offset;
        ByteSliceList* sliceList = new ByteSliceList(slice);
        return sliceList;
    }

    AUTIL_LOG(ERROR,
              "read file out of range, offset: [%lu], "
              "read length: [%lu], file length: [%lu]",
              offset, length, _length);
    return NULL;
}

FSResult<size_t> MemFileNode::Write(const void* buffer, size_t length) noexcept
{
    assert(buffer);
    if (length == 0) {
        return {FSEC_OK, length};
    }
    if (length + _length > _capacity) {
        Extend(CalculateSize(length + _length));
    }
    assert(_data);
    memcpy(_data + _length, buffer, length);
    _length += length;
    AllocateMemQuota(_length);
    return {FSEC_OK, length};
}

FSResult<void> MemFileNode::Close() noexcept
{
    _populated = false;
    if (_data != NULL) {
        _pool->deallocate(_data, _capacity);
        _memController->Free(_memPeak);
        _memPeak = 0;
        _capacity = 0;
        _length = 0;
        _data = NULL;
    }
    return FSEC_OK;
}

size_t MemFileNode::CalculateSize(size_t needLength) noexcept
{
    size_t size = _capacity ? _capacity : needLength;
    while (size < needLength) {
        size *= 2;
    }
    return size;
}

void MemFileNode::Resize(size_t size) noexcept
{
    if (size > _capacity) {
        Extend(size);
    }
    _length = size;
    AllocateMemQuota(_length);
}

void MemFileNode::Extend(size_t extendSize) noexcept
{
    assert(extendSize > _capacity);
    assert(_pool);
    if (!_data) {
        _data = (uint8_t*)_pool->allocate(extendSize);
        _capacity = extendSize;
        return;
    }
    uint8_t* dstBuffer = (uint8_t*)_pool->allocate(extendSize);
    memcpy(dstBuffer, _data, _length);
    _pool->deallocate(_data, _capacity);
    _data = dstBuffer;
    _capacity = extendSize;
}

MemFileNode* MemFileNode::Clone() const noexcept { return new MemFileNode(*this); }

}} // namespace indexlib::file_system
