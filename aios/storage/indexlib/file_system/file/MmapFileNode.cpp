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
#include "indexlib/file_system/file/MmapFileNode.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <errno.h>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/MMapFile.h"
#include "future_lite/Future.h"
#include "future_lite/Helper.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileCarrier.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/load_config/WarmupStrategy.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MmapFileNode);

MmapFileNode::MmapFileNode(const LoadConfig& loadConfig, const util::BlockMemoryQuotaControllerPtr& memController,
                           bool readOnly) noexcept
    : _loadStrategy(std::dynamic_pointer_cast<MmapLoadStrategy>(loadConfig.GetLoadStrategy()))
    , _data(NULL)
    , _length(0)
    , _type(FSFT_UNKNOWN)
    , _warmup(loadConfig.GetWarmupStrategy().GetWarmupType() != WarmupStrategy::WARMUP_NONE)
    , _populated(false)
    , _readOnly(readOnly)
{
    assert(_loadStrategy);
    assert(_loadStrategy->GetSlice() != 0);
    _memController.reset(new util::SimpleMemoryQuotaController(memController));
}

MmapFileNode::~MmapFileNode() noexcept { [[maybe_unused]] auto ret = Close(); }

ErrorCode MmapFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    const string& packageDataFilePath = packageOpenMeta.GetPhysicalFilePath();
    size_t length = packageOpenMeta.GetLength();
    size_t offset = packageOpenMeta.GetOffset();

    AUTIL_LOG(DEBUG,
              "mmap innerfilePath:%s, packageDataFilePath:%s,"
              "fileLength:%lu, offset:%lu",
              DebugString().c_str(), packageDataFilePath.c_str(), length, offset);

    if (packageOpenMeta.physicalFileCarrier) {
        const std::shared_ptr<MmapFileNode>& mmapFileNode = packageOpenMeta.physicalFileCarrier->GetMmapLockFileNode();
        if (mmapFileNode) {
            AUTIL_LOG(DEBUG, "file[%s] in package[%s] shared, useCount[%ld]", DebugString().c_str(),
                      packageDataFilePath.c_str(), mmapFileNode.use_count());
            _dependFileNode = mmapFileNode;
            return DoOpenInSharedFile(packageDataFilePath, offset, length, mmapFileNode->_file, openType);
        }
    }
    assert(IsInPackage());
    return DoOpenMmapFile(packageDataFilePath, offset, length, openType, packageOpenMeta.GetPhysicalFileLength());
}

ErrorCode MmapFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    return DoOpenMmapFile(path, 0, fileLength, openType, fileLength);
}

ErrorCode MmapFileNode::DoOpenMmapFile(const string& path, size_t offset, size_t length, FSOpenType openType,
                                       ssize_t fileLength) noexcept
{
    assert(!_file);
    int mapFlag = 0;
    int prot = 0;
    if (_readOnly) {
        mapFlag = MAP_SHARED;
        prot = PROT_READ;
    } else {
        mapFlag = MAP_PRIVATE;
        prot = PROT_READ | PROT_WRITE;
    }

    _type = _loadStrategy->IsLock() ? FSFT_MMAP_LOCK : FSFT_MMAP;

    AUTIL_LOG(DEBUG,
              "MMap file[%s], loadStrategy[%s], openType[%d], "
              "type[%d], prot [%d], mapFlag [%d]",
              path.c_str(), ToJsonString(*_loadStrategy, true).c_str(), openType, _type, prot, mapFlag);

    if (length == 0) {
        // the known empty file, may be in package file or file meta cached
        _file.reset(new fslib::fs::MMapFile(path, -1, NULL, 0, 0, fslib::EC_OK));
    } else {
        auto ret = FslibWrapper::MmapFile(path, fslib::READ, NULL, length, prot, mapFlag, offset, fileLength);
        RETURN_IF_FS_ERROR(ret.Code(), "mmap file [%s] failed", path.c_str());
        _file = ret.Value();
    }
    _data = _file->getBaseAddress();
    assert(_file->getLength() >= 0);
    _length = _file->getLength();

    if (_loadStrategy->IsLock() && !_warmup) {
        AUTIL_LOG(DEBUG, "lock enforce warmup, set warmp is true.");
        _warmup = true;
    }
    if (_loadStrategy->IsLock()) {
        _memController->Allocate(_length);
    }
    return FSEC_OK;
}

// multiple file share one mmaped shared package file
ErrorCode MmapFileNode::DoOpenInSharedFile(const string& path, size_t offset, size_t length,
                                           const std::shared_ptr<fslib::fs::MMapFile>& sharedFile,
                                           FSOpenType openType) noexcept
{
    assert(!_file);

    AUTIL_LOG(DEBUG, "MMap file[%s], loadStrategy[%s], openType[%d], type[%d], ", path.c_str(),
              ToJsonString(*_loadStrategy, true).c_str(), openType, _type);

    _file = sharedFile;
    _data = _file->getBaseAddress() + offset;
    _length = length;
    _type = _loadStrategy->IsLock() ? FSFT_MMAP_LOCK : FSFT_MMAP;

    if (_loadStrategy->IsLock() && !_warmup) {
        AUTIL_LOG(DEBUG, "lock enforce warmup, set warmp is true.");
        _warmup = true;
    }

    // dcache mmapFile has load all data in memory
    if (-1 == _file->getFd()) {
        if (!_loadStrategy->IsLock()) {
            AUTIL_LOG(ERROR, "file[%s] in shared package [%s] is anonymous mmap, should must be mmap lock",
                      DebugString().c_str(), sharedFile->getFileName());
            _type = FSFT_MMAP_LOCK;
        }
    }
    return FSEC_OK;
}

FSResult<void> MmapFileNode::Populate() noexcept
{
    ScopedLock lock(_lock);
    if (_populated) {
        return FSEC_OK;
    }
    if (_dependFileNode) {
        RETURN_IF_FS_ERROR(_dependFileNode->Populate(), "Populate [%s] failed", DebugString().c_str());
        _populated = true;
        return FSEC_OK;
    }

    fslib::ErrorCode ec = _file->populate(_loadStrategy->IsLock(), (int64_t)_loadStrategy->GetSlice(),
                                          (int64_t)_loadStrategy->GetInterval());
    if (ec == fslib::EC_OK) {
        _populated = true;
        return FSEC_OK;
    } else if (ec != fslib::EC_NOTSUP) {
        AUTIL_LOG(ERROR, "populate file [%s] failed, ec[%d], lock[%d], slice[%uB], interval[%ums]",
                  DebugString().c_str(), ec, _loadStrategy->IsLock(), _loadStrategy->GetSlice(),
                  _loadStrategy->GetInterval());
        return ParseFromFslibEC(ec);
    } else if (-1 == _file->getFd()) {
        // for old dcache version without populate
        _populated = true;
        return FSEC_OK;
    }

    if (_data && _loadStrategy->IsAdviseRandom()) {
        AUTIL_LOG(TRACE1, "madvse random in MmapFileNode.");
        if (madvise(_data, _length, MADV_RANDOM) < 0) {
            AUTIL_LOG(WARN, "madvice failed! errno:%d", errno);
        }
    }

    if (_warmup) {
        RETURN_IF_FS_ERROR(LoadData(), "load data for file[%s] failed", DebugString().c_str());
    }
    _populated = true;
    return FSEC_OK;
}

FSFileType MmapFileNode::GetType() const noexcept { return _type; }

size_t MmapFileNode::GetLength() const noexcept { return _length; }

void* MmapFileNode::GetBaseAddress() const noexcept
{
    assert(_populated);
    return _data;
}

FSResult<size_t> MmapFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_file);
    // assert(!_warmup || (_warmup && _populated));
    assert(_populated);

    if ((offset + length) <= _length) {
        assert((_length == 0) || (_data != NULL));
        memcpy(buffer, (void*)((uint8_t*)_data + offset), length);
        return {FSEC_OK, length};
    }

    AUTIL_LOG(ERROR, "read file [%s] out of range, offset [%lu], read length: [%lu], file length: [%lu]",
              DebugString().c_str(), offset, length, _length);
    return {FSEC_BADARGS, 0};
}

ByteSliceList* MmapFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_file);
    assert(_populated);

    if ((offset + length) <= _length) {
        assert((_length == 0) || (_data != NULL));
        ByteSlice* slice = ByteSlice::CreateObject(0);
        slice->size = length;
        slice->data = (uint8_t*)_data + offset;
        ByteSliceList* sliceList = new ByteSliceList(slice);
        return sliceList;
    }

    AUTIL_LOG(ERROR,
              "read file [%s] out of range, "
              "offset [%lu], read length: [%lu], file length: [%lu]",
              DebugString().c_str(), offset, length, _length);
    return NULL;
}

FSResult<size_t> MmapFileNode::Write(const void* buffer, size_t length) noexcept
{
    AUTIL_LOG(ERROR, "not supported Write");
    return {FSEC_NOTSUP, 0};
}

FSResult<void> MmapFileNode::Close() noexcept
{
    if (_dependFileNode) {
        AUTIL_LOG(DEBUG, "close file[%s] in package[%s] shared, useCount[%ld]", DebugString().c_str(),
                  _dependFileNode->DebugString().c_str(), _dependFileNode.use_count());
    }
    _populated = false;
    _file.reset();
    _dependFileNode.reset();
    return FSEC_OK;
}

ErrorCode MmapFileNode::LoadData() noexcept
{
    const char* base = (const char*)_data;
    AUTIL_LOG(DEBUG, "Begin load file [%s], length [%ldB], slice [%uB], interval [%ums].", DebugString().c_str(),
              _length, _loadStrategy->GetSlice(), _loadStrategy->GetInterval());

    for (int64_t offset = 0; offset < (int64_t)_length; offset += _loadStrategy->GetSlice()) {
        int64_t lockLen = min((int64_t)_length - offset, (int64_t)_loadStrategy->GetSlice());
        (void)WarmUp(base + offset, lockLen);
        if (_loadStrategy->IsLock()) {
            if (mlock(base + offset, lockLen) < 0) {
                AUTIL_LOG(ERROR, "lock file[%s] FAILED, errno[%d], errmsg[%s]", DebugString().c_str(), errno,
                          strerror(errno));
                return FSEC_ERROR;
            }
        }
        usleep(_loadStrategy->GetInterval() * 1000);
    }

    AUTIL_LOG(DEBUG, "End load.");
    return FSEC_OK;
}

uint8_t MmapFileNode::WarmUp(const char* addr, int64_t len) noexcept
{
    static int WARM_UP_PAGE_SIZE = getpagesize();

    uint8_t* pStart = (uint8_t*)addr;
    uint8_t* pEnd = pStart + len;

    uint8_t noUse = 0;
    while (pStart < pEnd) {
        noUse += *pStart;
        pStart += WARM_UP_PAGE_SIZE;
    }
    return noUse;
}

future_lite::Future<uint32_t> MmapFileNode::ReadVUInt32Async(size_t offset, ReadOption option) noexcept
{
    uint8_t* byte = static_cast<uint8_t*>(_data) + offset;
    uint32_t value = (*byte) & 0x7f;
    int shift = 7;
    while ((*byte) & 0x80) {
        ++byte;
        value |= ((*byte & 0x7F) << shift);
        shift += 7;
    }
    return future_lite::makeReadyFuture<uint32_t>(value);
}

bool MmapFileNode::MatchType(FSOpenType type, FSFileType fileType, bool needWrite) const noexcept
{
    if (needWrite && ReadOnly()) {
        return false;
    }
    if (_openType != type && _originalOpenType != type) {
        return false;
    }
    if (fileType != FSFT_UNKNOWN && _type != fileType) {
        return false;
    }
    return true;
}
}} // namespace indexlib::file_system
