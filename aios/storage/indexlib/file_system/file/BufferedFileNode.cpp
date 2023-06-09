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
#include "indexlib/file_system/file/BufferedFileNode.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/file_system/package/PackageOpenMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileNode);

BufferedFileNode::BufferedFileNode(fslib::Flag mode, const SessionFileCachePtr& fileCache) noexcept
    : _length(0)
    , _fileBeginOffset(0)
    , _mode(mode)
    , _fileCache(fileCache)
    , _flushController(NULL)
{
    assert(_mode == fslib::READ || _mode == fslib::WRITE || _mode == fslib::APPEND);
}

BufferedFileNode::~BufferedFileNode() noexcept
{
    if (_file) {
        auto ret = Close();
        if (!ret.OK()) {
            AUTIL_LOG(ERROR, "close failed");
        }
    }
}

ErrorCode BufferedFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    if (_mode == fslib::READ) {
        if (fileLength < 0) {
            auto [ec, length] = FslibWrapper::GetFileLength(path);
            RETURN_IF_FS_ERROR(ec, "Get file : [%s] meta FAILED", path.c_str());
            _length = (size_t)length;
        } else {
            _length = (size_t)fileLength;
        }
        _fileBeginOffset = 0;
        auto ec = CreateFile(path, false, _length, _fileBeginOffset, _length);
        RETURN_IF_FS_ERROR(ec, "CrateFile failed");
    } else {
        assert(_mode == fslib::WRITE || _mode == fslib::APPEND);
        auto ec = CreateFile(path, false, fileLength, 0, -1);
        RETURN_IF_FS_ERROR(ec, "CrateFile failed");
        _length = _file->tell();
    }

    if (unlikely(!_file)) {
        AUTIL_LOG(ERROR, "Open file: [%s] FAILED", path.c_str());
        return FSEC_ERROR;
    }
    if (!_file->isOpened()) {
        fslib::ErrorCode ec = _file->getLastError();
        AUTIL_LOG(ERROR, "Open file: [%s] FAILED, ec[%d]", path.c_str(), ec);
        return ParseFromFslibEC(ec);
    }
    return FSEC_OK;
}

ErrorCode BufferedFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    const string& packageFileDataPath = packageOpenMeta.GetPhysicalFilePath();
    if (ReadOnly()) {
        _fileBeginOffset = packageOpenMeta.GetOffset();
        _length = packageOpenMeta.GetLength();
        auto ec =
            CreateFile(packageFileDataPath, false, packageOpenMeta.GetPhysicalFileLength(), _fileBeginOffset, _length);
        RETURN_IF_FS_ERROR(ec, "CrateFile failed");
    } else {
        AUTIL_LOG(ERROR, "BufferedFileNode does not support open innerFile for WRITE mode");
        return FSEC_NOTSUP;
    }

    if (unlikely(!_file)) {
        AUTIL_LOG(ERROR, "Open file: [%s] FAILED", packageFileDataPath.c_str());
        return FSEC_ERROR;
    }
    if (!_file->isOpened()) {
        fslib::ErrorCode ec = _file->getLastError();
        RETURN_IF_FS_ERROR(ParseFromFslibEC(ec), "Open file: [%s] FAILED", packageFileDataPath.c_str());
    }
    return FSEC_OK;
}

FSFileType BufferedFileNode::GetType() const noexcept { return FSFT_BUFFERED; }

size_t BufferedFileNode::GetLength() const noexcept { return _length; }

void* BufferedFileNode::GetBaseAddress() const noexcept
{
    AUTIL_LOG(DEBUG, "Not support GetBaseAddress");
    return NULL;
}

FSResult<size_t> BufferedFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_file.get());
    size_t leftToReadLen = length;
    size_t totalReadLen = 0;
    while (leftToReadLen > 0) {
        size_t readLen = leftToReadLen > DEFAULT_READ_WRITE_LENGTH ? DEFAULT_READ_WRITE_LENGTH : leftToReadLen;
        ssize_t actualReadLen = _file->pread((uint8_t*)buffer + totalReadLen, readLen, offset + _fileBeginOffset);
        if (actualReadLen == -1) {
            AUTIL_LOG(ERROR, "Read data from file: [%s] FAILED, fslibec[%d]", _file->getFileName(),
                      _file->getLastError());
            return {FSEC_ERROR, 0};
        } else if (actualReadLen == 0) {
            break;
        }
        leftToReadLen -= actualReadLen;
        totalReadLen += actualReadLen;
        offset += actualReadLen;
    }
    return {FSEC_OK, totalReadLen};
}

ByteSliceList* BufferedFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support Read without buffer");
    return NULL;
}

void BufferedFileNode::CheckError() noexcept {}

FSResult<size_t> BufferedFileNode::Write(const void* buffer, size_t length) noexcept
{
    if (!_flushController) {
        _flushController = MultiPathDataFlushController::GetDataFlushController(_file->getFileName());
    }
    assert(_flushController);
    if (ReadOnly()) {
        AUTIL_LOG(ERROR, "ReadOnly file not support write.");
        return {FSEC_NOTSUP, 0};
    }
    assert(_file.get());
    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;

        auto [ec, len] = _flushController->Write(_file.get(), (uint8_t*)buffer + totalWriteLen, writeLen);
        RETURN2_IF_FS_ERROR(ec, len, "write[%s] failed", DebugString().c_str());
        leftToWriteLen -= writeLen;
        totalWriteLen += writeLen;
    }

    _length += length;
    return {FSEC_OK, length};
}

bool BufferedFileNode::ForceFlush() noexcept { return _file->flush() == fslib::EC_OK; }

FSResult<void> BufferedFileNode::Close() noexcept
{
    if (_file.get() && _file->isOpened()) {
        if (_fileCache) {
            _fileCache->Put(_file.release(), _originalFileName);
            return FSEC_OK;
        }

        fslib::ErrorCode ec = _file->close();
        if (ec != fslib::EC_OK) {
            string fileName = _file->getFileName();
            _file.reset();
            AUTIL_LOG(ERROR, "Close FAILED, file: [%s], fslibec[%d]", fileName.c_str(), ec);
            return FSEC_ERROR;
        }
        _file.reset();
    }
    return FSEC_OK;
}

void BufferedFileNode::IOCtlPrefetch(ssize_t blockSize, size_t beginOffset, ssize_t fileLength) noexcept
{
    if (unlikely(!_file)) {
        return;
    }
    if ((fileLength == 0) || (fileLength > 0 && fileLength <= blockSize)) {
        return;
    }
    int64_t blockCount = fileLength < 0 ? 4 : fileLength / blockSize + 1;
    fslib::ErrorCode ec = _file->ioctl(fslib::IOCtlRequest::SetPrefetchParams, blockCount, (int64_t)blockSize,
                                       (int64_t)beginOffset, (int64_t)fileLength);
    if (ec != fslib::EC_OK && ec != fslib::EC_NOTSUP) {
        AUTIL_LOG(WARN, "file [%s] advice prefetch block size [%ld] error [%d]", _physicalPath.c_str(), blockSize, ec);
        return;
    }
    ec = _file->ioctl(fslib::IOCtlRequest::AdvisePrefetch);
    if (ec == fslib::EC_NOTSUP) {
        AUTIL_LOG(DEBUG, "file [%s] advice prefetch not support", _physicalPath.c_str());
        return;
    } else if (unlikely(ec != fslib::EC_OK)) {
        AUTIL_LOG(WARN, "file [%s] advice prefetch error [%d]", _physicalPath.c_str(), ec);
        return;
    }
    AUTIL_LOG(DEBUG, "file [%s] advice prefetch blockSize[%lu], beginOffset[%lu], fileLength[%ld]",
              _physicalPath.c_str(), blockSize, beginOffset, fileLength);
}

FSResult<void> BufferedFileNode::CreateFile(const string& fileName, bool useDirectIO, ssize_t physicalFileLength,
                                            size_t beginOffset, ssize_t fileLength) noexcept
{
    _originalFileName = fileName;
    if (_inPackage && _mode == fslib::READ && !useDirectIO && _fileCache) {
        auto [ec, file] = _fileCache->Get(fileName, physicalFileLength);
        RETURN_IF_FS_ERROR(ec, "openFile [%s] failed", fileName.c_str());
        _file.reset(file);
        return FSEC_OK;
    }

    _fileCache.reset();
    _file.reset(fslib::fs::FileSystem::openFile(fileName, _mode, useDirectIO, physicalFileLength));
    if (_mode == fslib::READ) {
        IOCtlPrefetch(ReaderOption::DEFAULT_BUFFER_SIZE, beginOffset, fileLength);
    }
    return FSEC_OK;
}
}} // namespace indexlib::file_system
