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
#include "indexlib/file_system/fslib/FslibFileWrapper.h"

#include <assert.h>
#include <cstddef>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "fslib/fs/File.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"

using namespace std;

namespace indexlib { namespace file_system {

AUTIL_LOG_SETUP(indexlib.file_system, FslibFileWrapper);

map<string, uint32_t> FslibFileWrapper::_errorTypeList = map<string, uint32_t>();

FslibFileWrapper::FslibFileWrapper(fslib::fs::File* file, bool needClose) : _file(file), _needClose(needClose) {}

FslibFileWrapper::~FslibFileWrapper()
{
    if (!_needClose) {
        return;
    }

    if (_file) {
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]", _file->getFileName(),
                  util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    }
    DELETE_AND_SET_NULL(_file);
}

FSResult<void> FslibFileWrapper::Seek(int64_t offset, fslib::SeekFlag flag) noexcept
{
    assert(_file);

    fslib::ErrorCode ec = _file->seek(offset, flag);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "seek failed, offset[%lu] file[%s]", offset, _file->getFileName());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibFileWrapper::Tell(int64_t& pos) noexcept
{
    assert(_file);

    int64_t currentPos = _file->tell();
    if (currentPos < 0) {
        auto ec = _file->getLastError();
        AUTIL_LOG(ERROR, "tell failed, ec[%d] file[%s] pos[%ld]", ec, _file->getFileName(), currentPos);
        return FSEC_ERROR;
    }
    pos = currentPos;
    return FSEC_OK;
}

bool FslibFileWrapper::IsEof() noexcept
{
    assert(_file);

    return _file->isEof();
}

const char* FslibFileWrapper::GetFileName() const noexcept
{
    assert(_file);

    return _file->getFileName();
}

FSResult<void> FslibFileWrapper::GetLastError() const noexcept
{
    assert(_file);
    auto ec = _file->getLastError();
    return ParseFromFslibEC(ec);
}

FSResult<void> FslibFileWrapper::Flush() noexcept
{
    if (_file && _file->isOpened()) {
        fslib::ErrorCode ec = _file->flush();
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "flush failed, file[%s]", _file->getFileName());
            return FSEC_ERROR;
        }
        return FSEC_OK;
    }
    return FSEC_ERROR;
}

FSResult<void> FslibFileWrapper::Close() noexcept
{
    if (!_needClose) {
        return FSEC_OK;
    }
    if (_file && _file->isOpened()) {
        fslib::ErrorCode ec = _file->close();
        std::string fileName = _file->getFileName();
        delete _file;
        _file = NULL;
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "close failed. file[%s]", fileName.c_str());
            return FSEC_ERROR;
        }
    }
    return FSEC_OK;
}

FSResult<size_t> FslibFileWrapper::Write(const void* buffer, size_t length) noexcept
{
    size_t realLength = 0;
    auto ec = Write(buffer, length, realLength).Code();
    return {ec, realLength};
}

FSResult<size_t> FslibFileWrapper::PRead(void* buffer, size_t length, off_t offset) noexcept
{
    size_t realLength = 0;
    auto ec = PRead(buffer, length, offset, realLength).Code();
    return {ec, realLength};
}

FSResult<void> FslibFileWrapper::NiceWrite(const void* buffer, size_t length) noexcept
{
    size_t realLength = 0;
    auto ec = Write(buffer, length, realLength).Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "Write file[%s] FAILED, length[%lu]", _file->getFileName(), length);
        return ec;
    }
    if (unlikely(realLength != length)) {
        AUTIL_LOG(ERROR, "Write file[%s] FAILED, realLength [%lu] != length[%lu]", _file->getFileName(), realLength,
                  length);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

void FslibFileWrapper::WriteE(const void* buffer, size_t length) noexcept(false)
{
    size_t realLength = 0;
    auto ec = Write(buffer, length, realLength).Code();
    THROW_IF_FS_ERROR(ec, "Write file[%s] FAILED, length[%lu]", _file->getFileName(), length);
    if (realLength != length) {
        INDEXLIB_FATAL_ERROR(FileIO, "Write file[%s] FAILED, realLength [%lu] != length[%lu]", _file->getFileName(),
                             realLength, length);
    }
}

void FslibFileWrapper::PReadE(void* buffer, size_t length, off_t offset) noexcept(false)
{
    size_t realLength = 0;
    auto ec = PRead(buffer, length, offset, realLength).Code();
    THROW_IF_FS_ERROR(ec, "PRead file[%s] FAILED, length[%lu] offset[%ld]", _file->getFileName(), length, offset);
}

void FslibFileWrapper::CloseE() noexcept(false)
{
    auto ec = Close().Code();
    THROW_IF_FS_ERROR(ec, "Close file[%s] FAILED", _file->getFileName());
}

void FslibFileWrapper::FlushE() noexcept(false)
{
    auto ec = Flush().Code();
    THROW_IF_FS_ERROR(ec, "Flush file[%s] FAILED", _file->getFileName());
}

future_lite::Future<size_t> FslibFileWrapper::PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                         future_lite::Executor* executor) noexcept
{
    size_t realLength = 0;
    auto ec = PRead(buffer, length, offset, realLength);
    if (ec != FSEC_OK) {
        return future_lite::makeReadyFuture<size_t>(
            std::make_exception_ptr(util::FileIOException("PRead failed, ec[" + std::to_string(ec) + "]")));
    }
    return future_lite::makeReadyFuture<size_t>(realLength);
}

future_lite::coro::Lazy<FSResult<size_t>> FslibFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset,
                                                                        int advice, int64_t timeout) noexcept
{
    size_t readLength = 0;
    auto ec = PReadV(iov, iovcnt, offset, readLength).Code();
    co_return FSResult<size_t>(ec, readLength);
}

future_lite::coro::Lazy<FSResult<size_t>> FslibFileWrapper::PReadAsync(void* buffer, size_t length, off_t offset,
                                                                       int advice, int64_t timeout) noexcept
{
    iovec iov;
    iov.iov_base = buffer;
    iov.iov_len = length;
    co_return co_await PReadVAsync(&iov, 1, offset, advice, timeout);
}

future_lite::Future<size_t> FslibFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice,
                                                          future_lite::Executor* executor, int64_t timeout) noexcept
{
    size_t realLength = 0;
    auto ec = PReadV(iov, iovcnt, offset, realLength);
    if (ec != FSEC_OK) {
        return future_lite::makeReadyFuture<size_t>(
            std::make_exception_ptr(util::FileIOException("PReadV failed, ec[" + std::to_string(ec) + "]")));
    }
    return future_lite::makeReadyFuture<size_t>(realLength);
}
}} // namespace indexlib::file_system
