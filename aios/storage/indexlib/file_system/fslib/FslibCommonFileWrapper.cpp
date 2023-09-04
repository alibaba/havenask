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
#include "indexlib/file_system/fslib/FslibCommonFileWrapper.h"

#include <assert.h>
#include <cstddef>
#include <exception>
#include <stdint.h>
#include <sys/uio.h>
#include <utility>
#include <sys/statvfs.h>
#include <linux/limits.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/IOController.h"
#include "future_lite/Common.h"
#include "future_lite/Helper.h"
#include "future_lite/MoveWrapper.h"
#include "future_lite/Promise.h"
#include "future_lite/Try.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/file_system/fslib/FslibOption.h"
#include "indexlib/file_system/fslib/MultiPathDataFlushController.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"

namespace future_lite {
class Executor;
} // namespace future_lite

using namespace std;

using future_lite::Executor;
using future_lite::Future;
using future_lite::MoveWrapper;
using future_lite::Promise;
using future_lite::Try;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FslibCommonFileWrapper);

const size_t FslibCommonFileWrapper::MIN_ALIGNMENT = []() -> size_t {
    struct statvfs stat;
    char cwd[PATH_MAX];
    if (getcwd(cwd, PATH_MAX) == nullptr) {
        return 512;
    }
    if (statvfs(cwd, &stat) < 0) {
        return 512;
    }
    return stat.f_bsize;
}();

FslibCommonFileWrapper::FslibCommonFileWrapper(fslib::fs::File* file, bool useDirectIO, bool needClose)
    : FslibFileWrapper(file, needClose)
    , _useDirectIO(useDirectIO)
{
}

FslibCommonFileWrapper::~FslibCommonFileWrapper()
{
    if (!_needClose) {
        return;
    }
    if (_file) {
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]", _file->getFileName(),
                  util::IoExceptionController::HasFileIOException());
        DELETE_AND_SET_NULL(_file);
    }
}

FSResult<void> FslibCommonFileWrapper::Close() noexcept
{
    if (!_needClose) {
        return FSEC_OK;
    }
    return FslibFileWrapper::Close();
}

FSResult<void> FslibCommonFileWrapper::Read(void* buffer, size_t length, size_t& realLength) noexcept
{
    assert(_file);
    return Read(_file, buffer, length, realLength, _useDirectIO);
}

FSResult<void> FslibCommonFileWrapper::PRead(void* buffer, size_t length, off_t offset, size_t& realLength) noexcept
{
    try {
        realLength = PReadAsync(buffer, length, offset, IO_ADVICE_NORMAL, nullptr).get();
    } catch (...) {
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

Future<size_t> FslibCommonFileWrapper::PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                  Executor* executor) noexcept
{
    return InternalPReadASync(buffer, length, offset, advice, executor);
}

future_lite::coro::Lazy<FSResult<size_t>>
FslibCommonFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice, int64_t timeout) noexcept
{
    auto executor = co_await future_lite::CurrentExecutor();
    if (!executor) {
        ssize_t readResult = _file->preadv(iov, iovcnt, offset);
        FSResult<size_t> ec;
        if (readResult == -1) {
            ec.ec = FSEC_ERROR;
            ec.result = 0;
        } else {
            ec.ec = FSEC_OK;
            ec.result = readResult;
        }
        co_return ec;
    }
    fslib::IOController controller;
    controller.setTimeout(timeout);
    controller.setAdvice(advice);
    controller.setExecutor(executor);
    struct Awaiter {
        const iovec* _iov;
        int _iovcnt;
        off_t _offset;
        fslib::IOController* _controller;
        fslib::fs::File* _file;
        Awaiter(const iovec* iov, int iovcnt, off_t offset, fslib::IOController* controller, fslib::fs::File* file)
            : _iov(iov)
            , _iovcnt(iovcnt)
            , _offset(offset)
            , _controller(controller)
            , _file(file)
        {
        }

        bool await_ready() { return false; }
        void await_suspend(std::coroutine_handle<> continuation) noexcept
        {
            _file->preadv(_controller, _iov, _iovcnt, _offset, [continuation]() mutable { continuation.resume(); });
        }

        void await_resume() noexcept {}
    };
    co_await Awaiter(iov, iovcnt, offset, &controller, _file);
    FSResult<size_t> result;
    if (controller.getErrorCode() == fslib::EC_OK) {
        result.ec = ErrorCode::FSEC_OK;
        result.result = controller.getIoSize();
    } else {
        AUTIL_LOG(ERROR, "pread failed, file[%s] error[%d]", _file->getFileName(), controller.getErrorCode());
        result.ec = ParseFromFslibEC(controller.getErrorCode());
        result.result = 0;
    }
    co_return result;
}

FSResult<void> FslibCommonFileWrapper::PReadV(const iovec* iov, int iovcnt, off_t offset, size_t& realLength) noexcept
{
    assert(_file);
    size_t totalReadLen = 0;
    for (int i = 0; i < iovcnt; ++i) {
        totalReadLen += iov[i].iov_len;
    }
    if (totalReadLen > PANGU_MAX_READ_BYTES) {
        AUTIL_LOG(ERROR,
                  "PReadV data from file: [%s] FAILED, "
                  "total size [%ld] exceeds PANGU_MAX_READ_BYTES[%ld] errno[%d]",
                  _file->getFileName(), totalReadLen, PANGU_MAX_READ_BYTES, _file->getLastError());
        return FSEC_ERROR;
    }
    ssize_t readLen = _file->preadv(iov, iovcnt, offset);
    if (readLen == -1) {
        AUTIL_LOG(ERROR, "preadv failed, file[%s] errno[%d]", _file->getFileName(), _file->getLastError());
        return FSEC_ERROR;
    }
    realLength = readLen;
    return FSEC_OK;
}

Future<size_t> FslibCommonFileWrapper::PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice,
                                                   Executor* executor, int64_t timeout) noexcept
{
    assert(_file);
    size_t totalReadLen = 0;
    for (int i = 0; i < iovcnt; ++i) {
        totalReadLen += iov[i].iov_len;
    }
    if (totalReadLen > PANGU_MAX_READ_BYTES) {
        AUTIL_LOG(ERROR,
                  "PReadV data from file: [%s] FAILED, "
                  "total size [%ld] exceeds PANGU_MAX_READ_BYTES[%ld] errno[%d]",
                  _file->getFileName(), totalReadLen, PANGU_MAX_READ_BYTES, _file->getLastError());
        return future_lite::makeReadyFuture<size_t>(
            std::make_exception_ptr(util::OutOfRangeException("total size exceeds PANGU_MAX_READ_BYTES")));
    }
    Promise<size_t> promise;
    auto future = promise.getFuture();
    future.setExecutor(executor);
    MoveWrapper<decltype(promise)> p(std::move(promise));

    auto controller = new fslib::IOController;
    controller->setTimeout(timeout);
    controller->setAdvice(advice);
    controller->setExecutor(executor);

    future.checkout();
    if (executor) {
        future.setForceSched(true);
    }
    _file->preadv(controller, iov, iovcnt, offset, [p, controller, this]() mutable {
        if (controller->getErrorCode() == fslib::EC_OK) {
            p.get().setValue(controller->getIoSize());
        } else if (controller->getErrorCode() == fslib::EC_OPERATIONTIMEOUT) {
            try {
                INDEXLIB_FATAL_ERROR(Timeout, "read file[%s] timeout", _file->getFileName());
            } catch (...) {
                p.get().setException(std::current_exception());
            }
        } else {
            try {
                INDEXLIB_FATAL_ERROR(FileIO, "preadv file[%s] failed, fslibec[%d]", _file->getFileName(),
                                     controller->getErrorCode());
            } catch (...) {
                p.get().setException(std::current_exception());
            }
        }
        delete controller;
    });
    return future;
}

FSResult<void> FslibCommonFileWrapper::Write(const void* buffer, size_t length, size_t& realLength) noexcept
{
    assert(_file);
    return Write(_file, buffer, length, realLength);
}

FSResult<void> FslibCommonFileWrapper::Read(fslib::fs::File* file, void* buffer, size_t length, size_t& realLength,
                                            bool useDirectIO) noexcept
{
    size_t leftToReadLen = length;
    size_t totalReadLen = 0;
    while (leftToReadLen > 0) {
        size_t readLen = leftToReadLen > DEFAULT_READ_WRITE_LENGTH ? DEFAULT_READ_WRITE_LENGTH : leftToReadLen;
        ssize_t actualReadLen = file->read((uint8_t*)buffer + totalReadLen, readLen);
        if (actualReadLen == -1) {
            AUTIL_LOG(ERROR, "Read data from file: [%s] FAILED", file->getFileName());
            return FSEC_ERROR;
        } else if (actualReadLen == 0) {
            break;
        }
        leftToReadLen -= actualReadLen;
        totalReadLen += actualReadLen;
        if (useDirectIO && (actualReadLen % MIN_ALIGNMENT) != 0) {
            break;
        }
    }
    realLength = totalReadLen;
    return FSEC_OK;
}

FSResult<void> FslibCommonFileWrapper::Write(fslib::fs::File* file, const void* buffer, size_t length,
                                             size_t& realLength) noexcept
{
    DataFlushController* flushController = MultiPathDataFlushController::GetDataFlushController(file->getFileName());
    assert(flushController);

    size_t leftToWriteLen = length;
    size_t totalWriteLen = 0;
    while (leftToWriteLen > 0) {
        size_t writeLen = leftToWriteLen > DEFAULT_READ_WRITE_LENGTH ? DEFAULT_READ_WRITE_LENGTH : leftToWriteLen;
        auto [ec, retLength] = flushController->Write(file, (uint8_t*)buffer + totalWriteLen, writeLen);
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "data flush write failed, errno[%d] file[%s]", file->getLastError(), file->getFileName());
            return ec;
        }
        leftToWriteLen -= writeLen;
        totalWriteLen += writeLen;
    }
    realLength = totalWriteLen;
    return FSEC_OK;
}

Future<size_t> FslibCommonFileWrapper::InternalPReadASync(void* buffer, size_t length, off_t offset, int advice,
                                                          Executor* executor) noexcept
{
    if (length == 0) {
        return future_lite::makeReadyFuture((size_t)0);
    }
    size_t readLen = length > DEFAULT_READ_WRITE_LENGTH ? DEFAULT_READ_WRITE_LENGTH : length;
    auto future = SinglePreadAsync(buffer, readLen, offset, advice, executor)
                      .thenValue([buffer, length, offset, advice, executor, this](size_t result) mutable {
                          if (result > 0) {
                              if (_useDirectIO && (result % MIN_ALIGNMENT) != 0) {
                                  return future_lite::makeReadyFuture<size_t>(result);
                              }
                              return InternalPReadASync((char*)buffer + result, length - result, offset + result,
                                                        advice, executor)
                                  .thenValue([result](size_t value) { return result + value; });
                          } else {
                              return future_lite::makeReadyFuture((size_t)0);
                          }
                      });
    return future;
}

Future<size_t> FslibCommonFileWrapper::SinglePreadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                        Executor* executor) noexcept
{
    Promise<size_t> promise;
    auto future = promise.getFuture();
    future.setExecutor(executor);
    MoveWrapper<decltype(promise)> p(std::move(promise));
    fslib::IOController* controller = new fslib::IOController();
    controller->setAdvice(advice);
    controller->setExecutor(executor);

    future.checkout();
    if (executor) {
        future.setForceSched(true);
    }

    _file->pread(controller, buffer, length, offset, [p, controller, this]() mutable {
        if (controller->getErrorCode() == fslib::EC_OK) {
            p.get().setValue(controller->getIoSize());
        } else {
            try {
                INDEXLIB_FATAL_ERROR(FileIO, "pread file[%s] failed, fslibec[%d]", _file->getFileName(),
                                     controller->getErrorCode());
            } catch (...) {
                p.get().setException(std::current_exception());
            }
        }
        delete controller;
    });
    return future;
}
}} // namespace indexlib::file_system
