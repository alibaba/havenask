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

#include <cstdint>
#include <exception>
#include <map>
#include <stddef.h>
#include <string>
#include <sys/types.h>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "future_lite/Future.h"
#include "future_lite/Helper.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"

namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs
namespace future_lite {
class Executor;
} // namespace future_lite
struct iovec;

namespace indexlib { namespace file_system {

class FslibFileWrapper
{
public:
    FslibFileWrapper(const FslibFileWrapper&) = delete;
    FslibFileWrapper(fslib::fs::File* file, bool needClose = true);
    virtual ~FslibFileWrapper();

public:
    virtual FSResult<void> Read(void* buffer, size_t length, size_t& realLength) noexcept = 0;
    virtual FSResult<void> PRead(void* buffer, size_t length, off_t offset, size_t& realLength) noexcept = 0;
    virtual FSResult<void> PReadV(const iovec* iov, int iovcnt, off_t offset, size_t& realLength) noexcept = 0;
    virtual FSResult<void> Write(const void* buffer, size_t length, size_t& realLength) noexcept = 0;
    virtual FSResult<void> Seek(int64_t offset, fslib::SeekFlag flag) noexcept;
    virtual FSResult<void> Tell(int64_t& pos) noexcept;
    virtual FSResult<void> Flush() noexcept;
    virtual FSResult<void> Close() noexcept;

public:
    virtual future_lite::Future<size_t> PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                   future_lite::Executor* executor) noexcept;
    virtual future_lite::Future<size_t> PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice,
                                                    future_lite::Executor* executor, int64_t timeout) noexcept;

    /*will call PReadVAsync on default*/
    virtual future_lite::coro::Lazy<FSResult<size_t>> PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                                 int64_t timeout) noexcept;
    virtual future_lite::coro::Lazy<FSResult<size_t>> PReadVAsync(const iovec* iov, int iovcnt, off_t offset,
                                                                  int advice, int64_t timeout) noexcept;

public:
    bool IsEof() noexcept;
    const char* GetFileName() const noexcept;
    FSResult<void> GetLastError() const noexcept;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept;
    FSResult<size_t> PRead(void* buffer, size_t length, off_t offset) noexcept;
    FSResult<void> NiceWrite(const void* buffer, size_t length) noexcept;

public:
    // exception when error, todo rm
    void WriteE(const void* buffer, size_t length) noexcept(false);
    void PReadE(void* buffer, size_t length, off_t offset) noexcept(false);
    void FlushE() noexcept(false);
    void CloseE() noexcept(false);

    // TODO: RM BEGIN
public:
    // for test
    static const uint32_t ERROR_NONE = 0x00;
    static const uint32_t READ_ERROR = 0x01;
    static const uint32_t WRITE_ERROR = 0x02;
    static const uint32_t TELL_ERROR = 0x04;
    static const uint32_t SEEK_ERROR = 0x08;
    static const uint32_t CLOSE_ERROR = 0x10;
    static const uint32_t FLUSH_ERROR = 0x20;
    static const uint32_t COMMON_FILE_WRITE_ERROR = 0x40;
    static std::map<std::string, uint32_t> _errorTypeList;
    static void SetError(uint32_t errorType, const std::string& errorFile) {};
    static void CleanError() { _errorTypeList.clear(); }
    // TODO: RM END

protected:
    static const uint32_t DEFAULT_BUF_SIZE = 1024;
    static const uint32_t DEFAULT_READ_WRITE_LENGTH = 64 * 1024 * 1024;

    fslib::fs::File* _file;
    bool _needClose;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FslibFileWrapper> FslibFileWrapperPtr;

//////////////////////////////////////////////////////////

}} // namespace indexlib::file_system
