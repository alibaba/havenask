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
#include <sys/types.h>

#include "autil/Log.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"

namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs
namespace future_lite {
class Executor;
} // namespace future_lite
namespace indexlib { namespace file_system {
class DataFlushController;
}} // namespace indexlib::file_system
struct iovec;

namespace indexlib { namespace file_system {

// deprecated
class FslibCommonFileWrapper : public FslibFileWrapper
{
public:
    FslibCommonFileWrapper(fslib::fs::File* file, bool useDirectIO = false, bool needClose = true);
    ~FslibCommonFileWrapper();

public:
    FSResult<void> Read(void* buffer, size_t length, size_t& realLength) noexcept override;
    FSResult<void> PRead(void* buffer, size_t length, off_t offset, size_t& realLength) noexcept override;
    FSResult<void> PReadV(const iovec* iov, int iovcnt, off_t offset, size_t& realLength) noexcept override;
    FSResult<void> Write(const void* buffer, size_t length, size_t& realLength) noexcept override;
    FSResult<void> Close() noexcept override;

public:
    future_lite::Future<FSResult<size_t>> PReadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                     future_lite::Executor* executor) noexcept override;
    future_lite::Future<FSResult<size_t>> PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice,
                                                      future_lite::Executor* executor,
                                                      int64_t timeout) noexcept override;
    future_lite::coro::Lazy<FSResult<size_t>> PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice,
                                                          int64_t timeout) noexcept override;

private:
    static FSResult<void> Read(fslib::fs::File* file, void* buffer, size_t length, size_t& realLength,
                               bool useDirectIO) noexcept;
    static FSResult<void> Write(fslib::fs::File* file, const void* buffer, size_t length, size_t& realLength) noexcept;
    future_lite::Future<FSResult<size_t>> InternalPReadASync(void* buffer, size_t length, off_t offset, int advice,
                                                             future_lite::Executor* executor) noexcept;
    future_lite::Future<FSResult<size_t>> SinglePreadAsync(void* buffer, size_t length, off_t offset, int advice,
                                                           future_lite::Executor* executor) noexcept;

private:
    static const size_t MIN_ALIGNMENT;
    static constexpr size_t PANGU_MAX_READ_BYTES = 2 * 1024 * 1024;

    bool _useDirectIO;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FslibCommonFileWrapper> FslibCommonFileWrapperPtr;
}} // namespace indexlib::file_system
