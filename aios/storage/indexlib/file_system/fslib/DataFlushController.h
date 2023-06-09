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
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"

namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs

namespace indexlib { namespace file_system {

class DataFlushController
{
public:
    DataFlushController();
    ~DataFlushController();

public:
    // ** notice: force_flush=true, not fully function, may not flush in time,
    //            suggest false when write in piece
    // format : quota_size=10485760,quota_interval=1000,flush_unit=1048576,force_flush=true
    void InitFromString(const std::string& paramStr);
    bool MatchPathPrefix(const std::string& path) const noexcept;

public:
    FSResult<size_t> Write(fslib::fs::File* file, const void* buffer, size_t length) noexcept;
    void Reset() noexcept;

public:
    // exception when error, todo rm
    void WriteE(fslib::fs::File* file, const void* buffer, size_t length) noexcept(false);

private:
    int64_t ReserveQuota(int64_t quota) noexcept;

private:
    int64_t _baseLineTs;
    int64_t _remainQuota;
    uint32_t _quotaSize;
    uint32_t _flushUnit;
    uint32_t _interval;
    bool _forceFlush;
    std::string _pathPrefix;
    bool _inited;

    size_t _totalWaitCount;
    autil::ThreadMutex _lock;

private:
    static const uint32_t DEFAULT_FLUSH_UNIT_SIZE = 2 * 1024 * 1024; // 2 MB

private:
    friend class DataFlushControllerTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DataFlushController> DataFlushControllerPtr;
}} // namespace indexlib::file_system
