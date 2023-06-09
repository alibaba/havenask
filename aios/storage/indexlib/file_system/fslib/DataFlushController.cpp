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
#include "indexlib/file_system/fslib/DataFlushController.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <limits>
#include <memory>
#include <unistd.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DataFlushController);

DataFlushController::DataFlushController()
    : _baseLineTs(0)
    , _remainQuota(std::numeric_limits<uint32_t>::max())
    , _quotaSize(std::numeric_limits<uint32_t>::max())
    , _flushUnit(DEFAULT_FLUSH_UNIT_SIZE)
    , _interval(1)
    , _forceFlush(false)
    , _inited(false)
    , _totalWaitCount(0)
{
    Reset();
}

DataFlushController::~DataFlushController() {}

// quota_size=10485760,quota_interval=1000,flush_unit=1048576,force_flush=true
void DataFlushController::InitFromString(const string& paramStr)
{
    ScopedLock lock(_lock);
    if (_inited) {
        AUTIL_LOG(INFO, "DataFlushController has already been inited!");
        return;
    }

    AUTIL_LOG(INFO, "Init DataFlushController with param [%s]!", paramStr.c_str());
    vector<vector<string>> patternStrVec;
    StringUtil::fromString(paramStr, patternStrVec, "=", ",");
    for (size_t j = 0; j < patternStrVec.size(); j++) {
        if (patternStrVec[j].size() != 2) {
            AUTIL_LOG(ERROR,
                      "INDEXLIB_DATA_FLUSH_PARAM [%s] has format error:"
                      "inner should be kv pair format!",
                      paramStr.c_str());
            return;
        }

        if (patternStrVec[j][0] == "quota_size") // Byte
        {
            uint32_t quotaSize;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaSize) && quotaSize > 0) {
                _quotaSize = quotaSize;
                continue;
            }
            AUTIL_LOG(ERROR, "quota_size [%s] must be valid number!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "flush_unit") // Byte
        {
            uint32_t flushUnit;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), flushUnit) && flushUnit > 0) {
                _flushUnit = flushUnit;
                continue;
            }
            AUTIL_LOG(ERROR, "flush_unit [%s] must be valid number!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "quota_interval") // ms
        {
            uint32_t quotaInterval;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaInterval)) {
                _interval = quotaInterval * 1000;
                continue;
            }
            AUTIL_LOG(ERROR, "quota_interval [%s] must be valid [ms]!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "force_flush") {
            _forceFlush = (patternStrVec[j][1] == "true" ? true : false);
            continue;
        }

        if (patternStrVec[j][0] == "root_path") {
            _pathPrefix = util::PathUtil::NormalizePath(patternStrVec[j][1]);
            continue;
        }
        AUTIL_LOG(ERROR, "unknown key [%s] in INDEXLIB_DATA_FLUSH_PARAM [%s]!", patternStrVec[j][0].c_str(),
                  paramStr.c_str());
        return;
    }

    if (_flushUnit > _quotaSize) {
        AUTIL_LOG(WARN, "flush_unit[%u] should be less than quota_size[%u], use quota_size instead!", _flushUnit,
                  _quotaSize);
        _flushUnit = _quotaSize;
    }
    _inited = true;
}

bool DataFlushController::MatchPathPrefix(const std::string& path) const noexcept
{
    if (_pathPrefix.empty()) {
        return true;
    }
    string noramlizedPath = util::PathUtil::NormalizePath(path);
    return noramlizedPath.find(_pathPrefix) == 0;
}

int64_t DataFlushController::ReserveQuota(int64_t quota) noexcept
{
    assert(quota > 0);
    if (!_inited) {
        return quota;
    }

    ScopedLock lock(_lock);
    int64_t currentTs = TimeUtility::currentTime();
    if (currentTs >= _baseLineTs + _interval) {
        // new flush window
        _baseLineTs = currentTs;
        _remainQuota = _quotaSize;
    }

    if (_remainQuota <= 0) {
        // no quota in current flush window, wait to next
        int64_t sleepTs = _baseLineTs + _interval - currentTs;
        usleep(sleepTs);
        currentTs = TimeUtility::currentTime();
        _baseLineTs = currentTs;
        _remainQuota = _quotaSize;

        ++_totalWaitCount;
        if (_totalWaitCount % 10 == 0) {
            AUTIL_LOG(INFO, "wait [%lu] times for no flush quota left!", _totalWaitCount);
        }
    }

    int64_t availableQuota = min(_remainQuota, quota);
    availableQuota = min((int64_t)_flushUnit, availableQuota);
    _remainQuota -= availableQuota;
    return availableQuota;
}

void DataFlushController::Reset() noexcept
{
    _baseLineTs = 0;
    _remainQuota = std::numeric_limits<uint32_t>::max();
    _quotaSize = std::numeric_limits<uint32_t>::max();
    _flushUnit = DEFAULT_FLUSH_UNIT_SIZE;
    _interval = 1;
    _forceFlush = false;
    _inited = false;
    _totalWaitCount = 0;
}

FSResult<size_t> DataFlushController::Write(fslib::fs::File* file, const void* buffer, size_t length) noexcept
{
    if (unlikely(fslib::fs::FileSystem::GENERATE_ERROR("write", file->getFileName()) != fslib::EC_OK)) {
        return {FSEC_ERROR, 0};
    }

    assert(file);
    size_t writeLen = 0;
    int64_t needQuota = length;
    while (needQuota > 0) {
        int64_t approvedQuota = ReserveQuota(needQuota);
        assert(approvedQuota > 0);

        ssize_t actualWriteLen = file->write((uint8_t*)buffer + writeLen, (size_t)approvedQuota);
        if (actualWriteLen <= 0) {
            AUTIL_LOG(ERROR, "write data failed, file[%s] errno[%d]", file->getFileName(), file->getLastError());
            return {FSEC_ERROR, writeLen};
        }
        writeLen += (size_t)actualWriteLen;
        needQuota -= actualWriteLen;
        // notice: current repeatedly write small piece data will not trigger flush operation
        if (_forceFlush && approvedQuota == _flushUnit) {
            auto ret = file->flush();
            if (ret != fslib::EC_OK) {
                AUTIL_LOG(ERROR, "flush data failed, file[%s] errno[%d]", file->getFileName(), file->getLastError());
                return {FSEC_ERROR, writeLen};
            }
        }
    }
    return {FSEC_OK, writeLen};
}

void DataFlushController::WriteE(fslib::fs::File* file, const void* buffer, size_t length) noexcept(false)
{
    ErrorCode ec = Write(file, buffer, length).Code();
    if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "write to file [%s] failed, length[%lu]", file->getFileName(), length);
    }
}
}} // namespace indexlib::file_system
