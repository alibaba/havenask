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
#include "suez/common/DiskQuotaController.h"

#include <assert.h>
#include <cstdint>
#include <stdlib.h>
// #include "suez/common/MetricUtil.h"
#include <iostream>
#include <limits>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "suez/common/DiskUtil.h"
#include "suez/common/ServerMetrics.h"
#include "suez/sdk/PathDefine.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace kmonitor;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DiskQuotaController);

namespace suez {

const string DiskQuotaController::TMP_FILE_NAME_SURFIX = "._deploy_tmp_.";
const string DiskQuotaController::WORKDIR_RESERVE_DISKSIZE = "WORKDIR_RESERVE_DISKSIZE";

struct DiskQuotaStatusValue {
    double totalUsed;
    double totalQuota;
    double quotaUsed;
};

class DiskQuotaStatusMetrics : public MetricsGroup {
public:
    bool init(MetricsGroupManager *manager) override {
        REGISTER_STATUS_MUTABLE_METRIC(_diskUse, kDiskUse);
        REGISTER_STATUS_MUTABLE_METRIC(_diskUseRatio, kDiskUseRatio);
        REGISTER_STATUS_MUTABLE_METRIC(_diskQuotaUse, kDiskQuotaUse);
        REGISTER_STATUS_MUTABLE_METRIC(_diskQuotoUseRatio, kDiskQuotoUseRatio);
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, const DiskQuotaStatusValue *value) {
        assert(value != nullptr);
        REPORT_MUTABLE_METRIC(_diskUse, value->totalUsed);
        REPORT_MUTABLE_METRIC(_diskUseRatio, value->totalUsed * 100.0 / value->totalQuota);
        REPORT_MUTABLE_METRIC(_diskQuotaUse, value->quotaUsed);
        REPORT_MUTABLE_METRIC(_diskQuotoUseRatio, value->quotaUsed * 100.0 / value->totalQuota);
    }

private:
    MutableMetric *_diskUse = nullptr;
    MutableMetric *_diskUseRatio = nullptr;
    MutableMetric *_diskQuotaUse = nullptr;
    MutableMetric *_diskQuotoUseRatio = nullptr;
};

DiskQuotaController::DiskQuotaController() : _totalQuota(0), _workerReserveQuota(0) {
    setQuota(numeric_limits<int64_t>::max());
}

DiskQuotaController::~DiskQuotaController() {}

DiskQuotaStatus DiskQuotaController::reserveWithRetry(const string &remotePath,
                                                      const string &localPath,
                                                      const vector<string> &files,
                                                      int64_t size) {
    const int retryTime = 3;
    DiskQuotaStatus status = DQS_FAILED;
    for (int i = 0; i < retryTime; i++) {
        status = reserve(remotePath, localPath, files, size);
        if (DQS_OK == status || DQS_NOTENOUGH == status) {
            break;
        }
    }
    return status;
}

DiskQuotaStatus DiskQuotaController::reserve(const string &remotePath,
                                             const string &localPath,
                                             const vector<string> &files,
                                             int64_t size) {
    vector<string> localFiles;
    for (const auto &file : files) {
        localFiles.push_back(FileSystem::joinFilePath(localPath, file));
    }

    return reserve(DiskQuotaReserve(makeKey(remotePath, localPath, files), localFiles, size));
}

DiskQuotaStatus DiskQuotaController::reserve(const DiskQuotaReserve &diskQuotaReserve) {
    autil::ScopedLock lock(_mutex);
    AUTIL_LOG(INFO, "reserve %s begin", diskQuotaReserve.brief().c_str());
    auto key = diskQuotaReserve.key;
    if (_reserveMap.find(key) != _reserveMap.end()) {
        AUTIL_LOG(WARN, "%s already reserved", diskQuotaReserve.brief().c_str());
        return DQS_FAILED;
    }

    _reserveMap[key] = diskQuotaReserve;
    for (const auto &file : diskQuotaReserve.files) {
        _reserveFiles.insert(file);
        _reserveFiles.insert(file + TMP_FILE_NAME_SURFIX);
    }
    ScopeGuard releaseGuard([this, key]() { releaseInternal(key); });
    int64_t quotaLeft = 0;
    if (!getQuotaLeft(quotaLeft)) {
        return DQS_FAILED;
    }
    if (quotaLeft < 0) {
        AUTIL_LOG(WARN,
                  "try to reserve %s failed, current quota left %ld M",
                  diskQuotaReserve.brief().c_str(),
                  b2mb(quotaLeft));
        return DQS_NOTENOUGH;
    }
    releaseGuard.release();
    AUTIL_LOG(INFO, "reserve %s end", diskQuotaReserve.brief().c_str());
    return DQS_OK;
}

void DiskQuotaController::release(int64_t key) {
    autil::ScopedLock lock(_mutex);
    releaseInternal(key);
}

void DiskQuotaController::releaseInternal(int64_t key) {
    auto it = _reserveMap.find(key);
    if (_reserveMap.end() == it) {
        return;
    }
    for (const auto &file : it->second.files) {
        _reserveFiles.erase(file);
        _reserveFiles.erase(file + TMP_FILE_NAME_SURFIX);
    }
    AUTIL_LOG(INFO, "release %s done", it->second.brief().c_str());
    _reserveMap.erase(it);
}

void DiskQuotaController::setQuotaMb(int64_t quota) { setQuota(mb2b(quota)); }

void DiskQuotaController::setQuota(int64_t quota) {
    if (quota < 0) {
        AUTIL_LOG(ERROR, "disk quota can not less than zero:%ld", quota);
        return;
    }
    autil::ScopedLock lock(_mutex);
    if (_totalQuota == quota) {
        return;
    }

    string reserveSizeStr = autil::EnvUtil::getEnv(WORKDIR_RESERVE_DISKSIZE);
    if (!reserveSizeStr.empty()) {
        int64_t workerReserveQuota = 0;
        StringUtil::fromString(reserveSizeStr, workerReserveQuota);
        workerReserveQuota = mb2b(workerReserveQuota);
        if (workerReserveQuota < 0) {
            AUTIL_LOG(ERROR, "worker reserve quota can not less than zero:%ld", workerReserveQuota);
            return;
        }
        _workerReserveQuota = workerReserveQuota;
    } else {
        _workerReserveQuota = quota * 0.1;
    }
    AUTIL_LOG(INFO,
              "total disk quota: %ld M, %s: %ld M",
              b2mb(quota),
              WORKDIR_RESERVE_DISKSIZE.c_str(),
              b2mb(_workerReserveQuota));
    _totalQuota = quota;
}

void DiskQuotaController::setMetricsReporter(MetricsReporterPtr &metricsReporter) {
    _metricsReporter = metricsReporter;
}

int64_t
DiskQuotaController::makeKey(const string &remotePath, const string &localPath, const std::vector<std::string> &files) {
    stringstream ss;
    ss << remotePath << localPath << autil::StringUtil::toString(files);
    return HashAlgorithm::hashString64(ss.str().c_str());
}

bool DiskQuotaController::getWorkDirDiskTotalUse(const string &path, int64_t &size) const {
    int64_t usedDiskSize = 0;
    int64_t totalDiskSize = 0;

    bool ret = DiskUtil::GetDiskInfo(usedDiskSize, totalDiskSize);
    AUTIL_LOG(DEBUG,
              "get disk info ret[%d], usedDiskSize[%lu], totalDiskSize[%lu], _totalQuota[%lu]",
              ret,
              usedDiskSize,
              totalDiskSize,
              _totalQuota);
    // get disk info success and disk total size is right
    // disk total size is wrong when disk quota is disabled
    if (ret && totalDiskSize == _totalQuota) {
        size = usedDiskSize;
        return true;
    }

    if (!DiskUtil::GetDirectorySize(path.c_str(), usedDiskSize)) {
        AUTIL_LOG(WARN, "GetDirectorySize failed");
        return false;
    }
    AUTIL_LOG(DEBUG, "call GetDirectorySize, get usedDiskSize[%lu]", usedDiskSize);
    size = usedDiskSize;
    return true;
}

bool DiskQuotaController::getQuotaLeft(int64_t &quotaLeft) const {
    const string currentPath = PathDefine::getCurrentPath();
    if (currentPath.empty()) {
        AUTIL_LOG(ERROR, "get current path failed");
        return false;
    }

    int64_t totalUsed = 0, quotaUsed = 0;

    if (!getWorkDirDiskTotalUse(currentPath, totalUsed)) {
        AUTIL_LOG(WARN, "get work dir disk total use failed, path [%s]", currentPath.c_str());
        return false;
    }
    int64_t localFileSize = 0;
    for (auto &file : _reserveFiles) {
        if (!FileSystem::isExist(file)) {
            continue;
        }
        FileMeta localFileMeta;
        if (fslib::EC_OK != FileSystem::getFileMeta(file, localFileMeta)) {
            AUTIL_LOG(DEBUG, "get file meta %s failed", file.c_str());
            continue;
        }
        localFileSize += localFileMeta.fileLength;
    }
    quotaUsed = totalUsed - localFileSize;

    int64_t quotaReserved = 0;
    for (const auto &item : _reserveMap) {
        quotaReserved += item.second.quota;
    }
    quotaUsed += quotaReserved;
    quotaUsed += _workerReserveQuota;
    quotaLeft = _totalQuota - quotaUsed;
    AUTIL_LOG(INFO,
              "totalUsed = %ld M "
              "quotaReserved = %ld M quotaUsed = %ld M quotaLeft = %ld M",
              b2mb(totalUsed),
              b2mb(quotaReserved),
              b2mb(quotaUsed),
              b2mb(quotaLeft));

    DiskQuotaStatusValue value;
    value.totalUsed = totalUsed;
    value.quotaUsed = quotaUsed;
    value.totalQuota = _totalQuota;
    if (_metricsReporter != nullptr) {
        _metricsReporter->report<DiskQuotaStatusMetrics>(nullptr, &value);
    }
    return true;
}

} // namespace suez
