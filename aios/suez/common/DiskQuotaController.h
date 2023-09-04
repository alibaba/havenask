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

#include <algorithm>
#include <map>
#include <ostream>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Scope.h"
#include "kmonitor/client/MetricsReporter.h"

namespace suez {

inline int64_t b2mb(int64_t size) { return size / 1024 / 1024; }

inline int64_t mb2b(int64_t size) { return size * 1024 * 1024; }

enum DiskQuotaStatus {
    DQS_OK,
    DQS_FAILED,
    DQS_NOTENOUGH,
};

struct DiskQuotaReserve {
public:
    DiskQuotaReserve() {}
    DiskQuotaReserve(int64_t key, const std::vector<std::string> &files, int64_t quota) {
        this->key = key;
        this->files = files;
        this->quota = quota;
    }
    std::string brief() const {
        std::stringstream ss;
        ss << " key: " << key;
        ss << " num of files: " << files.size();
        ss << " some of files: ";
        for (int i = 0; i < std::min(int(files.size()), 3); i++) {
            ss << files[i] << ' ';
        }
        ss << " quota: " << b2mb(quota) << " M ";
        return ss.str();
    }
    int64_t key;
    std::vector<std::string> files;
    int64_t quota;
};

class DiskQuotaController {
public:
    DiskQuotaController();
    ~DiskQuotaController();

private:
    DiskQuotaController(const DiskQuotaController &);
    DiskQuotaController &operator=(const DiskQuotaController &);

public:
    DiskQuotaStatus reserveWithRetry(const std::string &remotePath,
                                     const std::string &localPath,
                                     const std::vector<std::string> &files,
                                     int64_t size);
    void release(int64_t key);
    void setQuotaMb(int64_t quota);
    void setQuota(int64_t quota);
    void setMetricsReporter(kmonitor::MetricsReporterPtr &metricsReporter);
    static int64_t
    makeKey(const std::string &remotePath, const std::string &localPath, const std::vector<std::string> &files);

private:
    DiskQuotaStatus reserve(const std::string &remotePath,
                            const std::string &localPath,
                            const std::vector<std::string> &files,
                            int64_t size);
    DiskQuotaStatus reserve(const DiskQuotaReserve &diskQuota);
    void releaseInternal(int64_t key);
    bool getWorkDirDiskTotalUse(const std::string &path, int64_t &size) const;
    bool getQuotaLeft(int64_t &quotaLeft) const;

private:
    std::map<int64_t, DiskQuotaReserve> _reserveMap;
    std::set<std::string> _reserveFiles;
    int64_t _totalQuota;
    int64_t _workerReserveQuota;
    mutable autil::ThreadMutex _mutex;
    kmonitor::MetricsReporterPtr _metricsReporter;

private:
    const static std::string TMP_FILE_NAME_SURFIX;
    const static std::string WORKDIR_RESERVE_DISKSIZE;
};

class DiskQuotaReleaseHelper {
public:
    DiskQuotaReleaseHelper(DiskQuotaController *controller,
                           const std::string &remote,
                           const std::string &local,
                           const std::vector<std::string> &files)
        : _guard([=]() {
            if (controller) {
                controller->release(DiskQuotaController::makeKey(remote, local, files));
            }
        }) {}

private:
    autil::ScopeGuard _guard;
};

} // namespace suez
