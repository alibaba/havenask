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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/admin/WorkerManager.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "worker_framework/LeaderChecker.h"

namespace swift {
namespace admin {

class WorkerManagerLocal : public WorkerManager {
public:
    typedef worker_framework::LeaderChecker LeaderChecker;

public:
    WorkerManagerLocal(const std::string &workDir, const std::string &logConfFile, int32_t maxRestartCount = -1);
    ~WorkerManagerLocal();

private:
    WorkerManagerLocal(const WorkerManagerLocal &);
    WorkerManagerLocal &operator=(const WorkerManagerLocal &);

public:
    bool init(const std::string &hippoRoot, const std::string &applicationId, LeaderChecker *leaderChecker) override;
    void getReadyRoleCount(const protocol::RoleType roleType,
                           const std::string &configPath,
                           const std::string &roleVersion,
                           uint32_t &total,
                           uint32_t &ready) override;
    void releaseSlotsPref(const std::vector<std::string> &roleNames) override;
    void setWorkDirAndLogFile(const std::string &workDir, const std::string &logConfFile);

private:
    std::string _workDir;
    std::string _logConfFile;
    int32_t _maxRestartCount = -1;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(WorkerManagerLocal);

} // namespace admin
} // namespace swift
