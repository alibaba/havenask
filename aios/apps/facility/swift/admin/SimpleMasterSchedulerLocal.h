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
#include <set>
#include <stdint.h>
#include <string>
#include <sys/types.h>
#include <vector>

#include "autil/Log.h"
#include "master_framework/RolePlan.h"
#include "master_framework/SimpleMasterScheduler.h"
#include "worker_framework/LeaderChecker.h"

namespace hippo {
struct ReleasePreference;
struct SlotId;
} // namespace hippo
namespace master_framework {
namespace simple_master {
class AppPlan;
} // namespace simple_master
} // namespace master_framework

struct ProcessLauncherParam {
    std::string binPath;
    std::vector<std::string> args;
    std::map<std::string, std::string> envs;
    std::string workDir;
};

using ProcessLaucherParamMap = std::map<std::string, ProcessLauncherParam>;

class SimpleMasterSchedulerLocal : public master_framework::simple_master::SimpleMasterScheduler {
public:
    SimpleMasterSchedulerLocal(const std::string &workDir,
                               const std::string &logConfFile,
                               int32_t maxRestartCount = -1);
    virtual ~SimpleMasterSchedulerLocal();

private:
    SimpleMasterSchedulerLocal(const SimpleMasterSchedulerLocal &);
    SimpleMasterSchedulerLocal &operator=(const SimpleMasterSchedulerLocal &);

public:
    bool init(const std::string &hippoZkRoot,
              worker_framework::LeaderChecker *leaderChecker,
              const std::string &applicationId) override;
    void setAppPlan(const master_framework::simple_master::AppPlan &appPlan) override;
    void releaseSlots(const std::vector<hippo::SlotId> &slots, const hippo::ReleasePreference &pref) override;
    void getAllRoleSlots(std::map<std::string, master_framework::master_base::SlotInfos> &roleSlots) override;

    static bool writeWorkerPids(const std::string &pidFile, const std::map<std::string, pid_t> &roleStatus);
    static std::map<std::string, pid_t> readWorkerPids(const std::string &pidFile);

private:
    virtual pid_t startProcess(const ProcessLauncherParam &param);
    virtual bool stopProcess(pid_t pid, int signal = 15);
    virtual bool existProcess(pid_t pid);
    virtual bool isAlive(pid_t pid, const std::string &workDir);
    virtual bool isStarted(pid_t pid);

    ProcessLaucherParamMap prepareWorkerLaunchMap(const master_framework::simple_master::AppPlan &appPlan);
    void prepareBrokerLaunchMap(const master_framework::simple_master::AppPlan &appPlan, ProcessLaucherParamMap &);
    void diffWorkers(const ProcessLaucherParamMap &brokerLaunchMap,
                     const std::map<std::string, pid_t> &brokerPids,
                     std::map<std::string, pid_t> &toKeep,
                     std::map<std::string, pid_t> &toStop,
                     std::set<std::string> &toStart);
    void doStopPids(const std::map<std::string, pid_t> &toStop);
    void doStartPids(const std::set<std::string> &toStart,
                     const ProcessLaucherParamMap &brokerLaunchMap,
                     std::map<std::string, pid_t> &newBrokerPids);
    static std::string getBaseDir(const std::string &path);

private:
    std::string _applicationId;
    std::string _workDir;
    std::string _baseDir;
    std::string _logConfFile;
    std::string _binaryPath;
    std::string _pidFile;
    int32_t _maxRestartCount = -1;
    int32_t _startCount = 0;

    static const std::string SWIFT_BROKER_WORKER_NAME;
    static const std::string LOG_FILE_SUFFIX;
    static const std::string SWIFT_LOG_FILE;

private:
    AUTIL_LOG_DECLARE();
};
