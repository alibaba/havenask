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
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/SharedPtrGuard.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "fslib/common/common_type.h"
#include "hippo/DriverCommon.h"

namespace build_service { namespace worker {

class AgentServiceImpl : public WorkerStateHandler
{
public:
    AgentServiceImpl(const std::map<std::string, std::string>& procArgMap, const proto::PartitionId& pid,
                     indexlib::util::MetricProviderPtr metricProvider, const proto::LongAddress& address,
                     const std::string& appZkRoot = "", const std::string& adminServiceName = "");

    ~AgentServiceImpl();

    AgentServiceImpl(const AgentServiceImpl&) = delete;
    AgentServiceImpl& operator=(const AgentServiceImpl&) = delete;
    AgentServiceImpl(AgentServiceImpl&&) = delete;
    AgentServiceImpl& operator=(AgentServiceImpl&&) = delete;

public:
    bool init() override;
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override { return false; }
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override;

private:
    typedef std::pair<std::string, std::string> ProcessKey; // first: RoleName, second: procName
    struct ProcessInfo {
        std::string cmd;
        std::string configPath;
        std::vector<std::pair<std::string, std::string>> envs;
    };

    struct ProcessData : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("role", roleName, roleName);
            json.Jsonize("name", procName, procName);
            json.Jsonize("pid", pid, pid);
        }

        static ProcessData make(const std::string& role, const std::string& name, pid_t pid)
        {
            ProcessData data;
            data.roleName = role;
            data.procName = name;
            data.pid = pid;
            return data;
        }
        std::string roleName;
        std::string procName;
        pid_t pid;
    };

private:
    bool updateAppPlan(const proto::AgentTarget& target);
    bool createProcessPlan(const proto::AgentTarget& target, std::map<ProcessKey, ProcessInfo>& plan) const;
    pid_t startProcess(const std::string& roleName, const std::string& procName, const std::string& cmd,
                       const std::string& configPath,
                       const std::vector<std::pair<std::string, std::string>>& envs = {});

    bool stopProcess(pid_t pid);
    bool existProcess(pid_t pid);
    void updateWorkerStatus();
    void updateLatestConfig();
    void cleanUselessDir();
    void updatePidFile();
    void reuseCurrentProc(bool& bPidChanged);

    virtual std::string getBinaryPath() const;
    std::string getRoleWorkDir(const std::string& roleName, const std::string& procName) const;
    bool createWorkDir(const std::string& dir);
    void prepareConfigSymLink(const std::string& workDir, const std::string& configPath);
    void prepareBinarySymLink(const std::string& workDir);
    bool killProcess(pid_t pid, int32_t signal);

    std::vector<std::string> setEnvForSubProcess(const std::string& workDir, const std::vector<hippo::PairType>& envs);
    char** makeArgsList(const std::vector<std::string>& args);

    // TODO: remove
    bool parseLegacyPidFile(const std::string& content, std::vector<ProcessData>& pidDatas);
    static void parseCmdline(std::string& cmdline, std::string& roleName);
    void prepareGlobalEnvironMap();
    std::string normalizeEnvArguments(const std::map<std::string, std::string>& envMap, const std::string& rawString);
    std::string getLocalConfigDirName(const std::string& configPath);

    void fillInUseLinkConfigDirNames(const std::set<std::string>& inUseRoleNames,
                                     std::set<std::string>& inUseConfigNames);
    void removeUselessDir(const std::string& rootDir, const std::set<std::string>& inUseSubDirNames);

    void extractToRemoveSubDirs(const fslib::RichFileList& fileList, const std::set<std::string>& inUseSubDirNames,
                                int64_t currentTimestamp, int64_t expireTimeInSec, size_t maxUselessDirCount,
                                std::vector<std::string>& toRemoveExpiredSubDirs,
                                std::vector<std::string>& toRemoveUnExpiredSubDirs);

    void removeTargetSubDirs(const std::string& rootDir, std::vector<std::string>& toRemoveExpireSubDirs,
                             std::vector<std::string>& toRemoveUnExpireSubDirs);

private:
    std::string _agentRoleName;
    std::map<std::string, std::string> _procArgMap;
    proto::AgentCurrent _current;
    mutable autil::RecursiveThreadMutex _lock;
    autil::ThreadMutex _processStatusLock;

    std::map<ProcessKey, ProcessInfo> _targetProcessPlan; // { roleName, name } -> {cmd, envs}
    std::map<ProcessKey, pid_t> _processStatus;           // { roleName, name } -> pid
    autil::LoopThreadPtr _workerStatusUpdaterThread;
    autil::LoopThreadPtr _latestConfigUpdaterThread;
    autil::LoopThreadPtr _cleanUselessDirThread;
    std::string _workDir;
    std::string _binaryPath;

    // first: configPath, second: download status
    typedef std::pair<std::vector<std::string>, bool> ConfigDownloadInfo;
    util::SharedPtrGuard<ConfigDownloadInfo> _latestConfigGuard;
    std::map<std::string, std::string> _globalEnvironMap;

    int64_t _loopThreadInterval;
    volatile int64_t _totalRestartCnt;
    int64_t _latestRestartTimestamp;
    int64_t _latestCleanDirTimestamp;

    // add metrics
    indexlib::util::MetricPtr _targetRoleCntMetric;
    indexlib::util::MetricPtr _subProcessCntMetric;
    indexlib::util::MetricPtr _totalRestartCntMetric;
    indexlib::util::MetricPtr _startProcessQps;
    indexlib::util::MetricPtr _startProcessFailQps;
    indexlib::util::MetricPtr _stopProcessQps;
    indexlib::util::MetricPtr _deadProcessQps;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AgentServiceImpl);

}} // namespace build_service::worker
