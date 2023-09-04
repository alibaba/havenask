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
#include "build_service/admin/SimpleMasterSchedulerLocal.h"

#include <dirent.h>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, SimpleMasterSchedulerLocal);

namespace {
const int64_t WORKER_STATUS_UPDATE_INTERVAL_US = (1 * 1000 * 1000);
} // namespace

bool asyncCall(const string& command, const std::string& binaryPath, pid_t& pid)
{
    pid_t childPid;
    if ((childPid = fork()) == -1) {
        return false;
    }
    if (childPid == 0) {
        // child process
        setpgid(childPid, childPid);
        const vector<string>& cmdParams = autil::StringUtil::split(command, " ");
        const char** argv = new const char*[cmdParams.size() + 1];
        const char* pathname = cmdParams[0].c_str();
        for (int i = 0; i < cmdParams.size(); ++i) {
            argv[i] = cmdParams[i].c_str();
        }
        std::string paths = autil::EnvUtil::getEnv("PATH") + ":" + binaryPath;
        assert(autil::EnvUtil::setEnv("PATH", paths, true));
        argv[cmdParams.size()] = NULL;
        execv(pathname, (char**)argv);
        delete[] argv;
        exit(0);
    } else {
        // parent process
        pid = childPid;
    }
    return true;
}

SimpleMasterSchedulerLocal::SimpleMasterSchedulerLocal(const std::string& workDir, const std::string& logConfFile)
    : _globalSlotCount(0)
    , _delayReleaseSecond(0)
    , _workDir(workDir)
    , _logConfFile(logConfFile)
    , _binaryPath(workDir)
{
    _delayReleaseSecond = autil::EnvUtil::getEnv("DELAY_RELEASE_SECOND_FOR_BS_LOCAL", 0);
    AUTIL_LOG(INFO, "delay release second is [%ld]", _delayReleaseSecond);
    auto binaryPath = autil::EnvUtil::getEnv("BINARY_PATH_FOR_BS_LOCAL");
    if (!binaryPath.empty()) {
        _binaryPath = binaryPath;
    } else {
        const string suffix("/usr/local/etc/bs/bs_admin_alog.conf");
        size_t pos = logConfFile.rfind(suffix);
        if (string::npos != pos) {
            std::string binaryRoot = logConfFile.substr(0, pos);
            _binaryPath = indexlib::util::PathUtil::JoinPath(binaryRoot, "usr/local/bin/");
        } else {
            AUTIL_LOG(WARN, "failed to parse logConfFile path:[%s] as binary path, use workDir:[%s] as binary path",
                      logConfFile.c_str(), workDir.c_str());
        }
    }
}

SimpleMasterSchedulerLocal::~SimpleMasterSchedulerLocal()
{
    if (_workerStatusUpdaterThread) {
        _workerStatusUpdaterThread->stop();
        _workerStatusUpdaterThread.reset();
    }
    autil::ScopedLock lock(_roleStatusLock);
    for (const auto& [roleName, processItems] : _roleName2ProcessItem) {
        for (const auto& processItem : processItems) {
            if (!stopProcess(processItem.pid)) {
                AUTIL_LOG(ERROR, "stop process [%s], pid [%d] failed", roleName.c_str(), processItem.pid);
            }
        }
    }
}

void SimpleMasterSchedulerLocal::updateWorkerStatus()
{
    AUTIL_LOG(INFO, "begin update role status");
    pid_t pid = waitpid(-1, NULL, WNOHANG);
    if (pid > 0) {
        AUTIL_LOG(INFO, "waitpid return[%d]", pid);
    }
    autil::ScopedLock lock(_roleStatusLock);
    for (auto& [roleName, processItems] : _roleName2ProcessItem) {
        for (auto& processItem : processItems) {
            if (!existProcess(processItem)) {
                if (!processItem.stopped) {
                    // restart process
                    auto [pid, slotId] = startProcess(roleName, processItem.rolePlan);

                    if (pid < 0) {
                        AUTIL_LOG(WARN, "restart worker[%s] failed, pid[%d].", roleName.c_str(), processItem.pid);
                    } else {
                        AUTIL_LOG(INFO, "restart worker[%s] success, pid[%d].", roleName.c_str(), processItem.pid);
                        processItem.pid = pid;
                        processItem.slotId.id = slotId;
                    }
                }
            }
        }
    }
}

bool SimpleMasterSchedulerLocal::init(const std::string& hippoZkRoot, worker_framework::LeaderChecker* leaderChecker,
                                      const std::string& applicationId)
{
    if (0 == applicationId.size()) {
        return false;
    }
    _workerStatusUpdaterThread =
        autil::LoopThread::createLoopThread(std::bind(&SimpleMasterSchedulerLocal::updateWorkerStatus, this),
                                            int64_t(WORKER_STATUS_UPDATE_INTERVAL_US), "roleStatUpd");
    if (!_workerStatusUpdaterThread) {
        AUTIL_LOG(ERROR, "create worker status updater thread fail");
        return false;
    }
    return true;
}

std::string SimpleMasterSchedulerLocal::genWorkerCmd(const std::string& roleName,
                                                     const master_framework::master_base::RolePlan& rolePlan,
                                                     int32_t slotId) const
{
    vector<string> keys = {"-z", "-r", "-s", "-m", "-a"};

    const string& workDir = _workDir + "/" + roleName;
    string args;
    for (const string& key : keys) {
        string value;
        rolePlan.getArg("build_service_worker", key, value);
        args += key + " " + value + " ";
    }
    args += " -l " + _logConfFile + " -w " + workDir + " -d rpc";

    string envStr;
    auto envs = rolePlan.processInfos[0].envs;
    envs.emplace_back("HIPPO_SLOT_ID", std::to_string(slotId));
    for (auto [key, value] : envs) {
        envStr += key + "=" + value + " ";
    }
    assert(!envStr.empty());
    return "/bin/mkdir -p " + workDir + ";/bin/env " + envStr + _binaryPath + "/build_service_worker " + args;
}

void SimpleMasterSchedulerLocal::setAppPlan(const master_framework::simple_master::AppPlan& appPlan)
{
    auto workerRoleMap = appPlan.rolePlans;
    autil::ScopedLock lock(_roleStatusLock);
    // 2. release plan里没有的
    for (auto iter = _roleName2ProcessItem.begin(); iter != _roleName2ProcessItem.end(); ++iter) {
        if (workerRoleMap.end() == workerRoleMap.find(iter->first)) {
            auto items = iter->second;
            std::vector<ProcessItem> newItems;
            for (auto item : items) {
                innerReleaseSlotUnsafe(item.slotId);
            }
        }
    }

    // 3. 停掉该被release的
    auto currentSecond = getCurrentSecond();
    decltype(_roleName2ProcessItem) newItems;
    for (auto iter = _roleName2ProcessItem.begin(); iter != _roleName2ProcessItem.end(); iter++) {
        auto items = iter->second;
        auto roleName = iter->first;
        for (auto item : items) {
            bool stillExist = true;
            if (item.toReleaseSecond != -1 && currentSecond >= item.toReleaseSecond) {
                pid_t pid = item.pid;
                if (stopProcess(pid)) {
                    stillExist = false;
                    AUTIL_LOG(INFO, "stop worker[%s], pid[%d] success", iter->first.c_str(), pid);
                }
            }
            if (stillExist) {
                newItems[roleName].push_back(item);
            }
        }
    }
    _roleName2ProcessItem = newItems;

    // 4. 去掉已经起来的
    for (auto iter = workerRoleMap.begin(); iter != workerRoleMap.end();) {
        bool hasValidProcess = false;
        auto itemsIter = _roleName2ProcessItem.find(iter->first);
        if (itemsIter != _roleName2ProcessItem.end()) {
            for (auto singleItem : itemsIter->second) {
                if (!singleItem.reclaiming) {
                    hasValidProcess = true;
                }
            }
        }
        if (hasValidProcess) {
            iter = workerRoleMap.erase(iter);
        } else {
            ++iter;
        }
    }

    // 5. 启动剩下的
    for (auto iter = workerRoleMap.begin(); iter != workerRoleMap.end(); ++iter) {
        auto [pid, slotId] = startProcess(iter->first, iter->second);
        if (pid < 0) {
            AUTIL_LOG(ERROR, "start worker[%s] failed", iter->first.c_str());
            continue;
        }
        ProcessItem item(slotId, pid, /*stopped*/ false, iter->second);
        _roleName2ProcessItem[iter->first].push_back(std::move(item));
    }
}

std::pair<pid_t, int32_t>
SimpleMasterSchedulerLocal::startProcess(const std::string& roleName,
                                         const master_framework::master_base::RolePlan& rolePlan)
{
    int32_t slotId = _globalSlotCount++;
    pid_t pid = -1;
    string cmd = genWorkerCmd(roleName, rolePlan, slotId);
    const vector<string>& cmdLst = autil::StringUtil::split(cmd, ";");
    assert(2 == cmdLst.size());

    for (auto& cmd : cmdLst) {
        if (asyncCall(cmd, _binaryPath, pid)) {
            AUTIL_LOG(INFO, "run cmd [%s], pid[%d]", cmd.c_str(), pid);
        } else {
            AUTIL_LOG(INFO, "run cmd [%s], failed!", cmd.c_str());
            return {-1, slotId};
        }
    }
    return {pid, slotId};
}

void SimpleMasterSchedulerLocal::getAllRoleSlots(
    std::map<std::string, master_framework::master_base::SlotInfos>& roleSlots)
{
    roleSlots.clear();
    autil::ScopedLock lock(_roleStatusLock);
    for (auto iter = _roleName2ProcessItem.begin(); iter != _roleName2ProcessItem.end(); iter++) {
        master_framework::master_base::SlotInfos infos;
        for (auto singleProcess : iter->second) {
            hippo::SlotInfo slotInfo;
            slotInfo.slotId = singleProcess.slotId;
            slotInfo.reclaiming = singleProcess.reclaiming;
            infos.push_back(slotInfo);
        }
        roleSlots.insert({iter->first, infos});
    }
}

void SimpleMasterSchedulerLocal::innerReleaseSlotUnsafe(const hippo::SlotId& slotId)
{
    for (auto& processorPair : _roleName2ProcessItem) {
        auto& items = processorPair.second;
        for (auto& item : items) {
            if (item.slotId == slotId && !item.reclaiming) {
                item.reclaiming = true;
                item.toReleaseSecond = getCurrentSecond() + _delayReleaseSecond;
                AUTIL_LOG(INFO, "pid [%d] will be release at [%ld]", item.pid, item.toReleaseSecond);
            }
        }
    }
}

void SimpleMasterSchedulerLocal::releaseSlots(const std::vector<hippo::SlotId>& slots,
                                              const hippo::ReleasePreference& pref)
{
    autil::ScopedLock lock(_roleStatusLock);
    for (const auto& slot : slots) {
        innerReleaseSlotUnsafe(slot);
    }
}

bool SimpleMasterSchedulerLocal::stopProcess(pid_t pid)
{
    string cmd = "/bin/kill " + to_string(pid);
    AUTIL_LOG(INFO, "run cmd [%s]", cmd.c_str());
    pid_t killProcessPid;
    return asyncCall(cmd, _binaryPath, killProcessPid);
}

namespace {
int isNumeric(const char* str)
{
    for (; *str; str++)
        if (*str < '0' || *str > '9')
            return 0; // false
    return 1;         // true
}
}; // namespace

bool SimpleMasterSchedulerLocal::existProcess(const ProcessItem& item) const
{
    DIR* dir = opendir("/proc/");
    if (dir == NULL) {
        AUTIL_LOG(ERROR, "open dir /proc failed.");
        return false;
    }
    bool found = false;
    static constexpr size_t bufLen = 4096;
    char buf[bufLen] = {0};
    struct dirent* dirEntry = NULL;
    while ((dirEntry = readdir(dir))) {
        if (dirEntry->d_type != DT_DIR) {
            continue;
        }
        if (!isNumeric(dirEntry->d_name)) {
            continue;
        }
        auto id = atoi(dirEntry->d_name);
        if (id != item.pid) {
            continue;
        }
        std::stringstream ss;
        ss << "/proc/" << dirEntry->d_name << "/status";
        FILE* fd = fopen(ss.str().c_str(), "rt");
        if (fd == NULL) {
            AUTIL_LOG(ERROR, "fopen file[%s] failed.", ss.str().c_str());
            continue;
        }
        [[maybe_unused]] auto len = fread(buf, bufLen, 1, fd);
        fclose(fd);
        // format like: State:\tZ
        char* pos = strstr(buf, "State:");
        if (pos == NULL) {
            continue;
        }
        if (pos[7] == 'Z') {
            // zombie
            AUTIL_LOG(INFO, "process pid[%d] is zombie.", item.pid);
            return false;
        }
        found = true;
        break;
    }
    if (!found) {
        AUTIL_LOG(INFO, "process[%d] not found, maybe killed.", item.pid);
    }
    return found;
}

int64_t SimpleMasterSchedulerLocal::getCurrentSecond() const { return autil::TimeUtility::currentTimeInSeconds(); }
}} // namespace build_service::admin
