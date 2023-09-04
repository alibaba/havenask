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
#include "SimpleMasterSchedulerLocal.h"

#include <iosfwd>
#include <memory>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "hippo/DriverCommon.h"
#include "master_framework/AppPlan.h"
#include "master_framework/Plan.h"
#include "master_framework/common.h"
#include "master_framework/proto/SimpleMaster.pb.h"
#include "swift/util/LocalProcessLauncher.h"

using namespace std;
using namespace hippo;
using namespace autil;
using namespace swift::util;
using namespace master_framework::simple_master;
using namespace master_framework::master_base;

const string SimpleMasterSchedulerLocal::SWIFT_BROKER_WORKER_NAME = "swift_broker";
const string SimpleMasterSchedulerLocal::LOG_FILE_SUFFIX = "/usr/local/etc/swift/swift_alog.conf";
const string SimpleMasterSchedulerLocal::SWIFT_LOG_FILE = "logs/swift/swift.log";
AUTIL_LOG_SETUP(swift, SimpleMasterSchedulerLocal);

SimpleMasterSchedulerLocal::SimpleMasterSchedulerLocal(const string &workDir,
                                                       const string &logConfFile,
                                                       int32_t maxRestartCount)
    : _workDir(workDir), _logConfFile(logConfFile), _maxRestartCount(maxRestartCount) {}

SimpleMasterSchedulerLocal::~SimpleMasterSchedulerLocal() {}

bool SimpleMasterSchedulerLocal::init(const string &hippoZkRoot,
                                      worker_framework::LeaderChecker *leaderChecker,
                                      const string &applicationId) {
    _baseDir = getBaseDir(_workDir);
    if (_baseDir.empty()) {
        AUTIL_LOG(ERROR, "get base dir is empaty, workdir [%s]", _workDir.c_str());
        return false;
    }
    _pidFile = StringUtil::formatString("%s/pids", _baseDir.c_str());
    size_t pos = _logConfFile.rfind(LOG_FILE_SUFFIX);
    if (string::npos != pos) {
        _binaryPath = _logConfFile.substr(0, pos);
    } else {
        AUTIL_LOG(ERROR, "logConfFile[%s] invalid pos[%ld], parse binary path fail", _logConfFile.c_str(), pos);
        return false;
    }
    if (0 == applicationId.size()) {
        return false;
    }
    _applicationId = applicationId;
    return true;
}

void SimpleMasterSchedulerLocal::setAppPlan(const AppPlan &appPlan) {
    auto oldBrokerPids = readWorkerPids(_pidFile);
    auto workerLaunchMap = prepareWorkerLaunchMap(appPlan);
    AUTIL_LOG(DEBUG, "planned broker count[%ld]", workerLaunchMap.size());
    map<string, pid_t> toKeep, toStop;
    set<string> toStart;
    diffWorkers(workerLaunchMap, oldBrokerPids, toKeep, toStop, toStart);
    map<string, pid_t> newBrokerPids = toKeep;
    doStopPids(toStop);
    doStartPids(toStart, workerLaunchMap, newBrokerPids);
    if (toStop.size() + toStart.size() > 0) {
        writeWorkerPids(_pidFile, newBrokerPids);
    }
}

ProcessLaucherParamMap SimpleMasterSchedulerLocal::prepareWorkerLaunchMap(const AppPlan &appPlan) {
    ProcessLaucherParamMap workerLaunchMap;
    prepareBrokerLaunchMap(appPlan, workerLaunchMap);
    return workerLaunchMap;
}

void SimpleMasterSchedulerLocal::prepareBrokerLaunchMap(const AppPlan &appPlan,
                                                        ProcessLaucherParamMap &workerLaunchMap) {
    vector<string> keys = {"-r", "-c", "-t", "-q", "-i"};
    for (const auto &item : appPlan.rolePlans) {
        const string &roleName = item.first;
        const auto &rolePlan = item.second;
        if (rolePlan.processInfos.empty() || rolePlan.processInfos[0].name != SWIFT_BROKER_WORKER_NAME) {
            continue;
        }
        ProcessLauncherParam param;
        string workDir = StringUtil::formatString("%s/%s", _baseDir.c_str(), roleName.c_str());
        param.workDir = workDir;
        param.binPath = _binaryPath + "/usr/local/bin/swift_broker";
        param.args.push_back("-l");
        param.args.push_back(_logConfFile);
        param.args.push_back("-w");
        param.args.push_back(workDir);
        for (const string &key : keys) {
            string value;
            rolePlan.getArg(SWIFT_BROKER_WORKER_NAME, key, value);
            param.args.push_back(key);
            param.args.push_back(value);
        }
        param.args.push_back("--recommendPort");
        workerLaunchMap[roleName] = param;
    }
}

string SimpleMasterSchedulerLocal::getBaseDir(const string &path) {
    if (0 == path.size()) {
        return "";
    }
    string outputStr = path;
    if (path[path.size() - 1] == '/') {
        outputStr = path.substr(0, path.size() - 1);
    }
    size_t pos = outputStr.rfind('/');
    if (string::npos == pos) {
        return "";
    }
    outputStr = outputStr.substr(0, pos);
    return outputStr;
}

void SimpleMasterSchedulerLocal::diffWorkers(const ProcessLaucherParamMap &workerLaunchMap,
                                             const map<string, pid_t> &pids,
                                             map<string, pid_t> &toKeep,
                                             map<string, pid_t> &toStop,
                                             set<string> &toStart) {
    for (auto &brokerPlan : workerLaunchMap) {
        const string &roleName = brokerPlan.first;
        const string &workDir = brokerPlan.second.workDir;
        auto iter = pids.find(roleName);
        if (iter != pids.end()) {
            if (isAlive(iter->second, workDir)) {
                toKeep[roleName] = iter->second;
            } else {
                toStart.insert(roleName);
            }
        } else {
            toStart.insert(roleName);
        }
    }
    for (auto iter = pids.begin(); iter != pids.end(); iter++) {
        if (toKeep.find(iter->first) == toKeep.end() && toStart.count(iter->first) == 0) {
            toStop[iter->first] = iter->second;
        }
    }
}

void SimpleMasterSchedulerLocal::doStopPids(const map<string, pid_t> &toStop) {
    for (auto iter : toStop) {
        pid_t pid = iter.second;
        if (stopProcess(pid)) {
            AUTIL_LOG(INFO, "stop broker[%s], pid[%d] success", iter.first.c_str(), pid);
        } else {
            AUTIL_LOG(INFO, "stop broker[%s], pid[%d] failed", iter.first.c_str(), pid);
        }
    }
}

void SimpleMasterSchedulerLocal::doStartPids(const set<string> &toStart,
                                             const ProcessLaucherParamMap &workerLaunchMap,
                                             map<string, pid_t> &newBrokerPids) {
    for (auto iter : toStart) {
        auto param = workerLaunchMap.find(iter);
        if (param == workerLaunchMap.end()) {
            AUTIL_LOG(INFO, "start param not exist  broker[%s]", iter.c_str());
            continue;
        }
        pid_t pid = startProcess(param->second);
        if (pid < 0) {
            AUTIL_LOG(ERROR, "start broker[%s], failed", iter.c_str());
            continue;
        } else {
            AUTIL_LOG(INFO, "start broker[%s], success", iter.c_str());
            newBrokerPids[iter] = pid;
        }
    }
}

bool SimpleMasterSchedulerLocal::isAlive(pid_t pid, const string &workDir) {
    LocalProcessLauncher procLauncher;
    if (existProcess(pid) && procLauncher.isMyProcess(pid, workDir)) {
        if (!isStarted(pid)) {
            AUTIL_LOG(WARN, "process [%d] is not start or hang, try killed ", pid);
            stopProcess(pid, 9);
            return false;
        } else {
            return true;
        }
    } else {
        return false;
    }
}

pid_t SimpleMasterSchedulerLocal::startProcess(const ProcessLauncherParam &param) {
    if (_maxRestartCount > 0 && _startCount > _maxRestartCount) {
        AUTIL_LOG(WARN,
                  "start process failed, start count [%d] is large than max restart count [%d]",
                  _startCount,
                  _maxRestartCount);
        return -1;
    }
    _startCount++;
    swift::util::LocalProcessLauncher launcher;
    return launcher.start(param.binPath, param.args, param.envs, param.workDir);
}

bool SimpleMasterSchedulerLocal::stopProcess(pid_t pid, int signal) {
    string cmd = "/bin/kill -" + to_string(signal) + " " + to_string(pid) + " 2>&1";
    AUTIL_LOG(INFO, "run cmd [%s]", cmd.c_str());
    auto ret = system(cmd.c_str());
    return ret == 0;
}

bool SimpleMasterSchedulerLocal::existProcess(pid_t pid) {
    return (0 == kill(pid, 0)); // process exists
}

bool SimpleMasterSchedulerLocal::isStarted(pid_t pid) {
    LocalProcessLauncher procLauncher;
    return procLauncher.hasPorcessPidFile(pid);
}

map<string, pid_t> SimpleMasterSchedulerLocal::readWorkerPids(const string &pidFile) {
    map<string, pid_t> lastWorkers;
    string pidContent;
    auto error = fslib::fs::FileSystem::readFile(pidFile, pidContent);
    if (fslib::EC_OK != error) {
        AUTIL_LOG(WARN, "read file[%s] fail, error[%d]", pidFile.c_str(), error);
        return lastWorkers;
    }
    const vector<string> &pids = StringUtil::split(pidContent, "\n");
    for (size_t i = 0; i < pids.size(); ++i) {
        const vector<string> &pidWithRoleName = StringUtil::split(pids[i], " ");
        if (2 != pidWithRoleName.size()) {
            AUTIL_LOG(WARN, "pids file should be [pid:rolename] but now[%s]", pids[i].c_str());
            continue;
        }
        lastWorkers[pidWithRoleName[0]] = stoi(pidWithRoleName[1]);
    }
    return lastWorkers;
}

bool SimpleMasterSchedulerLocal::writeWorkerPids(const string &pidFile, const map<string, pid_t> &roleStatus) {
    string pidContent = "";
    for (auto iter = roleStatus.begin(); iter != roleStatus.end(); ++iter) {
        pidContent += iter->first + " " + to_string(iter->second) + "\n";
    }
    AUTIL_LOG(INFO, "write pid file[%s:%s]", pidFile.c_str(), pidContent.c_str());
    auto error = fslib::fs::FileSystem::writeFile(pidFile, pidContent);
    if (fslib::EC_OK != error) {
        AUTIL_LOG(ERROR, "write pid file[%s:%s] fail, error[%d]", pidFile.c_str(), pidContent.c_str(), error);
        return false;
    }
    return true;
}

void SimpleMasterSchedulerLocal::releaseSlots(const vector<hippo::SlotId> &slots,
                                              const hippo::ReleasePreference &pref) {
    if (0 == slots.size()) {
        AUTIL_LOG(WARN, "No worker assigned");
        return;
    }
    auto pids = readWorkerPids(_pidFile);
    for (auto &slot : slots) {
        const string &roleName = slot.slaveAddress;
        auto iter = pids.find(roleName);
        if (pids.end() != iter) {
            AUTIL_LOG(INFO, "command change slot[%s]", roleName.c_str());
            if (stopProcess(iter->second)) {
                AUTIL_LOG(INFO, "stop worker[%s], pid[%d] success", roleName.c_str(), iter->second);
            } else {
                AUTIL_LOG(ERROR, "stop worker[%s], pid[%d] failed", roleName.c_str(), iter->second);
            }
        }
    }
}

void SimpleMasterSchedulerLocal::getAllRoleSlots(std::map<std::string, SlotInfos> &roleSlots) {
    auto pids = readWorkerPids(_pidFile);
    for (auto &item : pids) {
        roleSlots[item.first] = SlotInfos();
    }
}
