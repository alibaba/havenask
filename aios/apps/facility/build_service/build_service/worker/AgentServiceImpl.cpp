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
#include "build_service/worker/AgentServiceImpl.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "autil/CRC32C.h"
#include "build_service/common/ConfigDownloader.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, AgentServiceImpl);

#define STDOUT_FILE_NAME "stdout.out"
#define STDERR_FILE_NAME "stderr.out"

namespace {
const int64_t WORKER_THREAD_LOOP_INTERVAL_US =
    autil::EnvUtil::getEnv("bs_agent_thread_loop_interval", (int64_t)(5 * 1000 * 1000)); // 5s
const int64_t AGENT_INNER_RESTART_COUNT_THRESHOLD =
    autil::EnvUtil::getEnv("bs_agent_restart_count_threshold", (int64_t)10);
const int64_t AGENT_INNER_RESTART_COUNT_RESET_INTERVAL =
    autil::EnvUtil::getEnv("bs_agent_restart_count_reset_interval", (int64_t)600); // 10min
const bool AGENT_FORK_WITH_NEW_SESSION = autil::EnvUtil::getEnv("bs_agent_fork_with_new_session", true);
const int64_t AGENT_USELESS_DIR_EXPIRE_TIME_IN_SEC =
    autil::EnvUtil::getEnv("bs_agent_useless_dir_expire_time", (int64_t)(24 * 60 * 60)); // 24h
const int64_t AGENT_CLEAN_USELESS_DIR_INTERVAL =
    autil::EnvUtil::getEnv("bs_agent_clean_useless_dir_interval", (int64_t)600); // 10min
const size_t AGENT_MAX_KEEP_USELESS_DIR_COUNT =
    autil::EnvUtil::getEnv("bs_agent_max_keep_useless_dir_count", (size_t)256); // 256
} // namespace

AgentServiceImpl::AgentServiceImpl(const std::map<std::string, std::string>& procArgMap, const PartitionId& pid,
                                   indexlib::util::MetricProviderPtr metricProvider, const LongAddress& address,
                                   const string& appZkRoot, const string& adminServiceName)
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, "")
    , _procArgMap(procArgMap)
    , _loopThreadInterval(WORKER_THREAD_LOOP_INTERVAL_US)
    , _totalRestartCnt(0)
    , _latestRestartTimestamp(-1)
    , _latestCleanDirTimestamp(-1)
{
    *_current.mutable_longaddress() = address;

    _targetRoleCntMetric = DECLARE_METRIC(_metricProvider, "targetRoleCount", kmonitor::STATUS, "count");
    _subProcessCntMetric = DECLARE_METRIC(_metricProvider, "subProcessCount", kmonitor::STATUS, "count");
    _totalRestartCntMetric = DECLARE_METRIC(_metricProvider, "totalRestartCount", kmonitor::STATUS, "count");
    _startProcessQps = DECLARE_METRIC(_metricProvider, "startProcessQps", kmonitor::QPS, "count");
    _startProcessFailQps = DECLARE_METRIC(_metricProvider, "startProcessFailQps", kmonitor::QPS, "count");
    _stopProcessQps = DECLARE_METRIC(_metricProvider, "stopProcessQps", kmonitor::QPS, "count");
    _deadProcessQps = DECLARE_METRIC(_metricProvider, "deadProcessQps", kmonitor::QPS, "count");
}

AgentServiceImpl::~AgentServiceImpl()
{
    _workerStatusUpdaterThread.reset();
    _latestConfigUpdaterThread.reset();
    _cleanUselessDirThread.reset();

    autil::ScopedLock lock(_processStatusLock);
    for (auto iter = _processStatus.begin(); iter != _processStatus.end(); iter++) {
        if (!stopProcess(iter->second)) {
            AUTIL_LOG(ERROR, "stop process roleName [%s], procName [%s], pid [%d] failed", iter->first.first.c_str(),
                      iter->first.second.c_str(), iter->second);
        }
    }
}

bool AgentServiceImpl::init()
{
    _latestCleanDirTimestamp = autil::TimeUtility::currentTimeInSeconds();
    prepareGlobalEnvironMap();
    if (!fslib::util::FileUtil::getCurrentPath(_workDir)) {
        AUTIL_LOG(ERROR, "get cwd path fail.");
        return false;
    }

    _binaryPath = getBinaryPath();
    _workerStatusUpdaterThread = autil::LoopThread::createLoopThread(
        std::bind(&AgentServiceImpl::updateWorkerStatus, this), _loopThreadInterval, "roleStatUpd");
    if (!_workerStatusUpdaterThread) {
        AUTIL_LOG(ERROR, "create worker status updater thread fail");
        return false;
    }

    _latestConfigUpdaterThread = autil::LoopThread::createLoopThread(
        std::bind(&AgentServiceImpl::updateLatestConfig, this), _loopThreadInterval, "configUpd");
    if (!_latestConfigUpdaterThread) {
        AUTIL_LOG(ERROR, "create latest config updater thread fail");
        return false;
    }

    _cleanUselessDirThread = autil::LoopThread::createLoopThread(std::bind(&AgentServiceImpl::cleanUselessDir, this),
                                                                 _loopThreadInterval, "cleanUseless");
    if (!_cleanUselessDirThread) {
        AUTIL_LOG(ERROR, "create clean useless dir thread fail");
        return false;
    }
    bool ret = _cpuEstimater.Start(/*sampleCountLimit=*/5, /*checkInterval=*/60);
    if (!ret) {
        return false;
    }
    ret = _networkEstimater.Start();
    _current.set_status(proto::WS_STARTED);
    return ret;
}

void AgentServiceImpl::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    proto::AgentTarget target;
    if (!target.ParseFromString(state)) {
        BS_LOG(ERROR, "invalid agent target string: %s", state.c_str());
        return;
    }

    if (_current.targetstatus() == target) {
        return;
    }

    BS_LOG(INFO, "target status: %s", target.ShortDebugString().c_str());
    if (updateAppPlan(target)) {
        ScopedLock lock(_lock);
        *_current.mutable_targetstatus() = target;
    } else {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "update plan for target [" + target.ShortDebugString() + "]failed",
                       BS_RETRY);
        setFatalError();
    }
}

bool AgentServiceImpl::updateAppPlan(const proto::AgentTarget& target)
{
    std::vector<std::string> targetConfigs;
    auto addTargetConfig = [&](const std::string& configPath) {
        if (!configPath.empty()) {
            targetConfigs.push_back(configPath);
        }
    };

    if (target.has_configpath()) {
        addTargetConfig(target.configpath());
    }
    for (size_t i = 0; i < target.globalconfigs_size(); i++) {
        addTargetConfig(target.globalconfigs(i));
    }
    if (!targetConfigs.empty()) {
        _latestConfigGuard.set(std::make_shared<ConfigDownloadInfo>(std::make_pair(targetConfigs, false)));
    }

    // 1. 生成所有的启动命令、参数
    std::map<ProcessKey, ProcessInfo> processPlan;
    if (!createProcessPlan(target, processPlan)) {
        return false;
    }

    bool pidChange = false;
    autil::ScopedLock lock(_processStatusLock);
    _targetProcessPlan = processPlan;
    REPORT_METRIC(_targetRoleCntMetric, _targetProcessPlan.size());
    reuseCurrentProc(pidChange);

    // 2. 停掉plan里没有的
    for (auto iter = _processStatus.begin(); iter != _processStatus.end();) {
        if (processPlan.end() == processPlan.find(iter->first)) {
            pid_t pid = iter->second;
            if (stopProcess(pid) || !existProcess(pid)) {
                pidChange = true;
                AUTIL_LOG(INFO, "stop worker[%s] procName[%s], pid[%d] success", iter->first.first.c_str(),
                          iter->first.second.c_str(), pid);
                iter = _processStatus.erase(iter);
            } else {
                AUTIL_LOG(ERROR, "stop worker[%s] procName[%s], pid[%d] failed", iter->first.first.c_str(),
                          iter->first.second.c_str(), pid);
                ++iter;
            }
        } else {
            ++iter;
        }
    }

    // 3. 去掉已经起来的
    for (auto iter = processPlan.begin(); iter != processPlan.end();) {
        if (_processStatus.end() != _processStatus.find(iter->first)) {
            iter = processPlan.erase(iter);
        } else {
            ++iter;
        }
    }

    // 4. 启动剩下的
    for (auto iter = processPlan.begin(); iter != processPlan.end(); ++iter) {
        pid_t pid = startProcess(iter->first.first, iter->first.second, iter->second.cmd, iter->second.configPath,
                                 iter->second.envs);
        if (pid < 0) {
            AUTIL_LOG(ERROR, "start worker[%s], procName[%s] failed", iter->first.first.c_str(),
                      iter->first.second.c_str());
            INCREASE_QPS(_startProcessFailQps);
            continue;
        }
        _processStatus[iter->first] = pid;
        pidChange = true;
    }

    if (pidChange) {
        updatePidFile();
    }
    return true;
}

pid_t AgentServiceImpl::startProcess(const std::string& roleName, const std::string& procName, const std::string& cmd,
                                     const std::string& configPath,
                                     const std::vector<std::pair<std::string, std::string>>& envs)
{
    auto workDir = getRoleWorkDir(roleName, procName);
    if (!createWorkDir(workDir)) {
        AUTIL_LOG(ERROR,
                  "create workdir failed for process:[%s], "
                  "workdir:[%s]",
                  cmd.c_str(), workDir.c_str());
        return -1;
    }

    if (procName == "build_service_worker") {
        prepareBinarySymLink(workDir);
        prepareConfigSymLink(workDir, configPath);
    }
    pid_t pid;
    if ((pid = fork()) == -1) {
        return -1;
    }
    if (pid == 0) {
        // child process
        if (AGENT_FORK_WITH_NEW_SESSION) {
            setsid();
        } else {
            setpgid(0, 0);
        }
        (void)chdir(workDir.c_str());

        auto envParams = setEnvForSubProcess(workDir, envs);
        auto cmdParams = autil::StringUtil::split(cmd, " ");
        for (int i = 0; i < cmdParams.size(); ++i) {
            cmdParams[i] = autil::EnvUtil::envReplace(cmdParams[i]);
        }
        const char* pathname = cmdParams[0].c_str();

        char** argsList = makeArgsList(cmdParams);
        char** envList = makeArgsList(envParams);

        fclose(stdout);
        fclose(stderr);
        umask(0);

        string pidSuffixStr = "." + StringUtil::toString(getpid());
        string stdoutFile = string(STDOUT_FILE_NAME) + pidSuffixStr;
        string stderrFile = string(STDERR_FILE_NAME) + pidSuffixStr;
        (void)freopen(stdoutFile.c_str(), "w+", stdout);
        (void)freopen(stderrFile.c_str(), "w+", stderr);
        execve(pathname, argsList, envList);
        fprintf(stderr, "command(%s) is not execve successful, error code: %d(%s)\n", cmd.c_str(), errno,
                strerror(errno));
        fflush(stdout);
        fflush(stderr);
        _exit(-1); // error occurred
    }
    AUTIL_LOG(INFO, "start process. cmd:[%s], pid:[%d], workdir:[%s]", cmd.c_str(), pid, workDir.c_str());
    INCREASE_QPS(_startProcessQps);
    return pid;
}

bool AgentServiceImpl::stopProcess(pid_t pid) { return killProcess(pid, SIGTERM); }

bool AgentServiceImpl::killProcess(pid_t pid, int32_t signal)
{
    assert(pid > 1);
    if (kill(-pid, signal) != 0) {
        int ec = errno;
        AUTIL_LOG(ERROR,
                  "kill process failed. pid:[%d], sig:[%d], "
                  "errno:[%d], errmsg:[%s]",
                  pid, signal, ec, strerror(ec));
        return false;
    }
    AUTIL_LOG(INFO, "stop process [%d] by sig [%d]", pid, signal);
    INCREASE_QPS(_stopProcessQps);
    return true;
}

bool AgentServiceImpl::existProcess(pid_t pid)
{
    return (0 == kill(pid, 0)); // process exists
}

void AgentServiceImpl::updateLatestConfig()
{
    std::shared_ptr<ConfigDownloadInfo> latestConfigPtr;
    _latestConfigGuard.get(latestConfigPtr);
    if (latestConfigPtr == nullptr || latestConfigPtr->first.empty() || latestConfigPtr->second) {
        return;
    }
    bool success = true;
    for (auto remoteConfigPath : latestConfigPtr->first) {
        auto localConfigPath = fslib::util::FileUtil::joinFilePath(
            fslib::util::FileUtil::joinFilePath(_workDir, "config"), getLocalConfigDirName(remoteConfigPath));
        common::ConfigDownloader::DownloadErrorCode errorCode =
            common::ConfigDownloader::downloadConfig(remoteConfigPath, localConfigPath);
        if (errorCode == common::ConfigDownloader::DEC_NORMAL_ERROR ||
            errorCode == common::ConfigDownloader::DEC_DEST_ERROR) {
            success = false;
            BS_LOG(WARN, "downloadConfig from %s failed", remoteConfigPath.c_str());
        }
    }
    latestConfigPtr->second = success;
    if (success) {
        ScopedLock lock(_lock);
        _current.set_serviceready(true);
    }
}

void AgentServiceImpl::cleanUselessDir()
{
    auto currentTimestamp = autil::TimeUtility::currentTimeInSeconds();
    if (currentTimestamp - _latestCleanDirTimestamp < AGENT_CLEAN_USELESS_DIR_INTERVAL) {
        return;
    }
    _latestCleanDirTimestamp = currentTimestamp;
    AUTIL_LOG(INFO, "begin clean useless config and role dir");
    std::shared_ptr<ConfigDownloadInfo> latestConfigPtr;
    _latestConfigGuard.get(latestConfigPtr);
    std::set<std::string> inUseConfigNames;
    if (latestConfigPtr) {
        for (auto remoteConfigPath : latestConfigPtr->first) {
            inUseConfigNames.insert(getLocalConfigDirName(remoteConfigPath));
        }
    }
    std::set<std::string> inUseRoleNames;
    {
        autil::ScopedLock lock(_processStatusLock);
        for (auto iter = _processStatus.begin(); iter != _processStatus.end(); iter++) {
            inUseRoleNames.insert(iter->first.first);
        }
        for (auto iter = _targetProcessPlan.begin(); iter != _targetProcessPlan.end(); iter++) {
            inUseRoleNames.insert(iter->first.first);
        }
    }
    fillInUseLinkConfigDirNames(inUseRoleNames, inUseConfigNames);
    removeUselessDir(fslib::util::FileUtil::joinFilePath(_workDir, "target_roles"), inUseRoleNames);
    removeUselessDir(fslib::util::FileUtil::joinFilePath(_workDir, "config"), inUseConfigNames);
}

void AgentServiceImpl::fillInUseLinkConfigDirNames(const std::set<std::string>& inUseRoleNames,
                                                   std::set<std::string>& inUseConfigNames)
{
    string srcConfigRoot = fslib::util::FileUtil::joinFilePath(_workDir, "config");
    for (auto& roleName : inUseRoleNames) {
        string configDir =
            fslib::util::FileUtil::joinFilePath(getRoleWorkDir(roleName, "build_service_worker"), "config");
        fslib::FileList fileList;
        fslib::fs::FileSystem::listDir(configDir, fileList);
        for (auto item : fileList) {
            string configPath = fslib::util::FileUtil::joinFilePath(configDir, item);
            if (!fslib::util::FileUtil::isLink(configPath)) {
                continue;
            }
            string linkPath = fslib::util::FileUtil::readLink(configPath);
            if (linkPath.empty()) {
                continue;
            }
            string reletivePath;
            if (!indexlib::util::PathUtil::GetRelativePath(srcConfigRoot, linkPath, reletivePath)) {
                AUTIL_LOG(ERROR, "getReletivePath failed, parent path [%s], config path [%s]", srcConfigRoot.c_str(),
                          linkPath.c_str());
            }
            vector<string> dirNames = autil::StringUtil::split(reletivePath, "/");
            if (!dirNames.empty()) {
                AUTIL_LOG(INFO, "find in-use dir [%s] in config root [%s].", dirNames[0].c_str(),
                          srcConfigRoot.c_str());
                inUseConfigNames.insert(dirNames[0]);
            }
        }
    }
}

void AgentServiceImpl::removeUselessDir(const std::string& rootDir, const std::set<std::string>& inUseSubDirNames)
{
    fslib::RichFileList fileList;
    fslib::fs::FileSystem::listDir(rootDir, fileList);
    auto currentTimestamp = autil::TimeUtility::currentTimeInSeconds();

    vector<string> toRemoveExpireSubDirs;
    vector<string> toRemoveUnExpireSubDirs;
    extractToRemoveSubDirs(fileList, inUseSubDirNames, currentTimestamp, AGENT_USELESS_DIR_EXPIRE_TIME_IN_SEC,
                           AGENT_MAX_KEEP_USELESS_DIR_COUNT, toRemoveExpireSubDirs, toRemoveUnExpireSubDirs);

    removeTargetSubDirs(rootDir, toRemoveExpireSubDirs, toRemoveUnExpireSubDirs);
}

void AgentServiceImpl::extractToRemoveSubDirs(const fslib::RichFileList& fileList,
                                              const std::set<std::string>& inUseSubDirNames, int64_t currentTimestamp,
                                              int64_t expireTimeInSec, size_t maxUselessDirCount,
                                              std::vector<std::string>& toRemoveExpiredSubDirs,
                                              std::vector<std::string>& toRemoveUnExpiredSubDirs)
{
    std::vector<std::pair<std::string, uint64_t>> unexpiredUselessDirs;
    for (auto& item : fileList) {
        if (inUseSubDirNames.find(item.path) != inUseSubDirNames.end()) {
            continue;
        }
        if (item.lastModifyTime + expireTimeInSec >= currentTimestamp) {
            unexpiredUselessDirs.push_back(std::make_pair(item.path, item.lastModifyTime));
            continue;
        }
        toRemoveExpiredSubDirs.push_back(item.path);
    }

    if (unexpiredUselessDirs.size() > maxUselessDirCount) {
        AUTIL_LOG(INFO, "trigger remove useless dir for useless dir count over [%lu]", maxUselessDirCount);
        std::sort(unexpiredUselessDirs.begin(), unexpiredUselessDirs.end(),
                  [](const std::pair<std::string, uint64_t>& lft, const std::pair<std::string, uint64_t>& rht) {
                      return lft.second < rht.second;
                  });
        size_t toRemoveDirCount = unexpiredUselessDirs.size() - maxUselessDirCount;
        for (size_t i = 0; i < toRemoveDirCount; i++) {
            toRemoveUnExpiredSubDirs.push_back(unexpiredUselessDirs[i].first);
        }
    }
}

void AgentServiceImpl::removeTargetSubDirs(const std::string& rootDir, std::vector<std::string>& toRemoveExpireSubDirs,
                                           std::vector<std::string>& toRemoveUnExpireSubDirs)
{
    for (auto& dir : toRemoveExpireSubDirs) {
        string toRemoveDirPath = fslib::util::FileUtil::joinFilePath(rootDir, dir);
        AUTIL_LOG(INFO, "remove useless dir [%s] for over expired time [%ld]", toRemoveDirPath.c_str(),
                  AGENT_USELESS_DIR_EXPIRE_TIME_IN_SEC);
        if (!fslib::util::FileUtil::remove(toRemoveDirPath)) {
            AUTIL_LOG(ERROR, "remove dir [%s] failed", toRemoveDirPath.c_str());
        }
    }
    for (auto& dir : toRemoveUnExpireSubDirs) {
        string toRemoveDirPath = fslib::util::FileUtil::joinFilePath(rootDir, dir);
        AUTIL_LOG(INFO, "remove useless dir [%s] for over max keep count [%lu]", toRemoveDirPath.c_str(),
                  AGENT_MAX_KEEP_USELESS_DIR_COUNT);
        if (!fslib::util::FileUtil::remove(toRemoveDirPath)) {
            AUTIL_LOG(ERROR, "remove dir [%s] failed", toRemoveDirPath.c_str());
        }
    }
}

void AgentServiceImpl::updateWorkerStatus()
{
    pid_t pid = waitpid(-1, NULL, WNOHANG);
    if (pid > 0) {
        AUTIL_LOG(INFO, "waitpid return[%d]", pid);
    }
    AUTIL_LOG(INFO, "begin update role status");
    autil::ScopedLock lock(_processStatusLock);
    bool pidChange = false;
    for (auto iter = _processStatus.begin(); iter != _processStatus.end();) {
        auto pid = iter->second;
        auto targetIter = _targetProcessPlan.find(iter->first);
        if (targetIter == _targetProcessPlan.end()) {
            // should stop
            if (stopProcess(pid) || !existProcess(pid)) {
                pidChange = true;
                AUTIL_LOG(INFO, "stop useless worker[%s], procName [%s], pid[%d] success", iter->first.first.c_str(),
                          iter->first.second.c_str(), pid);
                iter = _processStatus.erase(iter);
            } else {
                AUTIL_LOG(ERROR, "stop worker[%s], procName [%s], pid[%d] failed", iter->first.first.c_str(),
                          iter->first.second.c_str(), pid);
                ++iter;
            }
            continue;
        }
        if (existProcess(pid)) {
            ++iter;
            continue;
        }
        pidChange = true;
        AUTIL_LOG(INFO, "process [%d] for worker [%s] procName [%s] is dead unexpectedly.", pid,
                  iter->first.first.c_str(), iter->first.second.c_str());
        INCREASE_QPS(_deadProcessQps);
        iter = _processStatus.erase(iter);
    }

    for (auto iter = _targetProcessPlan.begin(); iter != _targetProcessPlan.end(); iter++) {
        if (_processStatus.find(iter->first) != _processStatus.end()) {
            continue;
        }

        AUTIL_LOG(INFO, "find dead role [%s], procName [%s], try to start it.", iter->first.first.c_str(),
                  iter->first.second.c_str());
        _totalRestartCnt++;
        _latestRestartTimestamp = autil::TimeUtility::currentTimeInSeconds();
        REPORT_METRIC(_totalRestartCntMetric, _totalRestartCnt);
        pid_t pid = startProcess(iter->first.first, iter->first.second, iter->second.cmd, iter->second.configPath,
                                 iter->second.envs);
        if (pid < 0) {
            AUTIL_LOG(ERROR, "try to start worker[%s], procName[%s] failed", iter->first.first.c_str(),
                      iter->first.second.c_str());
            INCREASE_QPS(_startProcessFailQps);
            continue;
        }
        pidChange = true;
        _processStatus[iter->first] = pid;
    }

    if (pidChange) {
        updatePidFile();
    }
    auto currentTimeInSec = autil::TimeUtility::currentTimeInSeconds();
    if (_latestRestartTimestamp > 0 &&
        (currentTimeInSec - _latestRestartTimestamp) > AGENT_INNER_RESTART_COUNT_RESET_INTERVAL) {
        AUTIL_LOG(INFO, "reset total restart count, latest restart timestamp [%ld]", _latestRestartTimestamp);
        _totalRestartCnt = 0;
        _latestRestartTimestamp = -1;
        REPORT_METRIC(_totalRestartCntMetric, _totalRestartCnt);
    }
}

void AgentServiceImpl::updatePidFile()
{
    string pidFile = fslib::util::FileUtil::joinFilePath(_workDir, "pids");
    std::vector<ProcessData> pidDataVec;
    for (auto iter = _processStatus.begin(); iter != _processStatus.end(); ++iter) {
        pidDataVec.emplace_back(ProcessData::make(iter->first.first, iter->first.second, iter->second));
    }

    std::string pidContent = autil::legacy::ToJsonString(pidDataVec, true);
    REPORT_METRIC(_subProcessCntMetric, _processStatus.size());
    AUTIL_LOG(INFO, "write pid file[%s:%s]", pidFile.c_str(), pidContent.c_str());
    auto errorCode = fslib::fs::FileSystem::writeFile(pidFile, pidContent);
    if (fslib::EC_OK != errorCode) {
        AUTIL_LOG(ERROR, "write pid file[%s:%s] fail, error[%d]", pidFile.c_str(), pidContent.c_str(), errorCode);
    }
}

void AgentServiceImpl::reuseCurrentProc(bool& bPidChanged)
{
    string originPidFile = fslib::util::FileUtil::joinFilePath(_workDir, "pids");
    string originPidContent;
    auto error = fslib::fs::FileSystem::readFile(originPidFile, originPidContent);
    if (fslib::EC_NOENT == error) {
        return;
    }
    if (fslib::EC_OK != error) {
        AUTIL_LOG(ERROR, "read file[%s] fail, error[%d]", originPidFile.c_str(), error);
        return;
    }
    AUTIL_LOG(INFO, "pids[%s] content[%s]", originPidFile.c_str(), originPidContent.c_str());

    bool invalidFormat = false;
    std::vector<ProcessData> pidDataVec;
    try {
        autil::legacy::FromJsonString(pidDataVec, originPidContent);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "From json string failed, content[%s], exception[%s]", originPidContent.c_str(), e.what());
        invalidFormat = true;
    } catch (...) {
        AUTIL_LOG(ERROR, "From json string failed, content[%s]", originPidContent.c_str());
        invalidFormat = true;
    }

    if (invalidFormat && !parseLegacyPidFile(originPidContent, pidDataVec)) {
        return;
    }

    for (const auto& pidData : pidDataVec) {
        if (!existProcess(pidData.pid)) {
            continue;
        }
        auto key = std::make_pair(pidData.roleName, pidData.procName);
        auto iter = _processStatus.find(key);
        if (iter == _processStatus.end()) {
            _processStatus[key] = pidData.pid;
            bPidChanged = true;
            AUTIL_LOG(INFO, "find exist role [%s] procName [%s]", pidData.roleName.c_str(), pidData.procName.c_str());
            continue;
        }
        if (iter->second == pidData.pid) {
            continue;
        }
        bPidChanged = true;
        if (existProcess(iter->second)) {
            AUTIL_LOG(WARN, "stop duplicated legacy role [%s], pid [%d]", pidData.roleName.c_str(), pidData.pid);
            stopProcess(pidData.pid);
        } else {
            AUTIL_LOG(INFO, "use exist role [%s]", pidData.roleName.c_str());
            iter->second = pidData.pid;
        }
    }
}

void AgentServiceImpl::getCurrentState(std::string& state)
{
    ScopedLock lock(_lock);
    fillProtocolVersion(_current);
    fillCpuSpeed(_current);
    fillNetworkTraffic(_current);
    saveCurrent(_current, state);
}

bool AgentServiceImpl::hasFatalError()
{
    ScopedLock lock(_lock);
    return _hasFatalError || _totalRestartCnt > AGENT_INNER_RESTART_COUNT_THRESHOLD;
}

std::string AgentServiceImpl::getBinaryPath() const
{
    // for SimpleMasterSchedulerLocal
    auto binaryPath = autil::EnvUtil::getEnv("BINARY_PATH_FOR_BS_LOCAL");
    if (!binaryPath.empty()) {
        return binaryPath;
    }
    char buffer[2048];
    int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
    return fslib::util::FileUtil::getParentDir(std::string(buffer, ret));
}

std::string AgentServiceImpl::getRoleWorkDir(const std::string& roleName, const std::string& procName) const
{
    string reletivePath = "target_roles/" + roleName + "/" + procName;
    return fslib::util::FileUtil::joinFilePath(_workDir, reletivePath);
}

bool AgentServiceImpl::createWorkDir(const std::string& dir)
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(dir);
    if (fslib::EC_TRUE == ec) {
        return true;
    }

    if (fslib::EC_FALSE == ec) {
        ec = fslib::fs::FileSystem::mkDir(dir, true);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "create dir failed. dir:[%s], error:[%s]", dir.c_str(),
                      fslib::fs::FileSystem::getErrorString(ec).c_str());
            return false;
        }
        return true;
    }
    return false;
}

void AgentServiceImpl::prepareBinarySymLink(const std::string& workDir)
{
    auto targetBinaryPath = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::parentPath(workDir), "binary");
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(targetBinaryPath);
    if (fslib::EC_TRUE == ec && fslib::util::FileUtil::isLink(targetBinaryPath)) {
        if (!fslib::util::FileUtil::remove(targetBinaryPath)) {
            AUTIL_LOG(ERROR, "remove link dir [%s] failed", targetBinaryPath.c_str());
            return;
        }
    }

    auto srcBinaryPath = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::parentPath(_workDir), "binary");
    ec = fslib::fs::FileSystem::isExist(srcBinaryPath);
    if (fslib::EC_TRUE != ec) {
        AUTIL_LOG(WARN, "src binary path [%s] not exist, ignore prepare sym-link.", srcBinaryPath.c_str());
        return;
    }
    if (fslib::util::FileUtil::isLink(srcBinaryPath)) {
        string linkPath = fslib::util::FileUtil::readLink(srcBinaryPath);
        if (!linkPath.empty()) {
            srcBinaryPath = linkPath;
        }
    }
    if (indexlib::file_system::FslibWrapper::SymLink(srcBinaryPath, targetBinaryPath) !=
        indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(ERROR, "create sym-link from binary path [%s] to path [%s] fail.", srcBinaryPath.c_str(),
                  targetBinaryPath.c_str());
        return;
    }
    AUTIL_LOG(INFO, "create sym-link from binary path [%s] to path [%s] success.", srcBinaryPath.c_str(),
              targetBinaryPath.c_str());
}

void AgentServiceImpl::prepareConfigSymLink(const std::string& workDir, const std::string& configPath)
{
    if (configPath.empty()) {
        AUTIL_LOG(INFO, "no need prepare sym-link for config path");
        return;
    }

    auto srcConfigRoot = fslib::util::FileUtil::joinFilePath(fslib::util::FileUtil::joinFilePath(_workDir, "config"),
                                                             getLocalConfigDirName(configPath));
    auto srcConfigPath = common::ConfigDownloader::getLocalConfigPath(configPath, srcConfigRoot);
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(srcConfigPath);
    if (fslib::EC_TRUE != ec) {
        AUTIL_LOG(WARN, "latest config path [%s] not exist, ignore prepare sym-link.", srcConfigPath.c_str());
        return;
    }

    auto targetConfigRoot = fslib::util::FileUtil::joinFilePath(workDir, "config");
    ec = fslib::fs::FileSystem::isExist(targetConfigRoot);
    if (fslib::EC_FALSE == ec) {
        ec = fslib::fs::FileSystem::mkDir(targetConfigRoot, true);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "create dir failed. dir:[%s], error:[%s]", targetConfigRoot.c_str(),
                      fslib::fs::FileSystem::getErrorString(ec).c_str());
            return;
        }
    }

    auto targetConfigPath = common::ConfigDownloader::getLocalConfigPath(configPath, targetConfigRoot);
    ec = fslib::fs::FileSystem::isExist(targetConfigPath);
    if (fslib::EC_TRUE == ec) {
        AUTIL_LOG(INFO, "target config path [%s] already exist, ignore prepare sym-link.", targetConfigPath.c_str());
        return;
    }
    if (indexlib::file_system::FslibWrapper::SymLink(srcConfigPath, targetConfigPath) !=
        indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(ERROR, "create sym-link from config path [%s] to path [%s] fail.", srcConfigPath.c_str(),
                  targetConfigPath.c_str());
        return;
    }
    AUTIL_LOG(INFO, "create sym-link from config path [%s] to path [%s] success.", srcConfigPath.c_str(),
              targetConfigPath.c_str());
}

std::vector<std::string> AgentServiceImpl::setEnvForSubProcess(const std::string& workDir,
                                                               const std::vector<hippo::PairType>& envs)
{
    std::map<std::string, std::string> targetEnvMap = _globalEnvironMap;
    autil::EnvUtil::setEnv("HIPPO_PROC_WORKDIR", workDir, true);
    AUTIL_LOG(INFO, "set env [HIPPO_PROC_WORKDIR] = %s", workDir.c_str());
    targetEnvMap["HIPPO_PROC_WORKDIR"] = workDir;
    for (auto& env : envs) {
        auto normalizeEnvValue = normalizeEnvArguments(targetEnvMap, env.second);
        auto rValue = autil::EnvUtil::envReplace(normalizeEnvValue);
        autil::EnvUtil::setEnv(env.first, rValue);
        AUTIL_LOG(INFO, "set env [%s] = %s", env.first.c_str(), rValue.c_str());
        targetEnvMap[env.first] = rValue;
    }
    std::vector<std::string> envStrVec;
    for (auto& item : targetEnvMap) {
        envStrVec.push_back(item.first + "=" + item.second);
    }
    return envStrVec;
}

bool AgentServiceImpl::createProcessPlan(const proto::AgentTarget& target,
                                         std::map<ProcessKey, ProcessInfo>& plan) const
{
    map<std::string, std::string> targetRoleConfigMap;
    map<std::string, std::vector<hippo::ProcessInfo>> targetRoleInfoMap;
    for (size_t i = 0; i < target.targetroles_size(); i++) {
        const string& roleName = target.targetroles(i).rolename();
        const string& procInfoStr = target.targetroles(i).processinfo();
        string configPath;
        if (target.has_configpath()) {
            configPath = target.configpath();
        }
        if (target.targetroles(i).has_configpath()) {
            configPath = target.targetroles(i).configpath();
        }
        if (!configPath.empty()) {
            targetRoleConfigMap[roleName] = configPath;
        }
        std::vector<hippo::ProcessInfo>& procInfos = targetRoleInfoMap[roleName];
        try {
            autil::legacy::FromJsonString(procInfos, procInfoStr);
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "From json string failed, content[%s], exception[%s]", procInfoStr.c_str(), e.what());
            return false;
        } catch (...) {
            AUTIL_LOG(ERROR, "From json string failed, content[%s]", procInfoStr.c_str());
            return false;
        }
    }

    assert(plan.empty());
    set<string> keys = {"-z", "-r", "-s", "-m", "-a"};
    for (const auto& item : targetRoleInfoMap) {
        const string& roleName = item.first;
        const auto& processInfos = item.second;
        auto cIter = targetRoleConfigMap.find(roleName);
        for (size_t i = 0; i < processInfos.size(); i++) {
            const hippo::ProcessInfo& processInfo = processInfos[i];
            map<string, string> newArgs;
            if (processInfo.name == "build_service_worker") {
                newArgs = _procArgMap;
                for (size_t j = 0; j < processInfo.args.size(); j++) {
                    if (keys.find(processInfo.args[j].first) != keys.end()) {
                        newArgs[processInfo.args[j].first] = processInfo.args[j].second;
                    }
                }
                newArgs["-w"] = getRoleWorkDir(roleName, processInfo.name);
            } else {
                for (size_t j = 0; j < processInfo.args.size(); j++) {
                    newArgs[processInfo.args[j].first] = processInfo.args[j].second;
                }
            }
            string args;
            for (auto& item : newArgs) {
                args += " ";
                args += item.first;
                args += " ";
                args += item.second;
            }
            auto key = std::make_pair(roleName, processInfo.name);
            ProcessInfo pInfo;
            pInfo.cmd = _binaryPath + "/" + processInfo.cmd + args;
            pInfo.envs = processInfo.envs;
            if (cIter != targetRoleConfigMap.end()) {
                pInfo.configPath = cIter->second;
            }
            auto iter = plan.find(key);
            if (iter != plan.end()) {
                AUTIL_LOG(ERROR,
                          "Duplicated procName [%s] for role [%s] is found, "
                          "will ignore current process [%s]",
                          processInfo.name.c_str(), roleName.c_str(),
                          autil::legacy::ToJsonString(processInfo, true).c_str());
                continue;
            }
            plan[key] = pInfo;
        }
    }
    return true;
}

bool AgentServiceImpl::parseLegacyPidFile(const std::string& content, std::vector<ProcessData>& pidDatas)
{
    const vector<string>& originPids = autil::StringUtil::split(content, ",");
    for (const string& pidStr : originPids) {
        int pid;
        if (!autil::StringUtil::fromString(pidStr, pid)) {
            return false;
        }
        if (getpid() == pid) {
            continue;
        }
        if (!existProcess(pid)) {
            continue;
        }
        string srcFile = "/proc/" + pidStr + "/cmdline";
        string processCmd;
        auto error = fslib::fs::FileSystem::readFile(srcFile, processCmd);
        if (fslib::EC_OK != error) {
            continue;
        }
        string roleName;
        AgentServiceImpl::parseCmdline(processCmd, roleName);
        if (roleName.empty()) {
            continue;
        }
        pidDatas.emplace_back(ProcessData::make(roleName, "build_service_worker", pid));
    }
    return true;
}

void AgentServiceImpl::parseCmdline(std::string& cmdline, std::string& roleName)
{
    for (int i = 0; i < cmdline.size(); i++) {
        if ('\0' == cmdline[i]) {
            cmdline[i] = ' ';
        }
    }
    const vector<string>& cmdVectors = autil::StringUtil::split(cmdline, " ");
    for (int i = 0; i < cmdVectors.size(); i++) {
        if (cmdVectors[i] == "-r") {
            if (i + 1 < cmdVectors.size()) {
                roleName = cmdVectors[++i]; // increase i here
                break;
            }
        }
    }
}

void AgentServiceImpl::prepareGlobalEnvironMap()
{
    string srcFile = "/proc/" + autil::StringUtil::toString(getpid()) + "/environ";
    string processEnv;
    fslib::fs::FileSystem::readFile(srcFile, processEnv);
    for (int i = 0; i < processEnv.size(); i++) {
        if ('\0' == processEnv[i]) {
            processEnv[i] = '\t';
        }
    }

    const vector<string>& envStrVec = autil::StringUtil::split(processEnv, "\t");
    for (auto envStr : envStrVec) {
        auto pos = envStr.find("=");
        if (pos == string::npos) {
            AUTIL_LOG(ERROR, "invalid env str [%s]", envStr.c_str());
            continue;
        }
        _globalEnvironMap[envStr.substr(0, pos)] = envStr.substr(pos + 1);
    }
}

char** AgentServiceImpl::makeArgsList(const std::vector<std::string>& args)
{
    if (args.empty()) {
        return nullptr;
    }
    size_t listSize = args.size();
    char** res = new char*[listSize + 1];
    for (size_t i = 0; i < listSize; ++i) {
        size_t strSize = args[i].size();
        res[i] = new char[strSize + 1];
        strncpy(res[i], args[i].data(), strSize);
        res[i][strSize] = '\0';
    }
    res[listSize] = nullptr;
    return res;
}

std::string AgentServiceImpl::normalizeEnvArguments(const std::map<std::string, std::string>& envMap,
                                                    const std::string& rawString)
{
    if (rawString.find("$") == std::string::npos) {
        return rawString;
    }

    std::string normalizeStr = rawString;
    // use reverse iterator to avoid prefix match { HIPPO_APP & HIPPO_APP_INST_ROOT }
    for (auto iter = envMap.rbegin(); iter != envMap.rend(); iter++) {
        std::string oldStr = "$" + iter->first;
        std::string newStr = "${" + iter->first + "}";
        autil::StringUtil::replaceAll(normalizeStr, oldStr, newStr);
    }
    if (rawString != normalizeStr) {
        AUTIL_LOG(INFO, "normalize env string from [%s] to [%s]", rawString.c_str(), normalizeStr.c_str());
    }
    return normalizeStr;
}

std::string AgentServiceImpl::getLocalConfigDirName(const std::string& configPath)
{
    uint32_t configPathCrc = autil::CRC32C::Value(configPath.c_str(), configPath.size());
    return autil::StringUtil::toString(configPathCrc);
}

}} // namespace build_service::worker
