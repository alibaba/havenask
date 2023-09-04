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
#include "build_service/worker/WorkerStateHandler.h"

#include "build_service/common/ConfigDownloader.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/HeartbeatDefine.h"
#include "fslib/util/FileUtil.h"

using namespace std;
BS_NAMESPACE_USE(config);
BS_NAMESPACE_USE(proto);
BS_NAMESPACE_USE(util);
BS_NAMESPACE_USE(common);

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, WorkerStateHandler);

string WorkerStateHandler::downloadConfig(const string& remoteConfigPath)
{
    _downloadConfigStartTs = autil::TimeUtility::currentTime();
    string localConfigPath = staticDownloadConfig(remoteConfigPath);
    if (localConfigPath.empty()) {
        BS_LOG(ERROR, "downloadConfig from %s failed", remoteConfigPath.c_str());
        setFatalError();
    }
    _downloadConfigStartTs = 0;
    return localConfigPath;
}

string WorkerStateHandler::staticDownloadConfig(const string& remoteConfigPath)
{
    string localConfigPath;
    ConfigDownloader::DownloadErrorCode errorCode = ConfigDownloader::downloadConfig(remoteConfigPath, localConfigPath);
    if (errorCode == ConfigDownloader::DEC_NORMAL_ERROR || errorCode == ConfigDownloader::DEC_DEST_ERROR) {
        localConfigPath = "";
    }
    return localConfigPath;
}

void WorkerStateHandler::getUsingResources(string& usingResourcesStr)
{
    common::ResourceKeeperMap resources;
    doGetResourceKeeperMap(resources);
    std::map<std::string, proto::WorkerNodeBase::ResourceInfo> usingResources;
    for (auto& it : resources) {
        proto::WorkerNodeBase::ResourceInfo resourceInfo;
        resourceInfo.resourceStr = ToJsonString(it.second);
        resourceInfo.resourceId = it.second->getResourceId();
        usingResources.insert(make_pair(it.second->getResourceName(), resourceInfo));
    }
    usingResourcesStr = ToJsonString(usingResources);
}

bool WorkerStateHandler::updateTargetResourceKeeperMap(const string& targetResourcesStr)
{
    autil::ScopedLock lock(_resourceLock);
    string debugString;
    std::map<std::string, proto::WorkerNodeBase::ResourceInfo> targetResources;
    try {
        FromJsonString(targetResources, targetResourcesStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "json target resource [%s] failed", targetResourcesStr.c_str());
        return false;
    }
    for (auto iter = targetResources.begin(); iter != targetResources.end(); iter++) {
        debugString += ("[" + iter->first + " " + ToJsonString(iter->second) + "],");
    }
    BS_INTERVAL_LOG(60, INFO, "target resource status: %s", debugString.c_str());

    bool resourceChanged = false;
    if (targetResources.size() != _currentResource.size()) {
        resourceChanged = true;
    }
    if (!resourceChanged) {
        for (auto newTargetIter = targetResources.begin(); newTargetIter != targetResources.end(); newTargetIter++) {
            auto oldIter = _currentResource.find(newTargetIter->first);
            if (oldIter == _currentResource.end() || oldIter->second != newTargetIter->second) {
                resourceChanged = true;
                break;
            }
        }
    }
    if (resourceChanged) {
        _currentResource = targetResources;
    }
    return resourceChanged;
}

common::ResourceKeeperMap WorkerStateHandler::getCurrentResourceKeeperMap()
{
    autil::ScopedLock lock(_resourceLock);
    common::ResourceKeeperMap ret;
    for (auto it = _currentResource.begin(); it != _currentResource.end(); it++) {
        ret[it->first] = common::ResourceKeeper::deserializeResourceKeeper(it->second.resourceStr);
    }
    return ret;
}

bool WorkerStateHandler::readReservedClusterCheckpoints(const PartitionId& pid, string& reservedCheckpointJsonStr)
{
    string generationDir = PathDefine::getGenerationZkRoot(_appZkRoot, pid.buildid());
    string reservedCCPFilePath = PathDefine::getReservedCheckpointFileName(generationDir, pid.clusternames(0));

    bool existFlag = false;
    if (!fslib::util::FileUtil::isExist(reservedCCPFilePath, existFlag)) {
        BS_LOG(ERROR, "failed to determin existence of file [%s]", reservedCCPFilePath.c_str());
        return false;
    }
    if (!existFlag) {
        reservedCheckpointJsonStr = "";
        return true;
    }
    if (!fslib::util::FileUtil::readFile(reservedCCPFilePath, reservedCheckpointJsonStr)) {
        BS_LOG(ERROR, "failed to read content of file [%s]", reservedCCPFilePath.c_str());
        return false;
    }
    return true;
}

bool WorkerStateHandler::overWriteCounterConfig(const PartitionId& pid, CounterConfig& counterConfig)
{
    if (counterConfig.position == "zookeeper") {
        string counterFilePath;
        bool ignoreBackupId = pid.role() == ROLE_PROCESSOR ? true : false;
        if (!CounterSynchronizer::getCounterZkFilePath(_appZkRoot, pid, counterFilePath, ignoreBackupId)) {
            BS_LOG(ERROR, "rewrite CounterConfig failed");
            return false;
        }
        counterConfig.params[COUNTER_PARAM_FILEPATH] = counterFilePath;
    } else if (counterConfig.position == "redis") {
        string redisKey = CounterSynchronizer::getCounterRedisKey(_adminServiceName, pid.buildid());
        string redisField;
        if (!CounterSynchronizer::getCounterRedisHashField(pid, redisField)) {
            BS_LOG(ERROR, "rewrite CounterConfig failed");
            return false;
        }
        counterConfig.params[COUNTER_PARAM_REDIS_KEY] = redisKey;
        counterConfig.params[COUNTER_PARAM_REDIS_FIELD] = redisField;
    } else {
        BS_LOG(ERROR, "unsupported counter position [%s]", counterConfig.position.c_str());
        return false;
    }
    return true;
}

void WorkerStateHandler::setFatalError() { _hasFatalError = true; }

void WorkerStateHandler::reportMetrics(bool hasTarget)
{
    int64_t interval = autil::TimeUtility::currentTime() - _initTimestamp;
    if (!hasTarget) {
        REPORT_METRIC(_waitingTargetTime, interval);
    }
    REPORT_METRIC(_workerRunningTime, interval);

    int64_t ts = _downloadConfigStartTs;
    if (ts > 0) {
        int64_t waitTime = autil::TimeUtility::currentTime() - ts;
        REPORT_METRIC(_downLoadingConfigTime, waitTime);
    }

    if (_cpuQuota > 0) {
        int64_t ratio = 100 * _cpu.getUsage() * std::thread::hardware_concurrency() / _cpuQuota;
        REPORT_METRIC(_cpuRatioMetric, ratio);
    }

    if (_memQuota > 0) {
        _mem.update();
        int64_t ratio = 100.0 * _mem.getMemRss() / _memQuota / 1024;
        REPORT_METRIC(_memRatioMetric, ratio);
    }
}

void WorkerStateHandler::cleanUselessErrorInfos()
{
    while (_errorInfos.size() > MAX_ERROR_COUNT) {
        _errorInfos.pop_front();
    }

    // avoid redundant error infos
    sort(_errorInfos.begin(), _errorInfos.end(), [](const proto::ErrorInfo& lhs, const proto::ErrorInfo& rhs) -> bool {
        if (lhs.errortime() != rhs.errortime()) {
            return lhs.errortime() < rhs.errortime();
        }
        if (lhs.errorcode() != rhs.errorcode()) {
            return lhs.errorcode() < rhs.errorcode();
        }
        return lhs.errormsg() < rhs.errormsg();
    });
    _errorInfos.erase(unique(_errorInfos.begin(), _errorInfos.end()), _errorInfos.end());
}

}} // namespace build_service::worker
