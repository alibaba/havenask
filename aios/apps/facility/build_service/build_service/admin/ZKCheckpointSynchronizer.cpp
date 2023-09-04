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
#include "build_service/admin/ZKCheckpointSynchronizer.h"

#include <memory>

#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/admin/ZKClusterCheckpointSynchronizer.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ZKCheckpointSynchronizer);

#define CS_LOG(level, format, args...) BS_LOG(level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)
#define CS_INTERVAL_LOG(interval, level, format, args...)                                                              \
    BS_INTERVAL_LOG(interval, level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)

ZKCheckpointSynchronizer::ZKCheckpointSynchronizer(const proto::BuildId& buildId) : CheckpointSynchronizer(buildId) {}

ZKCheckpointSynchronizer::~ZKCheckpointSynchronizer() { clear(); }

void ZKCheckpointSynchronizer::clear()
{
    // make sure zk is closed first
    _zk.setCreateCallback(0);
    _zk.setDataCallback(0);
    _zk.setConnCallback(0);
    while (0 != _mutex.trylock()) {
        CS_LOG(INFO, "mutex is still holded");
        usleep(100 * 1000);
    }
    _zk.close();
    _mutex.unlock();
    CheckpointSynchronizer::clear();
}

bool ZKCheckpointSynchronizer::init(
    const std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>& clusterSynchronizers,
    bool isLeaderFollowerMode)
{
    clear();
    if (!CheckpointSynchronizer::init(clusterSynchronizers, isLeaderFollowerMode)) {
        CS_LOG(ERROR, "init failed");
        return false;
    }
    std::vector<common::PathInfo> pathInfos;
    std::map<std::string, std::string> suezVersionZkRoots;

    for (const auto& [clusterName, synchronizer] : clusterSynchronizers) {
        auto zkSynchronizer = std::dynamic_pointer_cast<ZKClusterCheckpointSynchronizer>(synchronizer);
        if (zkSynchronizer == nullptr) {
            CS_LOG(ERROR, "dynamic cast to zk cluster checkpoint synchornizer failed,  clusterName[%s]",
                   clusterName.c_str());
            return false;
        }
        std::string suezVersionZkRoot = zkSynchronizer->getSuezVersionZkRoot();
        suezVersionZkRoots[clusterName] = suezVersionZkRoot;
        const auto& rangeVec = zkSynchronizer->getRanges();
        for (const auto& range : rangeVec) {
            pathInfos.emplace_back(suezVersionZkRoot, clusterName, _buildId, range);
        }
        _metricsReporter = zkSynchronizer->getMetricsReporter();
        _checkpointAccessor = zkSynchronizer->getCheckpointAccessor();
    }
    using namespace std::placeholders;
    if (_zk.isConnected()) {
        CS_LOG(WARN, "zk is already connected");
        return true;
    }
    if (pathInfos.empty()) {
        CS_LOG(ERROR, "path infos is empty");
        return false;
    }
    std::string host = fslib::util::FileUtil::getHostFromZkPath(pathInfos[0].root);
    if (host == "") {
        CS_LOG(ERROR, "get host from zk root [%s] failed. ZKCheckpointSynchronizer init failed",
               pathInfos[0].root.c_str());
        return false;
    }
    for (const auto& pathInfo : pathInfos) {
        std::string zkHost = fslib::util::FileUtil::getHostFromZkPath(pathInfo.root);
        if (host != zkHost) {
            CS_LOG(ERROR, "zk host not match [%s] [%s]", host.c_str(), zkHost.c_str());
            return false;
        }
    }
    _zk.setCreateCallback(bind(&ZKCheckpointSynchronizer::onDataChange, this, _1, _2, _3));
    _zk.setDataCallback(bind(&ZKCheckpointSynchronizer::onDataChange, this, _1, _2, _3));
    _zk.setConnCallback(bind(&ZKCheckpointSynchronizer::connectionChangedNotify, this, _1, _2, _3));
    _zk.setParameter(host, /*timeout=*/10000); // ms
    if (!_zk.open()) {
        _zk.setCreateCallback(0);
        _zk.setDataCallback(0);
        _zk.setConnCallback(0);
        CS_LOG(ERROR, "zk[%s] open failed. ZKCheckpointSynchronizer init failed.", host.c_str());
        return false;
    }
    for (const auto& pathInfo : pathInfos) {
        _pathInfos.insert(std::pair(pathInfo.path, pathInfo));
        _clusterToPaths[pathInfo.clusterName].push_back(pathInfo.path);
        updateVersionMeta(pathInfo.path);
    }

    CS_LOG(INFO, "checkpoint synchronizer [%s] init succeed", host.c_str());
    return true;
}

bool ZKCheckpointSynchronizer::sync()
{
    if (_checkpointAccessor == nullptr) {
        CS_LOG(ERROR, "checkpoint accessor is nullptr");
        return false;
    }
    if (_zk.isBad()) {
        CS_INTERVAL_LOG(60, INFO, "connection to zookeeper is bad, try to reconnect.");
        if (!_zk.open()) {
            CS_INTERVAL_LOG(60, ERROR, "zookeeper open failed.");
            return false;
        }
        CS_INTERVAL_LOG(60, INFO, "try to establish a new connection to zookeeper.");
    }

    for (const auto& [clusterName, paths] : _clusterToPaths) {
        std::string checkpointKey =
            common::GenerationLevelCheckpoint::genGenerationLevelCheckpointKey(_buildId, clusterName,
                                                                               /*isDaily=*/false);
        std::string tmpCheckpointName;
        std::string tmpCheckpointValue;
        common::GenerationLevelCheckpoint latestCheckpoint;
        if (_checkpointAccessor->getLatestCheckpoint(checkpointKey, tmpCheckpointName, tmpCheckpointValue)) {
            if (!ClusterCheckpointSynchronizer::generationCheckpointfromStr(tmpCheckpointValue, &latestCheckpoint)) {
                return false;
            }
            auto clusterSync = getClusterSynchronizer(clusterName);
            if (clusterSync == nullptr) {
                CS_INTERVAL_LOG(60, ERROR, "sync failed, not found [%s] cluster synchronizer.", clusterName.c_str());
                return false;
            }
            if (autil::TimeUtility::currentTimeInMilliSeconds() - latestCheckpoint.createTime <
                clusterSync->getOptions().syncIntervalMs) {
                continue;
            }
        }
        for (const auto& path : paths) {
            updateVersionMeta(path);
        }
    }
    return CheckpointSynchronizer::sync();
}

bool ZKCheckpointSynchronizer::updateVersionMeta(const std::string& path)
{
    common::PathInfo pathInfo;
    if (!getPathInfo(path, &pathInfo)) {
        CS_LOG(ERROR, "get path [%s] not found", path.c_str());
        assert(false);
        return false;
    }
    // fill version meta with default value if this is no version meta.

    std::string value;
    ZOO_ERRORS error = _zk.getData(path, value, /*watch=*/true);
    if (ZOK != error) {
        if (ZNONODE == error) {
            bool exist;
            _zk.check(path, exist, /*watch=*/true);
        }
        CS_INTERVAL_LOG(600, ERROR, "get path [%s] data failed, error[%d]", path.c_str(), (int)error);
        return false;
    }
    common::VersionMetaWrapper versionMetaWrapper;
    try {
        autil::legacy::FromJsonString(versionMetaWrapper, value);
    } catch (const autil::legacy::ExceptionBase& e) {
        CS_LOG(ERROR, "version meta wrapper load failed, path[%s], value[%s], exception[%s]", path.c_str(),
               value.c_str(), e.what());
        return false;
    } catch (const std::exception& e) {
        CS_LOG(ERROR, "version meta wrapper load failed, path[%s], value[%s], exception[%s]", path.c_str(),
               value.c_str(), e.what());
        return false;
    } catch (...) {
        CS_LOG(ERROR, "version meta wrapper load failed, path[%s], value[%s], unknown exception", path.c_str(),
               value.c_str());
        return false;
    }
    const auto& versionMeta = versionMetaWrapper.getVersionMeta();
    if (versionMeta.GetVersionId() == indexlibv2::INVALID_VERSIONID) {
        CS_LOG(ERROR, "update version meta failed, path[%s], check version id is INVALID_VERSIONID", path.c_str());
        return false;
    }
    std::string errorMsg;
    std::string versionMetaStr = autil::legacy::ToJsonString(versionMeta, /*isCompact=*/true);
    if (!publishPartitionLevelCheckpoint(pathInfo.clusterName, pathInfo.range, versionMetaStr, errorMsg)) {
        CS_LOG(ERROR, "path[%s] is updated failed  versionId[%d], versionMeta[%s].", path.c_str(),
               versionMeta.GetVersionId(), versionMetaStr.c_str());
    }
    CS_LOG(INFO, "path[%s] is updated,  versionId[%d], versionMeta[%s].", path.c_str(), versionMeta.GetVersionId(),
           versionMetaStr.c_str());
    return true;
}

void ZKCheckpointSynchronizer::onDataChange(cm_basic::ZkWrapper* zk, const std::string& path,
                                            cm_basic::ZkWrapper::ZkStatus status)
{
    (void)zk;
    CS_LOG(INFO, "path [%s] data changed, status [%d]", path.c_str(), int(status));
    autil::ScopedLock lock(_mutex);
    updateVersionMeta(path);
}

void ZKCheckpointSynchronizer::connectionChangedNotify(cm_basic::ZkWrapper* zk, const std::string& path,
                                                       cm_basic::ZkWrapper::ZkStatus status)
{
    (void)zk;
    CS_LOG(INFO, "connection status changed notify, path[%s], status [%d]", path.c_str(), int(status));
    autil::ScopedLock lock(_mutex);
    if (_metricsReporter) {
        kmonitor::MetricsTags tags;
        _metricsReporter->reportSuezVersionZkConnection(int(status), tags);
    }
}

bool ZKCheckpointSynchronizer::getPathInfo(const std::string& path, common::PathInfo* pathInfo) const
{
    auto iter = _pathInfos.find(path);
    if (iter == _pathInfos.end()) {
        return false;
    }
    *pathInfo = iter->second;
    return true;
}

}} // namespace build_service::admin
