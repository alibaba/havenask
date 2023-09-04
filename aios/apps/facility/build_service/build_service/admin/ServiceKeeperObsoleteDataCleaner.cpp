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
#include "build_service/admin/ServiceKeeperObsoleteDataCleaner.h"

#include "autil/EnvUtil.h"
#include "build_service/admin/ServiceKeeper.h"
#include "build_service/common/PathDefine.h"
#include "fslib/util/FileUtil.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
using namespace autil;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ServiceKeeperObsoleteDataCleaner);

#ifdef NDEBUG
const int64_t ServiceKeeperObsoleteDataCleaner::DEFAULT_CLEAN_INTERVAL = 60 * 60; // 1h
#else
const int64_t ServiceKeeperObsoleteDataCleaner::DEFAULT_CLEAN_INTERVAL = 60; // 1 min
#endif
const int64_t ServiceKeeperObsoleteDataCleaner::DEFAULT_CLEAN_VERSIONS_INTERVAL = 60; // 1min
const int64_t ServiceKeeperObsoleteDataCleaner::DEFAULT_STOPPED_GENERATION_KEEP_TIME =
    60 * 60 * 24 * 365 * 10; // 10 year
const uint32_t ServiceKeeperObsoleteDataCleaner::RESERVE_GENERATION_COUNT = 10;
const uint32_t ServiceKeeperObsoleteDataCleaner::RESERVE_INDEX_GENERATION_COUNT = 8;

ServiceKeeperObsoleteDataCleaner::ServiceKeeperObsoleteDataCleaner(ServiceKeeper* keeper)
    : _keeper(keeper)
    , _defaultCleanInterval(DEFAULT_CLEAN_INTERVAL)
    , _reserveGenerationCount(RESERVE_GENERATION_COUNT)
    , _reserveIndexGenerationCount(RESERVE_INDEX_GENERATION_COUNT)
    , _deleteOldIndex(true)
    , _stoppedGenerationKeepTime(DEFAULT_STOPPED_GENERATION_KEEP_TIME)
{
}

ServiceKeeperObsoleteDataCleaner::~ServiceKeeperObsoleteDataCleaner() {}

bool ServiceKeeperObsoleteDataCleaner::start()
{
    getParamsFromEnv();
    _cleanLoopThread = LoopThread::createLoopThread(bind(&ServiceKeeperObsoleteDataCleaner::cleanLoop, this),
                                                    _defaultCleanInterval * 1000 * 1000);
    if (!_cleanLoopThread) {
        string errorMsg = "create clean loop thread failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    _cleanVersionsThread =
        LoopThread::createLoopThread(bind(&ServiceKeeperObsoleteDataCleaner::cleanVersionsLoop, this),
                                     DEFAULT_CLEAN_VERSIONS_INTERVAL * 1000 * 1000);
    if (!_cleanVersionsThread) {
        string errorMsg = "create clean versions loop thread failed!";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

void ServiceKeeperObsoleteDataCleaner::getParamsFromEnv()
{
    _reserveGenerationCount = EnvUtil::getEnv("service_reserve_old_generation_count", RESERVE_GENERATION_COUNT);
    _reserveIndexGenerationCount = EnvUtil::getEnv("service_reserve_old_index_count", RESERVE_INDEX_GENERATION_COUNT);
    _deleteOldIndex = EnvUtil::getEnv("need_delete_old_index", true);
    if (!_deleteOldIndex) {
        _reserveIndexGenerationCount = numeric_limits<uint32_t>::max();
    }
    if (_deleteOldIndex) {
        _reserveGenerationCount = max(_reserveGenerationCount, _reserveIndexGenerationCount + 1);
    }
    _defaultCleanInterval = EnvUtil::getEnv("service_clean_interval", DEFAULT_CLEAN_INTERVAL);
    _stoppedGenerationKeepTime = EnvUtil::getEnv("stopped_generation_keep_time", DEFAULT_STOPPED_GENERATION_KEEP_TIME);

    BS_LOG(INFO, "actual reserved stopped index count: %ud", _reserveIndexGenerationCount);
    BS_LOG(INFO, "actual reserved generation count: %ud", _reserveGenerationCount);
    BS_LOG(INFO, "stopped generation keep time: %ld", _stoppedGenerationKeepTime);
    BS_LOG(INFO, "generation clean interval: %ld", _defaultCleanInterval);
}

void ServiceKeeperObsoleteDataCleaner::cleanLoop()
{
    BS_LOG(INFO, "begin clean stop generation");
    int64_t currentTime = TimeUtility::currentTimeInSeconds();
    cleanObsoleteIndex(currentTime);
    clearStoppedGenerations(currentTime);
    BS_LOG(INFO, "end clean stop generation");
}

bool ServiceKeeperObsoleteDataCleaner::needDeleteGenerationInfo(const vector<GenerationKeeperPtr>& keepers, size_t idx,
                                                                int64_t currentTime)
{
    if (idx + _reserveGenerationCount < keepers.size()) {
        return true;
    }

    if (currentTime - keepers[idx]->getStopTimeStamp() > _stoppedGenerationKeepTime) {
        string buildId = keepers[idx]->getBuildId().ShortDebugString();
        BS_LOG(INFO, "delete obsolete generation [%s], stop time [%ld], current time [%ld]", buildId.c_str(),
               keepers[idx]->getStopTimeStamp(), currentTime);
        return true;
    }
    return false;
}

void ServiceKeeperObsoleteDataCleaner::clearStoppedGenerations(int64_t currentTime)
{
    FSLIB_LONG_INTERVAL_LOG("clean stopped generation");
    BizGenerationsMap generations;
    _keeper->getStoppedGenerations(generations);
    for (auto it = generations.begin(); it != generations.end(); ++it) {
        vector<GenerationKeeperPtr>& keeperVec = it->second;
        sort(keeperVec.begin(), keeperVec.end(), GenerationCompare());
        for (size_t i = 0; i < keeperVec.size(); i++) {
            bool isReserved = !needDeleteGenerationInfo(keeperVec, i, currentTime);
            clearOneStoppedGeneration(keeperVec[i]->getBuildId(), isReserved);
        }
    }
}

void ServiceKeeperObsoleteDataCleaner::clearStoppedGenerationInfo(const BuildId& buildId)
{
    bool isRecoverFailed = false;
    const GenerationKeeperPtr& gKeeper = _keeper->getGeneration(buildId, true, isRecoverFailed);
    if (isRecoverFailed) {
        BS_LOG(WARN, "generation recover failed, can not stop buildId:  %s.", buildId.ShortDebugString().c_str());
        return;
    }
    if (gKeeper) {
        gKeeper->clearCounters();
    }

    string zkPath = PathDefine::getGenerationZkRoot(_keeper->getZkRoot(), buildId);
    if (!gKeeper->isIndexDeleted() && _deleteOldIndex) {
        BS_LOG(WARN, "index has not been deleted, can not remove generation dir %s.", zkPath.c_str());
        return;
    }
    if (gKeeper && !gKeeper->deleteTmpBuilderIndex()) {
        BS_LOG(WARN, "clean tmp builder generation dir %s failed.", zkPath.c_str());
        return;
    }
    if (!fslib::util::FileUtil::remove(zkPath)) {
        BS_LOG(WARN, "clean old generation dir %s failed.", zkPath.c_str());
        return;
    }
    _keeper->removeStoppedGeneration(buildId);
    BS_LOG(INFO, "clean old generation dir %s success", zkPath.c_str());
}

void ServiceKeeperObsoleteDataCleaner::clearOneStoppedGeneration(const BuildId& buildId, bool isReserved)
{
    string zkPath = PathDefine::getGenerationZkRoot(_keeper->getZkRoot(), buildId);
    if (!isReserved) {
        clearStoppedGenerationInfo(buildId);
    } else {
        vector<string> fileList;
        if (!fslib::util::FileUtil::listDir(zkPath, fileList)) {
            BS_LOG(WARN, "list generation dir %s failed.", zkPath.c_str());
            return;
        }
        for (size_t i = 0; i < fileList.size(); ++i) {
            if (fileList[i].find(PathDefine::ZK_GENERATION_STATUS_STOP_FILE) != string::npos) {
                continue;
            }
            if (fileList[i] == COUNTER_DIR_NAME) {
                continue;
            }
            fslib::util::FileUtil::remove(fslib::util::FileUtil::joinFilePath(zkPath, fileList[i]));
        }
    }
}

bool ServiceKeeperObsoleteDataCleaner::needDeleteOldIndex(const vector<GenerationKeeperPtr>& keepers, size_t idx,
                                                          int64_t currentTime) const
{
    if (!_deleteOldIndex) {
        return false;
    }

    if (idx + _reserveIndexGenerationCount < keepers.size()) {
        return true;
    }

    if (currentTime - keepers[idx]->getStopTimeStamp() > _stoppedGenerationKeepTime) {
        string buildId = keepers[idx]->getBuildId().ShortDebugString();
        BS_LOG(INFO, "delete obsolete index [%s], stop time [%ld], current time [%ld]", buildId.c_str(),
               keepers[idx]->getStopTimeStamp(), currentTime);
        return true;
    }
    return false;
}

void ServiceKeeperObsoleteDataCleaner::cleanObsoleteIndex(int64_t currentTime) const
{
    FSLIB_LONG_INTERVAL_LOG("clean index");
    BizGenerationsMap generations;
    _keeper->getStoppedGenerations(generations);
    for (auto it = generations.begin(); it != generations.end(); ++it) {
        vector<GenerationKeeperPtr>& keeperVec = it->second;
        sort(keeperVec.begin(), keeperVec.end(), GenerationCompare());
        for (size_t i = 0; i < keeperVec.size(); i++) {
            if (needDeleteOldIndex(keeperVec, i, currentTime)) {
                const BuildId& buildId = keeperVec[i]->getBuildId();
                bool isRecoverFailed = false;
                const GenerationKeeperPtr& gKeeper = _keeper->getGeneration(buildId, true, isRecoverFailed);
                if (!isRecoverFailed && !gKeeper->isIndexDeleted()) {
                    gKeeper->deleteIndex();
                }
            }
        }
    }
}

void ServiceKeeperObsoleteDataCleaner::cleanVersionsLoop()
{
    while (true) {
        _cleanVersionsQueueLock.lock();
        if (_cleanVersionsQueue.empty()) {
            _cleanVersionsQueueLock.unlock();
            return;
        }
        auto element = _cleanVersionsQueue.front();
        _cleanVersionsQueue.pop();
        _cleanVersionsQueueLock.unlock();
        const GenerationKeeperPtr& gKeeper = get<0>(element);
        const string& clusterName = get<1>(element);
        versionid_t version = get<2>(element);
        if (!gKeeper->cleanVersions(clusterName, version)) {
            BS_LOG(WARN, "clean versions for cluster[%s] version[%d] failed.", clusterName.c_str(), version);
        }
    }
}

bool ServiceKeeperObsoleteDataCleaner::cleanVersions(const proto::BuildId& buildId, const string& clusterName,
                                                     versionid_t version)
{
    bool isRecoverFailed = false;
    const GenerationKeeperPtr& gKeeper = _keeper->getGeneration(buildId, false, isRecoverFailed);
    if (gKeeper) {
        _cleanVersionsQueueLock.lock();
        _cleanVersionsQueue.push(std::make_tuple(gKeeper, clusterName, version));
        _cleanVersionsQueueLock.unlock();
        return true;
    }
    BS_LOG(ERROR, "get GenerationKeeper fail with buildId[%s] cluster[%s]", buildId.ShortDebugString().c_str(),
           clusterName.c_str());
    return false;
}

}} // namespace build_service::admin
