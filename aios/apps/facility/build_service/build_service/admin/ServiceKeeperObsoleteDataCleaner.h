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

#include <map>
#include <memory>
#include <queue>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class ServiceKeeper;

// BizGenerationsMap: pair<appName, dataTable> ==> vector<generationKeeper>
typedef std::map<std::pair<std::string, std::string>, std::vector<GenerationKeeperPtr>> BizGenerationsMap;

class ServiceKeeperObsoleteDataCleaner
{
public:
    ServiceKeeperObsoleteDataCleaner(ServiceKeeper* keeper);
    ~ServiceKeeperObsoleteDataCleaner();

private:
    ServiceKeeperObsoleteDataCleaner(const ServiceKeeperObsoleteDataCleaner&);
    ServiceKeeperObsoleteDataCleaner& operator=(const ServiceKeeperObsoleteDataCleaner&);

public:
    bool start();
    bool cleanVersions(const proto::BuildId& buildId, const std::string& clusterName, versionid_t version);

private:
    void cleanLoop();
    void cleanObsoleteIndex(int64_t currentTime) const;
    void clearStoppedGenerations(int64_t currentTime);
    void clearOneStoppedGeneration(const proto::BuildId& buildId, bool isReserved);
    void getParamsFromEnv();
    void cleanVersionsLoop();
    void clearStoppedGenerationInfo(const proto::BuildId& buildId);
    bool needDeleteGenerationInfo(const std::vector<GenerationKeeperPtr>& keepers, size_t idx, int64_t currentTime);
    bool needDeleteOldIndex(const std::vector<GenerationKeeperPtr>& keepers, size_t idx, int64_t currentTime) const;

private:
    struct GenerationCompare {
        bool operator()(GenerationKeeperPtr k1, GenerationKeeperPtr k2)
        {
            return (k1->getStopTimeStamp() < k2->getStopTimeStamp()) ||
                   (k1->getStopTimeStamp() == k2->getStopTimeStamp() &&
                    k1->getBuildId().generationid() < k2->getBuildId().generationid());
        }
    };

private:
    ServiceKeeper* _keeper;
    autil::LoopThreadPtr _cleanLoopThread;

private:
    int64_t _defaultCleanInterval;
    uint32_t _reserveGenerationCount;
    uint32_t _reserveIndexGenerationCount;
    typedef std::queue<std::tuple<GenerationKeeperPtr, std::string, versionid_t>> CleanVersionsQueue;
    CleanVersionsQueue _cleanVersionsQueue;
    autil::ThreadMutex _cleanVersionsQueueLock;
    autil::LoopThreadPtr _cleanVersionsThread;
    bool _deleteOldIndex;
    int64_t _stoppedGenerationKeepTime;

private:
    static const int64_t DEFAULT_CLEAN_INTERVAL;
    static const int64_t DEFAULT_CLEAN_VERSIONS_INTERVAL;
    static const uint32_t RESERVE_GENERATION_COUNT;
    static const uint32_t RESERVE_INDEX_GENERATION_COUNT;
    static const int64_t DEFAULT_STOPPED_GENERATION_KEEP_TIME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ServiceKeeperObsoleteDataCleaner);

}} // namespace build_service::admin
