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

#include "aios/network/anet/atomic.h"
#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/common/Spec.h"
#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "autil/LoopThread.h"

namespace multi_call {

class ServerTopoMap;
class HeartbeatServerStream;
class HeartbeatServerStreamQueue;

class HeartbeatServerManager
{
public:
    HeartbeatServerManager();
    ~HeartbeatServerManager();

private:
    HeartbeatServerManager(const HeartbeatServerManager &);
    HeartbeatServerManager &operator=(const HeartbeatServerManager &);

public:
    bool init(const GigAgentPtr &agent, const Spec &spec);
    void stop();
    bool updateSpec(const Spec &spec);
    bool publish(const ServerBizTopoInfo &info, SignatureTy &signature);
    bool publish(const std::vector<ServerBizTopoInfo> &infoVec,
                 std::vector<SignatureTy> &signatureVec);
    bool publishGroup(PublishGroupTy group, const std::vector<ServerBizTopoInfo> &infoVec,
                      std::vector<SignatureTy> &signatureVec);
    bool updateVolatileInfo(SignatureTy signature, const BizVolatileInfo &info);
    bool unpublish(SignatureTy signature);
    bool unpublish(const std::vector<SignatureTy> &signatureVec);
    void stopAllBiz();
    std::vector<ServerBizTopoInfo> getPublishInfos() const;
    std::string getTopoInfoStr() const;
    void notify();

public:
    void addStream(const std::shared_ptr<HeartbeatServerStream> &stream);
    int64_t getNewId();
    const ServerTopoMapPtr &getServerTopoMap() const;

private:
    bool initLogThread(const std::string &logPrefix);
    void logThread();
    bool createTopoMap(const ServerBizTopoInfo &info, SignatureTy &signature,
                       BizTopoMapPtr &topoMapPtr);
    bool createTopoMap(const std::vector<ServerBizTopoInfo> &infoVec,
                       std::vector<SignatureTy> &signatureVec, BizTopoMapPtr &topoMapPtr);
    bool createReplaceTopoMap(PublishGroupTy group, const std::vector<ServerBizTopoInfo> &infoVec,
                              std::vector<SignatureTy> &signatureVec, BizTopoMapPtr &topoMapPtr);
    BizTopoPtr createBizTopo(PublishGroupTy group, const ServerBizTopoInfo &info);
    std::shared_ptr<BizStat> getBizStat(const std::string &bizName, PartIdTy partId);
    const std::shared_ptr<HeartbeatServerStreamQueue> &getQueue();
    void clearBiz();

private:
    static constexpr size_t QUEUE_COUNT = 5;

private:
    atomic64_t _idCounter;
    atomic64_t _nextQueueId;
    GigAgentPtr _agent;
    std::shared_ptr<ServerTopoMap> _serverTopoMap;
    std::vector<std::shared_ptr<HeartbeatServerStreamQueue>> _streamQueues;
    std::string _logPrefix;
    autil::LoopThreadPtr _logThread;
    std::string _snapshotStr;
    mutable autil::ThreadMutex _publishLock;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatServerManager);

} // namespace multi_call
