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
#ifndef ISEARCH_BS_HEARTBEAT_H
#define ISEARCH_BS_HEARTBEAT_H

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
namespace build_service { namespace admin {

class Heartbeat
{
private:
    struct Status {
        std::string current;
        std::string target;
        std::string targetResources;
        std::string usingResources;
        std::string targetIdentifier;
        std::string currentIdentifier;
        int64_t statusUpdateTime = -1;
        int64_t workerStartTime = -1;
    };
    typedef std::map<proto::PartitionId, Status> HeartbeatCache;

public:
    Heartbeat();
    virtual ~Heartbeat();

private:
    Heartbeat(const Heartbeat&);
    Heartbeat& operator=(const Heartbeat&);

public:
    bool start(int64_t interval);
    // update active nodes, sync current status to node, sync target status to cache.
    void syncNodesStatus(const proto::WorkerNodes& nodes);
    void stop();
    void setUsingResources(const proto::PartitionId& partitionId, const std::string& resources);

protected:
    // sync heartbeat to cache status, never update map node(key)!!!
    virtual void syncStatus() = 0;

protected:
    // for child class
    void setCurrentStatus(const proto::PartitionId& partitionId, const std::string& currentStatus,
                          const std::string& currentIdentifier, int64_t startTimestamp);
    void getTargetStatus(const proto::PartitionId& partitionId, std::string& target, std::string& requiredIdentifier);
    const std::string& getDependResource(const proto::PartitionId& partitionId);
    std::vector<proto::PartitionId> getAllPartIds() const;

public:
    // for test
    void setTargetStatus(const proto::PartitionId& partitionId, const std::string& targetStatus);
    std::string getCurrentStatus(const proto::PartitionId& partitionId);

private:
    mutable autil::ThreadMutex _mutex;
    HeartbeatCache _heartbeatCache;
    autil::LoopThreadPtr _loopThreadPtr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Heartbeat);

}} // namespace build_service::admin

#endif // ISEARCH_BS_HEARTBEAT_H
