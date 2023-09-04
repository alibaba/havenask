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
#ifndef ISEARCH_BS_RPCWORKERHEARTBEAT_H
#define ISEARCH_BS_RPCWORKERHEARTBEAT_H

#include "build_service/common_define.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerHeartbeat.h"
#include "worker_framework/WorkerBase.h"

using namespace worker_framework;

namespace build_service { namespace worker {

class RpcWorkerHeartbeat : public WorkerHeartbeat, public proto::HeartbeatService
{
public:
    RpcWorkerHeartbeat(const std::string& identifier, proto::PartitionId partitionId, proto::WorkerNodeBase* workerNode,
                       WorkerBase* workerBase);
    ~RpcWorkerHeartbeat();

private:
    RpcWorkerHeartbeat(const RpcWorkerHeartbeat&);
    RpcWorkerHeartbeat& operator=(const RpcWorkerHeartbeat&);

public:
    bool start() override;
    void stop() override;
    void heartbeat(::google::protobuf::RpcController* controller, const proto::TargetRequest* request,
                   proto::CurrentResponse* response, ::google::protobuf::Closure* done) override;

public:
    bool isStarted() const;
    void setStarted(bool started);
    void setWorkerNode(proto::WorkerNodeBase* workerNode) { _workerNode = workerNode; }
    void setPid(const proto::PartitionId& pid) { _partitionId = pid; }

private:
    int64_t _startTimestamp = -1;
    std::string _identifier;
    proto::PartitionId _partitionId;
    WorkerBase* _workerBase;
    mutable autil::ThreadMutex _startedLock;
    bool _started;

private:
    static const size_t RPC_SERVICE_THREAD_NUM;
    static const size_t RPC_SERVICE_QUEUE_SIZE;
    static const std::string RPC_SERVICE_THEAD_POOL_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RpcWorkerHeartbeat);

}} // namespace build_service::worker

#endif // ISEARCH_BS_RPCWORKERHEARTBEAT_H
