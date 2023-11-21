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
#ifndef ISEARCH_MULTI_CALL_GRPCSERVERWORKER_H
#define ISEARCH_MULTI_CALL_GRPCSERVERWORKER_H

#include <grpc++/alarm.h>
#include <grpc++/generic/async_generic_service.h>
#include <grpc++/grpc++.h>
#include <grpc++/impl/codegen/byte_buffer.h>

#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/rpc/GigRpcWorker.h"
#include "aios/network/gig/multi_call/util/RandomGenerator.h"
#include "autil/LockFreeThreadPool.h"
#include "autil/Thread.h"

namespace multi_call {

typedef std::shared_ptr<grpc::ServerCompletionQueue> ServerCompletionQueuePtr;

struct ServerCompletionQueueStatus {
    ServerCompletionQueueStatus(const ServerCompletionQueuePtr &cq_) : stopped(false), cq(cq_) {
    }
    volatile bool stopped;
    autil::ReadWriteLock enqueueLock;
    ServerCompletionQueuePtr cq;
};

MULTI_CALL_TYPEDEF_PTR(ServerCompletionQueueStatus);

class GigRpcServer;

class GrpcServerWorker : public GigRpcWorker
{
public:
    GrpcServerWorker(GigRpcServer &owner);
    ~GrpcServerWorker();

public:
    bool init(const GrpcServerDescription &desc);

private:
    GrpcServerWorker(const GrpcServerWorker &);
    GrpcServerWorker &operator=(const GrpcServerWorker &);

public:
    void stop();
    int32_t getPort() const;
    const GigAgentPtr &getAgent() const;

private:
    // virtual for ut
    virtual grpc::SslServerCredentialsOptions getSslOptions(const SecureConfig &secureConfig) const;
    void workLoop(size_t idx, ServerCompletionQueueStatusPtr cqsPtr);

public:
    grpc::AsyncGenericService &getAsyncGenericService() {
        return _genericService;
    }

private:
    GigRpcServer &_owner;
    GrpcServerDescription _desc;
    RandomGenerator _randomGenerator;
    std::vector<autil::ThreadPtr> _workThreads;
    std::vector<::grpc::Alarm *> _shutdownAlarms;
    std::vector<ServerCompletionQueueStatusPtr> _completionQueues;
    grpc::ServerBuilder _builder;
    std::unique_ptr<grpc::Server> _server;
    grpc::AsyncGenericService _genericService;
    autil::LockFreeThreadPool *_callBackThreadPool;
    int32_t _listenPort;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcServerWorker);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCSERVERWORKER_H
