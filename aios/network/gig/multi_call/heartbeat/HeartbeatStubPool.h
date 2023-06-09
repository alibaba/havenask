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
#ifndef ISEARCH_MULTI_CALL_HEARTBEATSTUBPOOL_H
#define ISEARCH_MULTI_CALL_HEARTBEATSTUBPOOL_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/Heartbeat.pb.h"
#include "aios/network/gig/multi_call/service/ArpcConnection.h"
#include "aios/network/gig/multi_call/service/ConnectionManager.h"
#include "aios/network/gig/multi_call/service/GrpcConnection.h"
#include "autil/Lock.h"
#include <grpc++/generic/generic_stub.h>
#include <string>
#include <unordered_map>

namespace multi_call {

typedef std::shared_ptr<HeartbeatService_Stub> HeartbeatServiceStubPtr;
typedef std::shared_ptr<grpc::GenericStub> HeartbeatGrpcStubPtr;

struct HeartbeatStub {
    HeartbeatStub(HeartbeatServiceStubPtr &hb, RPCChannelPtr &ch)
        : stubPtr(hb), channelPtr(ch) {}
    HeartbeatServiceStubPtr stubPtr;
    RPCChannelPtr channelPtr;
};
MULTI_CALL_TYPEDEF_PTR(HeartbeatStub);

class HeartbeatStubPool {
public:
    HeartbeatStubPool(const ConnectionManagerPtr &cm);
    ~HeartbeatStubPool();

private:
    HeartbeatStubPool(const HeartbeatStubPool &);
    HeartbeatStubPool &operator=(const HeartbeatStubPool &);

public:
    bool init();
    HeartbeatServiceStubPtr getServiceStub(const std::string &hbSpec);
    bool cleanObsoleteStub(const std::unordered_set<std::string> &allSpecSet);
    // for test
    void addServiceStub(const std::string &hbSpec, HeartbeatStubPtr &stub);
    const std::unordered_map<std::string, HeartbeatStubPtr> &
    getStubPool() const;

public:
    HeartbeatGrpcStubPtr getGrpcStub(const std::string &hbSpec);
    GrpcClientWorkerPtr &getGrpcClientWorker();

private:
    HeartbeatServiceStubPtr createServiceStub(const std::string &hbSpec);

private:
    const ConnectionManagerPtr _connectionManager;
    // arpc
    autil::ThreadMutex _stubPoolLock;
    std::unordered_map<std::string, HeartbeatStubPtr> _stubPool;
    // grpc
    GrpcClientWorkerPtr _grpcClientWorker;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatStubPool);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HEARTBEATSTUBPOOL_H
