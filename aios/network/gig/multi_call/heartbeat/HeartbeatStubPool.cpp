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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatStubPool.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/gig/multi_call/service/GrpcConnectionPool.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatStubPool);

HeartbeatStubPool::HeartbeatStubPool(const ConnectionManagerPtr &cm)
    : _connectionManager(cm) {}

HeartbeatStubPool::~HeartbeatStubPool() {}

bool HeartbeatStubPool::init() {
    if (_connectionManager->supportProtocol(MC_PROTOCOL_GRPC)) {
        ConnectionPoolPtr connectionPool =
            _connectionManager->getConnectionPool(MC_PROTOCOL_GRPC);
        if (!connectionPool) {
            AUTIL_LOG(ERROR, "get grpc connection pool failed");
            return false;
        }
        GrpcConnectionPoolPtr grpcConnPool =
            std::dynamic_pointer_cast<GrpcConnectionPool>(connectionPool);
        if (!grpcConnPool) {
            AUTIL_LOG(ERROR, "cast to grpc connection pool failed");
            return false;
        }
        _grpcClientWorker = grpcConnPool->getGrpcClientWorker();
        if (!_grpcClientWorker) {
            AUTIL_LOG(ERROR, "get grpc client workder failed");
            return false;
        }
    }
    return true;
}

HeartbeatServiceStubPtr
HeartbeatStubPool::getServiceStub(const std::string &hbSpec) {
    {
        autil::ScopedLock lock(_stubPoolLock);
        auto iter = _stubPool.find(hbSpec);
        if (iter != _stubPool.end()) {
            HeartbeatStubPtr &hbStubPtr = iter->second;
            RPCChannelPtr &channelPtr = hbStubPtr->channelPtr;
            if (channelPtr && !channelPtr->ChannelBroken()) {
                return hbStubPtr->stubPtr;
            }
            _stubPool.erase(iter); // broken, erase it
        }
    }

    return createServiceStub(hbSpec);
}

HeartbeatServiceStubPtr
HeartbeatStubPool::createServiceStub(const std::string &hbSpec) {

    ConnectionPtr conn =
        _connectionManager->getConnection(hbSpec, MC_PROTOCOL_ARPC);
    if (!conn) {
        AUTIL_LOG(WARN, "hb get connection failed for spec:[%s]",
                  hbSpec.c_str());
        return HeartbeatServiceStubPtr();
    }

    ArpcConnectionPtr arpcConn =
        std::dynamic_pointer_cast<ArpcConnection>(conn);
    if (!arpcConn) {
        AUTIL_LOG(WARN, "hb cast to  arpc connection failed for spec:[%s]",
                  hbSpec.c_str());
        return HeartbeatServiceStubPtr();
    }

    RPCChannelPtr channelPtr = arpcConn->getChannel();
    if (!channelPtr) {
        return HeartbeatServiceStubPtr();
    }
    HeartbeatServiceStubPtr stubPtr(new HeartbeatService_Stub(
        channelPtr.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL));

    {
        autil::ScopedLock lock(_stubPoolLock);
        HeartbeatStubPtr hbStub(new HeartbeatStub(stubPtr, channelPtr));
        _stubPool[hbSpec] = std::move(hbStub);
    }
    return stubPtr;
}

bool HeartbeatStubPool::cleanObsoleteStub(
    const std::unordered_set<std::string> &allSpecSet) {
    autil::ScopedLock lock(_stubPoolLock);
    if (allSpecSet.size() >= _stubPool.size()) {
        return false;
    }
    std::unordered_map<std::string, HeartbeatStubPtr> stubPoolKeep;
    std::unordered_map<std::string, HeartbeatStubPtr>::iterator stubIter,
        stubIterEnd = _stubPool.end();
    for (auto iter = allSpecSet.begin(); iter != allSpecSet.end(); iter++) {
        stubIter = _stubPool.find(*iter);
        if (stubIter != stubIterEnd) {
            stubPoolKeep[*iter] = std::move(stubIter->second);
        }
    }
    _stubPool.swap(stubPoolKeep);
    return true;
}

void HeartbeatStubPool::addServiceStub(const std::string &hbSpec,
                                       HeartbeatStubPtr &stub) {
    autil::ScopedLock lock(_stubPoolLock);
    _stubPool[hbSpec] = stub;
}

const std::unordered_map<std::string, HeartbeatStubPtr> &
HeartbeatStubPool::getStubPool() const {
    return _stubPool;
}

HeartbeatGrpcStubPtr HeartbeatStubPool::getGrpcStub(const std::string &hbSpec) {
    ConnectionPtr conn =
        _connectionManager->getConnection(hbSpec, MC_PROTOCOL_GRPC);
    if (!conn) {
        AUTIL_LOG(WARN, "hb get grpc connection failed for spec:[%s]",
                  hbSpec.c_str());
        return HeartbeatGrpcStubPtr();
    }

    GrpcConnectionPtr grpcConn =
        std::dynamic_pointer_cast<GrpcConnection>(conn);
    if (!grpcConn) {
        AUTIL_LOG(WARN, "hb cast to  grpc connection failed for spec:[%s]",
                  hbSpec.c_str());
        return HeartbeatGrpcStubPtr();
    }

    GrpcChannelPtr channel = grpcConn->getChannel();
    if (!channel) {
        AUTIL_LOG(WARN, "hb get grpc channel failed for spec:[%s]",
                  hbSpec.c_str());
        return HeartbeatGrpcStubPtr();
    }
    HeartbeatGrpcStubPtr grpcStub(new grpc::GenericStub(channel));
    return grpcStub;
}

GrpcClientWorkerPtr &HeartbeatStubPool::getGrpcClientWorker() {
    return _grpcClientWorker;
}

} // namespace multi_call
