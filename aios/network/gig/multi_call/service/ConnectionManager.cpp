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
#include "aios/network/gig/multi_call/service/ConnectionManager.h"
#include "aios/network/gig/multi_call/service/ConnectionPoolFactory.h"
#include "autil/ThreadPool.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ConnectionManager);

ConnectionManager::ConnectionManager() : _callBackThreadPool(nullptr) {}

ConnectionManager::~ConnectionManager() { stop(); }

void ConnectionManager::stop() {
    for (size_t i = 0; i < MC_PROTOCOL_UNKNOWN; i++) {
        if (_poolVector[i]) {
            _poolVector[i]->stop();
            _poolVector[i].reset();
        }
    }
    if (_callBackThreadPool) {
        _callBackThreadPool->stop();
        DELETE_AND_SET_NULL(_callBackThreadPool);
    }
}

bool ConnectionManager::init(const ConnectionConfig &config,
                             const MiscConfig &miscConf) {
    if (config.empty()) {
        AUTIL_LOG(WARN, "no connection type configured");
        return true;
    }
    auto callbackThreadNum = miscConf.callbackThreadNum;
    if (callbackThreadNum > 0) {
        _callBackThreadPool = new autil::LockFreeThreadPool(
            miscConf.callbackThreadNum, 1000, autil::WorkItemQueueFactoryPtr(),
            "GigConnMgrCB");
        if (!_callBackThreadPool->start()) {
            AUTIL_LOG(ERROR, "start callback thread pool failed");
            return false;
        }
    }
    for (const auto &conf : config) {
        const auto &typeStr = conf.first;
        auto type = convertProtocolType(typeStr);
        if (MC_PROTOCOL_UNKNOWN == type) {
            AUTIL_LOG(ERROR, "unknown config type [%s]", typeStr.c_str());
            return false;
        }
        if (_poolVector[type]) {
            AUTIL_LOG(ERROR, "duplicate config for type [%s]", typeStr.c_str());
            return false;
        }
        auto connectionPool = createConnectionPool(type, typeStr);
        if (!connectionPool) {
            return false;
        }
        if (!connectionPool->init(conf.second)) {
            return false;
        }
        _poolVector[type] = connectionPool;
    }
    return true;
}

ConnectionPoolPtr
ConnectionManager::createConnectionPool(ProtocolType type,
                                        const std::string &typeStr) {
    return ConnectionPoolFactory::createConnectionPool(type, typeStr);
}

ConnectionPtr ConnectionManager::getConnection(const std::string &spec,
                                               ProtocolType type) {
    const auto &connectionPool = getConnectionPool(type);
    if (!connectionPool) {
        return ConnectionPtr();
    }
    auto con = connectionPool->getConnection(spec);
    if (con) {
        con->setCallBackThreadPool(_callBackThreadPool);
    }
    return con;
}

void ConnectionManager::syncConnection(const set<string> &keepSpecs) {
    if (_poolVector[MC_PROTOCOL_TCP]) {
        _poolVector[MC_PROTOCOL_TCP]->syncConnection(keepSpecs);
    }
    if (_poolVector[MC_PROTOCOL_HTTP]) {
        _poolVector[MC_PROTOCOL_HTTP]->syncConnection(keepSpecs);
    }
    if (_poolVector[MC_PROTOCOL_ARPC]) {
        _poolVector[MC_PROTOCOL_ARPC]->syncConnection(keepSpecs);
    }
    if (_poolVector[MC_PROTOCOL_GRPC]) {
        _poolVector[MC_PROTOCOL_GRPC]->syncConnection(keepSpecs);
    }
    if (_poolVector[MC_PROTOCOL_GRPC_STREAM]) {
        _poolVector[MC_PROTOCOL_GRPC_STREAM]->syncConnection(keepSpecs);
    }
    if (_poolVector[MC_PROTOCOL_RDMA_ARPC]) {
        _poolVector[MC_PROTOCOL_RDMA_ARPC]->syncConnection(keepSpecs);
    }
}

bool ConnectionManager::supportProtocol(ProtocolType type) const {
    return _poolVector[type] != nullptr;
}

ConnectionPoolPtr ConnectionManager::getConnectionPool(ProtocolType type) {
    if (type >= MC_PROTOCOL_UNKNOWN) {
        AUTIL_LOG(ERROR, "invalid type [%u]", type);
        return ConnectionPoolPtr();
    }
    auto &connectionPool = _poolVector[type];
    if (!connectionPool) {
        AUTIL_LOG(ERROR, "type [%u] not enabled", type);
        return ConnectionPoolPtr();
    }
    return connectionPool;
}

} // namespace multi_call
