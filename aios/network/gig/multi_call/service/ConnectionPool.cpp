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
#include "aios/network/gig/multi_call/service/ConnectionPool.h"

#include "aios/network/gig/multi_call/service/ConnectionFactory.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ConnectionPool);

ConnectionPool::ConnectionPool() : _transport(NULL), _queueSize(0), _threadNum(0) {
}

ConnectionPool::~ConnectionPool() {
    stop();
}

void ConnectionPool::stop() {
    if (_transport) {
        _transport->stop();
        _transport->wait();
    }
    DELETE_AND_SET_NULL(_transport);
}

bool ConnectionPool::init(const ProtocolConfig &config) {
    auto transport = createTransport(config.threadNum);
    if (!transport || !transport->start()) {
        delete transport;
        AUTIL_LOG(ERROR, "anet transport start failed!");
        return false;
    }
    transport->setName("Gig");
    transport->setTimeoutLoopInterval(config.anetLoopTimeout);
    _queueSize = config.queueSize;
    _threadNum = config.threadNum;
    _transport = transport;
    return true;
}

ConnectionPtr ConnectionPool::getConnection(const std::string &spec) {
    auto connection = doGetConnection(spec);
    if (connection) {
        return connection;
    } else {
        return addConnection(spec);
    }
}

ConnectionPtr ConnectionPool::addConnection(const std::string &spec) {
    ConnectionPtr con;
    {
        ScopedReadWriteLock lock(_mapLock, 'w');
        auto it = _connectionMap.find(spec);
        if (_connectionMap.end() == it) {
            con = _connectionFactory->createConnection(spec);
            if (!con) {
                AUTIL_LOG(ERROR, "create connection failed, spec [%s]", spec.c_str());
                return ConnectionPtr();
            }
            _connectionMap[spec] = con;
        } else {
            con = it->second;
        }
    }
    return con;
}

ConnectionPtr ConnectionPool::doGetConnection(const std::string &spec) {
    ScopedReadWriteLock lock(_mapLock, 'r');
    auto it = _connectionMap.find(spec);
    if (_connectionMap.end() == it) {
        return ConnectionPtr();
    }
    return it->second;
}

void ConnectionPool::syncConnection(const set<string> &keepSpecs) {
    const auto &removeList = getRemoveList(keepSpecs);
    for (const auto &spec : removeList) {
        removeConnection(spec);
    }
}

void ConnectionPool::removeConnection(const std::string &spec) {
    ScopedReadWriteLock lock(_mapLock, 'w');
    _connectionMap.erase(spec);
}

std::vector<std::string> ConnectionPool::getRemoveList(const set<string> &keepSpecs) {
    std::vector<std::string> allSpec;
    std::vector<std::string> removeList;
    {
        ScopedReadWriteLock lock(_mapLock, 'r');
        for (const auto &con : _connectionMap) {
            allSpec.push_back(con.second->getSpec());
        }
    }
    for (const auto &spec : allSpec) {
        if (keepSpecs.end() == keepSpecs.find(spec)) {
            removeList.push_back(spec);
        }
    }
    return removeList;
}

anet::Transport *ConnectionPool::createTransport(uint32_t threadNum) {
    return new anet::Transport(threadNum);
}

} // namespace multi_call
