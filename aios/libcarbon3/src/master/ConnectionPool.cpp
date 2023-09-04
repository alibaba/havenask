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
#include "master/ConnectionPool.h"

using namespace std;
using namespace autil;
using namespace anet;

BEGIN_CARBON_NAMESPACE(master);
CARBON_LOG_SETUP(master, ConnectionPool);


ConnectionPool::ConnectionPool()
{
}

ConnectionPool::~ConnectionPool() {
    stop();
}

void ConnectionPool::stop() {
}

bool ConnectionPool::init() {
    return true;
}

HttpConnectionPtr ConnectionPool::getConnection(const std::string &spec) {
    auto connection = doGetConnection(spec);
    if (connection) {
        return connection;
    } else {
        return addConnection(spec);
    }
}

HttpConnectionPtr ConnectionPool::addConnection(const std::string &spec) {
    HttpConnectionPtr con;
    {
        ScopedReadWriteLock lock(_mapLock, 'w');
        auto it = _connectionMap.find(spec);
        if (_connectionMap.end() == it) {    
            HttpConnection *connection = new HttpConnection(ANetTransportSingleton::getInstance(), spec);
            con.reset(connection);
            if (!con) {
                CARBON_LOG(ERROR, "create connection failed, spec [%s]", spec.c_str());
                return HttpConnectionPtr();
            }
            _connectionMap[spec] = con;
        } else {
            con = it->second;
        }
    }
    return con;
}

HttpConnectionPtr ConnectionPool::doGetConnection(const std::string &spec) {
    ScopedReadWriteLock lock(_mapLock, 'r');
    auto it = _connectionMap.find(spec);
    if (_connectionMap.end() == it) {
        return HttpConnectionPtr();
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
            allSpec.push_back(con.first);
        }
    }
    for (const auto &spec : allSpec) {
        if (keepSpecs.end() == keepSpecs.find(spec)) {
            removeList.push_back(spec);
        }
    }
    return removeList;
}

END_CARBON_NAMESPACE(master);