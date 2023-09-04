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
#ifndef CARBON_CONNECTIONPOOL_H
#define CARBON_CONNECTIONPOOL_H

#include "aios/network/anet/transport.h"
#include "aios/network/anet/anet.h"
#include "autil/LoopThread.h"
#include "autil/Lock.h"
#include <unordered_map>
#include "common/common.h"
#include "master/ANetTransportSingleton.h"
#include "master/HttpConnection.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
using namespace anet;
BEGIN_CARBON_NAMESPACE(master);
CARBON_TYPEDEF_PTR(Connection);

class ConnectionFactory;

class ConnectionPool
{
public:
    ConnectionPool();
    virtual ~ConnectionPool();
private:
    ConnectionPool(const ConnectionPool &);
    ConnectionPool& operator=(const ConnectionPool &);
public:
    virtual bool init();
    void stop();
    HttpConnectionPtr getConnection(const std::string &spec);
    void syncConnection(const std::set<std::string> &keepSpecs);
private:
    HttpConnectionPtr addConnection(const std::string &spec);
    HttpConnectionPtr doGetConnection(const std::string &spec);
    void removeConnection(const std::string &spec);
    std::vector<std::string> getRemoveList(const std::set<std::string> &keepSpecs);
protected:
    anet::Transport *_transport;
    autil::ReadWriteLock _mapLock;
    std::unordered_map<std::string, HttpConnectionPtr> _connectionMap;
private:
    CARBON_LOG_DECLARE();
};

END_CARBON_NAMESPACE(master);
#endif //CARBON_CONNECTIONPOOL_H