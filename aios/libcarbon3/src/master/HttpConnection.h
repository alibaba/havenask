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
#ifndef CARBON_MASTER_HTTPCONNECTION_H
#define CARBON_MASTER_HTTPCONNECTION_H

#include "autil/Lock.h"
#include "aios/network/anet/anet.h"
#include <list>
#include "common/common.h"
#include "carbon/Log.h"

using namespace anet;

BEGIN_CARBON_NAMESPACE(master);
class HttpConnection
{
public:
    HttpConnection(anet::Transport *transport,
                  const std::string &spec);
    ~HttpConnection();
private:
    HttpConnection(const HttpConnection &);
    HttpConnection& operator=(const HttpConnection &);
public:
    Packet *sendPacket(Packet *packet, bool block = false, bool longConn = true);
    void recycleConnection(anet::Connection *con);
private:
    anet::Connection *getAnetConnection();
    anet::Connection *createConnection();
private:
    anet::Transport *_transport;
    autil::ReadWriteLock _listLock;
    std::list<anet::Connection *> _connectionList;
    std::string _spec;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HttpConnection);
END_CARBON_NAMESPACE(master);
#endif 
