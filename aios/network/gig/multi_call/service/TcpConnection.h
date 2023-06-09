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
#ifndef ISEARCH_MULTI_CALL_TCPCONNECTION_H
#define ISEARCH_MULTI_CALL_TCPCONNECTION_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/Connection.h"
#include "autil/Lock.h"

namespace anet {
class Connection;
class Transport;
} // namespace anet

namespace multi_call {

class TcpConnection : public Connection {
public:
    TcpConnection(anet::Transport *transport, const std::string &spec,
                  size_t queueSize);
    ~TcpConnection();

private:
    TcpConnection(const TcpConnection &);
    TcpConnection &operator=(const TcpConnection &);

public:
    void post(const RequestPtr &request, const CallBackPtr &callBack) override;

private:
    anet::Connection *createAnetConnection();
    anet::Connection *getAnetConnection();

private:
    anet::Transport *_transport;
    autil::ReadWriteLock _connectLock;
    anet::Connection *_connection;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(TcpConnection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_TCPCONNECTION_H
