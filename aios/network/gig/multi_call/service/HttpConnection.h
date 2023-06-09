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
#ifndef ISEARCH_MULTI_CALL_HTTPCONNECTION_H
#define ISEARCH_MULTI_CALL_HTTPCONNECTION_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/Connection.h"
#include "autil/Lock.h"
#include <list>

namespace anet {
class Connection;
class Transport;
} // namespace anet

namespace multi_call {

class HttpConnection : public Connection,
                       public std::enable_shared_from_this<HttpConnection> {
public:
    HttpConnection(anet::Transport *transport, const std::string &spec,
                   size_t queueSize);
    ~HttpConnection();

private:
    HttpConnection(const HttpConnection &);
    HttpConnection &operator=(const HttpConnection &);

public:
    void post(const RequestPtr &request, const CallBackPtr &callBack) override;
    void recycleConnection(anet::Connection *con);
    void closeConnection(anet::Connection *con) const;

private:
    anet::Connection *getAnetConnection();
    anet::Connection *createConnection();

private:
    anet::Transport *_transport;
    autil::ReadWriteLock _listLock;
    std::list<anet::Connection *> _connectionList;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HttpConnection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HTTPCONNECTION_H
