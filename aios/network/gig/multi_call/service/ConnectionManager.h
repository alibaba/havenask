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
#ifndef ISEARCH_MULTI_CALL_CONNECTIONMANAGER_H
#define ISEARCH_MULTI_CALL_CONNECTIONMANAGER_H

#include "autil/LockFreeThreadPool.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/service/ConnectionPool.h"

namespace multi_call {

class ConnectionManager {
public:
    ConnectionManager();
    virtual ~ConnectionManager();

private:
    ConnectionManager(const ConnectionManager &);
    ConnectionManager &operator=(const ConnectionManager &);

public:
    bool init(const ConnectionConfig &config, const MiscConfig &miscConf);
    ConnectionPtr getConnection(const std::string &spec, ProtocolType type);
    void syncConnection(const std::set<std::string> &keepSpecs);
    void stop();
    bool supportProtocol(ProtocolType type) const;
    ConnectionPoolPtr getConnectionPool(ProtocolType type);
    autil::LockFreeThreadPool *getCallBackThreadPool() const {
        return _callBackThreadPool;
    }

private:
    // virtual for ut
    virtual ConnectionPoolPtr
    createConnectionPool(ProtocolType type, const std::string &typeStr = "");

private:
    ConnectionPoolPtr _poolVector[MC_PROTOCOL_UNKNOWN + 1];
    autil::LockFreeThreadPool *_callBackThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ConnectionManager);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONNECTIONMANAGER_H
