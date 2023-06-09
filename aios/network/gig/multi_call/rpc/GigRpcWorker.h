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
#ifndef ISEARCH_MULTI_CALL_GIGRPCWORKER_H
#define ISEARCH_MULTI_CALL_GIGRPCWORKER_H

#include "aios/network/gig/multi_call/agent/GigAgent.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/gig/multi_call/stream/GigServerStreamCreator.h"
#include "autil/Lock.h"
#include <grpc++/impl/codegen/proto_utils.h>

namespace multi_call {

struct GigRpcMethodArg {
    GigRpcMethodArg() : request(nullptr), response(nullptr) {}
    ~GigRpcMethodArg() {
        freeProtoMessage(request);
        freeProtoMessage(response);
    }

public:
    bool validate() const {
        return request != nullptr && response != nullptr && method;
    }

public:
    google::protobuf::Message *request;
    google::protobuf::Message *response;
    GigRpcMethod method;
    CompatibleFieldInfo compatibleInfo;
    // default false, heartbeat request donot need to create query info
    bool isHeartbeatMethod = false;
};

MULTI_CALL_TYPEDEF_PTR(GigRpcMethodArg);

struct GigStreamRpcArg {
    GigStreamRpcArg() : request(nullptr), response(nullptr) {}
    ~GigStreamRpcArg() {
        freeProtoMessage(request);
        freeProtoMessage(response);
    }

public:
    bool validate() const {
        return request != nullptr && response != nullptr &&
               creator.get() != nullptr;
    }

public:
    google::protobuf::Message *request;
    google::protobuf::Message *response;
    GigServerStreamCreatorPtr creator;
};

MULTI_CALL_TYPEDEF_PTR(GigStreamRpcArg);

class ServerDescription;

class GigRpcWorker {
public:
    GigRpcWorker();
    virtual ~GigRpcWorker();

private:
    GigRpcWorker(const GigRpcWorker &);
    GigRpcWorker &operator=(const GigRpcWorker &);

public:
    bool addMethod(const std::string &methodName,
                   const GigRpcMethodArgPtr &arg);
    GigRpcMethodArgPtr getMethodArg(const std::string &methodName) const;
    bool registerStreamService(const GigServerStreamCreatorPtr &creator);
    bool unRegisterStreamService(const GigServerStreamCreatorPtr &creator);
    GigServerStreamCreatorPtr
    getStreamService(const std::string &methodName) const;
    const GigAgentPtr &getAgent() const { return _agent; }

protected:
    GigAgentPtr _agent;
    mutable autil::ReadWriteLock _serviceLock;
    std::unordered_map<std::string, GigRpcMethodArgPtr> _serviceMap;
    std::unordered_map<std::string, GigServerStreamCreatorPtr>
        _streamServiceMap;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRpcWorker);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGRPCWORKER_H
