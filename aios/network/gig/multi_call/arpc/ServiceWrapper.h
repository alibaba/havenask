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
#ifndef ISEARCH_MULTI_CALL_SERVICEWRAPPER_H
#define ISEARCH_MULTI_CALL_SERVICEWRAPPER_H

#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/gig/multi_call/arpc/ArpcClosure.h"
#include "google/protobuf/service.h"

namespace multi_call {

class GigRpcServer;
class GigAgent;

class ServiceWrapper : public google::protobuf::Service {
public:
    ServiceWrapper(GigRpcServer &owner, google::protobuf::Service *rpcService,
                   const CompatibleFieldInfo &compatibleInfo);
    ~ServiceWrapper();

private:
    ServiceWrapper(const ServiceWrapper &);
    ServiceWrapper &operator=(const ServiceWrapper &);

public:
    const google::protobuf::ServiceDescriptor *GetDescriptor() override;
    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;
    const google::protobuf::Message &GetRequestPrototype(
        const google::protobuf::MethodDescriptor *method) const override;
    const google::protobuf::Message &GetResponsePrototype(
        const google::protobuf::MethodDescriptor *method) const override;

private:
    std::string getGigRequestMeta(arpc::Tracer *trace,
                                  const google::protobuf::Message *request);
    void addEagleEyeTraceInfo(std::shared_ptr<QuerySession> &querySession,
                              const google::protobuf::Message *request);

private:
    GigRpcServer &_owner;
    std::shared_ptr<GigAgent> _agent;
    google::protobuf::Service *_rpcService;
    CompatibleFieldInfo _compatibleInfo;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ServiceWrapper);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SERVICEWRAPPER_H
