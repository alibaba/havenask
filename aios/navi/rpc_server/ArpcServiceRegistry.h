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
#pragma once

#include "autil/Lock.h"
#include "navi/rpc_server/ArpcRegistryParam.h"
#include <google/protobuf/service.h>

namespace navi {

class NaviSnapshot;

class ArpcServiceRegistry: public google::protobuf::Service
{
public:
    ArpcServiceRegistry(NaviSnapshot *snapshot,
                        const std::shared_ptr<google::protobuf::Service> &service);
    ~ArpcServiceRegistry();
    ArpcServiceRegistry(const ArpcServiceRegistry &) = delete;
    ArpcServiceRegistry &operator=(const ArpcServiceRegistry &) = delete;
public:
    bool init(const ArpcRegistryParam &param);
    const ArpcRegistryParam &getParam() const {
        return _param;
    }
    const std::string &getServiceName() const {
        return _service->GetDescriptor()->full_name();
    }
private:
    const google::protobuf::ServiceDescriptor* GetDescriptor() override;
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
    void runGraph(google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request,
                  google::protobuf::Message *response,
                  google::protobuf::Closure *done);
private:
    NaviSnapshot *_snapshot;
    std::shared_ptr<google::protobuf::Service> _service;
    ArpcRegistryParam _param;
    std::vector<GraphId> _graphIds;
};

NAVI_TYPEDEF_PTR(ArpcServiceRegistry);

}

