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
#include "navi/rpc_server/ArpcServiceRegistry.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/log/NaviLogger.h"
#include "navi/rpc_server/ArpcGraphClosure.h"
#include "navi/rpc_server/NaviArpcRequestData.h"
#include <autil/StringUtil.h>

namespace navi {

ArpcServiceRegistry::ArpcServiceRegistry(NaviSnapshot *snapshot,
                                         const std::shared_ptr<google::protobuf::Service> &service)
    : _snapshot(snapshot)
    , _service(service)
{
}

ArpcServiceRegistry::~ArpcServiceRegistry() {
}

bool ArpcServiceRegistry::init(const ArpcRegistryParam &param) {
    auto desc = _service->GetDescriptor();
    if (!desc) {
        NAVI_KERNEL_LOG(ERROR, "get service descriptor failed");
        return false;
    }
    const auto &methodName = param.arpcParam.method;
    if (methodName.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty method name");
        return false;
    }
    const auto &graphParam = param.graphParam;
    const auto &requestDataName = graphParam.requestDataName;
    if (requestDataName.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty request data name, method [%s]",
                        methodName.c_str());
        return false;
    }
    const auto &graphDef = graphParam.graph;
    auto subCount = graphDef.sub_graphs_size();
    if (subCount == 0) {
        NAVI_KERNEL_LOG(ERROR, "invalid graph, no sub graph, method [%s]",
                        methodName.c_str());
        return false;
    }
    for (int i = 0; i < subCount; i++) {
        auto graphId = graphDef.sub_graphs(i).graph_id();
        if (graphId >= 0) {
            _graphIds.push_back(graphId);
        }
    }
    auto methodDesc = desc->FindMethodByName(methodName);
    if (!methodDesc) {
        NAVI_KERNEL_LOG(ERROR, "method [%s] not exist in service [%s] def",
                        methodName.c_str(), desc->full_name().c_str());
        return false;
    }
    _param = param;
    initHttpAliasMap();
    return true;
}

void ArpcServiceRegistry::initHttpAliasMap() {
    const auto &arpcParam = _param.arpcParam;
    auto httpPath = "/" + GetDescriptor()->name() + "/" + arpcParam.method;
    for (const auto &alias : arpcParam.httpAliasVec) {
        _httpAliasMap[alias] = httpPath;
    }
}

const google::protobuf::ServiceDescriptor *ArpcServiceRegistry::GetDescriptor() {
    return _service->GetDescriptor();
}

void ArpcServiceRegistry::CallMethod(
    const google::protobuf::MethodDescriptor *method,
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    google::protobuf::Message *response,
    google::protobuf::Closure *done)
{
    const auto &methodName = _param.arpcParam.method;
    if (method->name() != methodName) {
        controller->SetFailed("rpc method not supported: " + method->full_name() + ", expect: " + methodName + "\n");
        done->Run();
        return;
    }
    runGraph(controller, request, response, done);
}

const google::protobuf::Message &ArpcServiceRegistry::GetRequestPrototype(
    const google::protobuf::MethodDescriptor *method) const
{
    return _service->GetRequestPrototype(method);
}

const google::protobuf::Message &ArpcServiceRegistry::GetResponsePrototype(
    const google::protobuf::MethodDescriptor *method) const
{
    return _service->GetResponsePrototype(method);
}

void ArpcServiceRegistry::runGraph(
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    google::protobuf::Message *response,
    google::protobuf::Closure *done)
{
    const auto &methodName = _param.arpcParam.method;
    const auto &graphParam = _param.graphParam;
    std::string clientIp;
    auto gigController =
        dynamic_cast<multi_call::GigRpcController *>(controller);
    if (gigController) {
        clientIp = gigController->getClientIp();
    } else {
        clientIp = "unknown";
    }
    auto requestData =
        std::make_shared<NaviArpcRequestData>(request, nullptr, methodName, clientIp);
    RunGraphParams runGraphParams = graphParam.params;
    NamedData namedRequest;
    namedRequest.name = graphParam.requestDataName;
    namedRequest.data = requestData;
    for (auto graphId : _graphIds) {
        namedRequest.graphId = graphId;
        auto ret = runGraphParams.addNamedData(namedRequest);
        (void)ret;
        assert(ret);
    }
    auto graphDef = new GraphDef();
    graphDef->CopyFrom(graphParam.graph);
    auto closure = new ArpcGraphClosure(methodName, controller, response, done);
    std::string debugParamStr;
    closure->init(debugParamStr);
    if (!debugParamStr.empty()) {
        runGraphParams.setDebugParamStr(debugParamStr);
        closure->setNeedDebug(true);
        requestData->setAiosDebug(true);
    }
    _snapshot->runGraphAsync(graphDef, runGraphParams, nullptr, closure);
}

}
