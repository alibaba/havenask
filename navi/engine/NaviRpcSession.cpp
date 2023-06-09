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
#include "navi/engine/NaviRpcSession.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace navi {

NaviRpcSession::NaviRpcSession(const NaviLoggerPtr &logger,
                               TaskQueue *taskQueue,
                               BizManager *bizManager,
                               RpcParam *rpcParam)
    : NaviWorkerBase(logger, taskQueue, bizManager)
    , _rpcParam(rpcParam)
{
    _logger.addPrefix("rpc");
    auto arena = rpcParam->getRequest()->GetArena();
    _graphDef = google::protobuf::Arena::CreateMessage<GraphDef>(arena);
    _graph = NAVI_POOL_NEW_CLASS(_pool, Graph, getGraphParam(), nullptr);
}

NaviRpcSession::~NaviRpcSession() {
    clear();
    {
        NaviLoggerScope scope(_logger);
        NAVI_POOL_DELETE_CLASS(_graph);
    }
    multi_call::freeProtoMessage(_graphDef);
    DELETE_AND_SET_NULL(_rpcParam);
}

void NaviRpcSession::run() {
    NAVI_LOG(SCHEDULE1, "start");
    if (!initGraphDef()) {
        _graph->notifyFinish(EC_INIT_GRAPH);
        return;
    }
    assert(_graphDef);
    auto ec = _graph->init(_graphDef);
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
        return;
    }
    ec = _graph->run();
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
    }
}

bool NaviRpcSession::initGraphDef() {
    auto request = _rpcParam->getRequest();
    if (request->has_graph()) {
        _graphDef->CopyFrom(request->graph());
        return true;
    } else {
        return initGraphDefFromConfig();
    }
}

bool NaviRpcSession::initGraphDefFromConfig() {
    auto request = _rpcParam->getRequest();
    const auto &bizName = request->biz_name();
    auto biz = _param.bizManager->getBiz(_logger.logger.get(), "server_biz");
    if (!biz) {
        NAVI_LOG(ERROR, "biz [%s] not exist", bizName.c_str());
        return false;
    }
    const auto &graphName = request->graph_name();
    auto graphInfo = biz->getGraphInfo("graph_a");
    if (!graphInfo) {
        NAVI_LOG(ERROR,
                 "biz [%s], graph [%s] not exist, check biz graph config",
                 bizName.c_str(), graphName.c_str());
        return false;
    }
    _graphDef->CopyFrom(graphInfo->getGraphDef());
    return true;
}

void NaviRpcSession::drop() {
    NAVI_LOG(SCHEDULE1, "dropped");
    auto ec = EC_QUEUE_FULL;
    if (getTimeoutChecker()->timeout()) {
        ec = EC_TIMEOUT;
    }
    _graph->notifyFinish(ec);
}

}
