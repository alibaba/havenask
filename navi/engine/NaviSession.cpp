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
#include "navi/engine/NaviSession.h"
#include "autil/StringUtil.h"
#include "navi/engine/Graph.h"
#ifndef AIOS_OPEN_SOURCE
#include "lockless_allocator/LocklessApi.h"
#endif
#include "navi/log/NaviLogger.h"
#include "navi/resource/MetaInfoResourceBase.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace navi {

NaviSession::NaviSession(const NaviLoggerPtr &logger,
                         TaskQueue *taskQueue,
                         BizManager *bizManager,
                         GraphDef *graphDef,
                         const ResourceMap *resourceMap,
                         NaviUserResultClosure *closure)
    : NaviWorkerBase(logger, taskQueue, bizManager, closure)
    , _graphDef(graphDef)
{
    _logger.addPrefix("local");
    _graph = NAVI_POOL_NEW_CLASS(_pool, Graph, getGraphParam(), nullptr);
    _graph->setResourceMap(resourceMap);
    NAVI_LOG(SCHEDULE1, "construct");
}

NaviSession::~NaviSession() {
    NAVI_LOG(SCHEDULE1, "destructed");
    clear();
    // todo: fill results
    {
        NaviLoggerScope scope(_logger);
        NAVI_POOL_DELETE_CLASS(_graph);
    }
    multi_call::freeProtoMessage(_graphDef);
}

void NaviSession::run() {
#ifndef AIOS_OPEN_SOURCE
    MallocPoolScope mallocScope;
#endif
    NAVI_LOG(SCHEDULE1, "start");
    auto ec = _graph->init(_graphDef);
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
        return;
    }
    if (!getUserResult()->hasOutput()) {
        NAVI_LOG(ERROR, "graph has no output");
        _graph->notifyFinish(EC_NO_GRAPH_OUTPUT);
        return;
    }
    ec = _graph->run();
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
    }
}

void NaviSession::checkGraph() {
    auto ec = _graph->init(_graphDef);
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
    }
    ec = _graph->run();
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
    }
}

void NaviSession::drop() {
    NAVI_LOG(SCHEDULE1, "dropped");
    auto ec = EC_QUEUE_FULL;
    if (getTimeoutChecker()->timeout()) {
        ec = EC_TIMEOUT;
    }
    _graph->notifyFinish(ec);

}

void NaviSession::finalizeMetaInfoToResult() {
    NAVI_LOG(SCHEDULE1, "start finalize meta info to result");
    auto metaInfoResource = getMetaInfoResource();
    if (metaInfoResource) {
        {
            NaviLoggerScope scope(getLogger());
            metaInfoResource->finalize();
        }
        getUserResult()->setMetaInfoResource(metaInfoResource);
    }
}

}
