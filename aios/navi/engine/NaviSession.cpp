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
#include "lockless_allocator/LocklessApi.h"
#include "navi/log/NaviLogger.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace navi {

NaviSession::NaviSession(const NaviLoggerPtr &logger,
                         TaskQueue *taskQueue,
                         BizManager *bizManager,
                         GraphDef *graphDef,
                         const ResourceMap *resourceMap,
                         NaviUserResultClosure *closure)
    : NaviWorkerBase(logger, taskQueue, bizManager, closure)
    , _resourceMap(resourceMap)
    , _graphDef(graphDef)
    , _graph(nullptr)
{
}

NaviSession::~NaviSession() {
    NAVI_LOG(SCHEDULE1, "destructed");
    clear();
    // todo: fill results
    {
        NaviLoggerScope scope(_logger);
        delete _graph;
    }
    multi_call::freeProtoMessage(_graphDef);
}

bool NaviSession::doInit(GraphParam *param) {
    _logger.addPrefix("local");
    _graph = new Graph(param, nullptr);
    _graph->setResourceMap(_resourceMap);
    NAVI_LOG(SCHEDULE1, "construct");
    return true;
}

void NaviSession::run() {
    MallocPoolScope mallocScope;
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
    if (_graph->isTimeout()) {
        ec = EC_TIMEOUT;
    }
    _graph->notifyFinish(ec);

}

bool NaviSession::isTimeout() const {
    auto timeoutChecker = _graph->getTimeoutChecker();
    if (timeoutChecker->timeout()) {
        auto curTime = autil::TimeUtility::currentTime();
        auto beginTime = timeoutChecker->beginTime();
        NAVI_LOG(ERROR,
                 "session timeout, expect time [%ld ms] use time [%ld ms], start "
                 "[%ld], end[%ld]",
                 timeoutChecker->timeoutMs(),
                 (curTime - beginTime) / FACTOR_MS_TO_US,
                 beginTime / FACTOR_MS_TO_US,
                 curTime / FACTOR_MS_TO_US);
        NAVI_LOG(SCHEDULE1, "timeout bt: %s",
                 _logger.logger->firstErrorBackTrace().c_str());
        return true;
    }
    return false;
}

}
