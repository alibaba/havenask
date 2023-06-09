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
#include "navi/engine/SubGraphBase.h"
#include "navi/engine/Graph.h"
#include "navi/log/NaviLogger.h"

namespace navi {

SubGraphBase::SubGraphBase(SubGraphDef *subGraphDef,
                           GraphParam *param,
                           const GraphDomainPtr &domain,
                           const SubGraphBorderMapPtr &borderMap,
                           Graph *graph)
    : _logger(this, "SubGraph", param->logger)
    , _pool(param->pool)
    , _subGraphDef(subGraphDef)
    , _param(param)
    , _domain(domain)
    , _borderMap(borderMap)
    , _graph(graph)
    , _terminated(false)
{
    _logger.addPrefix("%d", getGraphId());
}

SubGraphBase::~SubGraphBase() {
    NAVI_LOG(SCHEDULE1, "deleted");
}


GraphId SubGraphBase::getGraphId() const {
    return _subGraphDef->graph_id();
}

NaviPartId SubGraphBase::getPartCount() const {
    return _subGraphDef->location().part_info().part_count();
}

NaviPartId SubGraphBase::getPartId() const {
    return _subGraphDef->location().this_part_id();
}

void SubGraphBase::setOverrideData(const std::vector<OverrideData> &overrideData) {
    _overrideData = overrideData;
}

void SubGraphBase::notifyFinish(ErrorCode ec) {
    _graph->notifyFinish(ec);
}

void SubGraphBase::terminateThis(ErrorCode ec) {
    bool expect = false;
    if (!_terminated.compare_exchange_weak(expect, true)) {
        return;
    }
    terminate(ec);
}

bool SubGraphBase::terminated() const {
    return _terminated.load(std::memory_order_relaxed);
}

const SubGraphDef &SubGraphBase::getSubGraphDef() const {
    return *_subGraphDef;
}

void SubGraphBase::setErrorCode(ErrorCode ec) {
    _param->worker->setErrorCode(ec);
}

ErrorCode SubGraphBase::getErrorCode() const {
    return _param->worker->getErrorCode();
}

const std::string &SubGraphBase::getBizName() const {
    return _subGraphDef->location().biz_name();
}

Graph *SubGraphBase::getGraph() const {
    return _graph;
}

GraphParam *SubGraphBase::getParam() const {
    return _param;
}

NaviWorkerBase *SubGraphBase::getWorker() const {
    return _param->worker;
}

}
