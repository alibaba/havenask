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
#include "navi/engine/GraphParam.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/ForkGraphResult.h"

namespace navi {

GraphParam::GraphParam() {
}

GraphParam::~GraphParam() {
}

bool GraphParam::init(SessionId id,
                      NaviWorkerBase *worker_,
                      const NaviLoggerPtr &logger_,
                      const RunGraphParams &runParams_,
                      const NaviSymbolTablePtr &naviSymbolTable,
                      const NaviHostInfo *hostInfo)
{
    worker = worker_;
    bizManager = worker->getBizManager();
    creatorManager = bizManager->getBuildinCreatorManager().get();
    logger = worker->getLogger().logger;
    runParams = runParams_;
    runParams.setSessionId(id);
    graphResult = std::make_shared<GraphResult>();
    graphResult->init(logger_, runParams.getTraceFormatPattern(),
                      naviSymbolTable, hostInfo);
    return true;
}

ErrorCode GraphParam::init(GraphId forkGraphId,
                           const std::string &forkBizName,
                           const std::string &forkNode,
                           const GraphParam &parent,
                           const NaviLoggerPtr &logger_,
                           const ForkGraphParam &forkParam,
                           int64_t remainTimeMs)
{
    worker = parent.worker;
    bizManager = parent.bizManager;
    creatorManager = parent.creatorManager;
    logger = std::make_shared<NaviLogger>(*logger_);
    querySession = parent.querySession;
    runParams = parent.runParams;
    if (forkParam.collectMetric) {
        runParams.setCollectMetric(true);
    }
    if (forkParam.collectPerf) {
        runParams.setCollectPerf(true);
    }
    runParams.clearNamedData();
    runParams.setTimeoutMs(std::min(forkParam.timeoutMs, remainTimeMs));
    graphResult =
        std::make_shared<ForkGraphResult>(forkGraphId, forkBizName, forkNode);
    graphResult->init(logger_,
                      runParams.getTraceFormatPattern(),
                      parent.graphResult->getGraphMetric()->getNaviSymbolTable(),
                      parent.graphResult->getHostInfo());
    parent.graphResult->addSubResult(graphResult);
    for (const auto &overrideData : forkParam.overrideDataVec) {
        if (!runParams.overrideEdgeData(overrideData)) {
            NAVI_KERNEL_LOG(
                ERROR,
                "override edge data failed, graphId [%d], partId [%d], outputNode [%s], outputPort [%s], data "
                "count [%lu]",
                overrideData.graphId,
                overrideData.partId,
                overrideData.outputNode.c_str(),
                overrideData.outputPort.c_str(),
                overrideData.datas.size());
            return EC_OVERRIDE_EDGE_DATA;
        }
    }
    for (const auto &namedData : forkParam.namedDataVec) {
        if (!runParams.addNamedData(namedData)) {
            NAVI_KERNEL_LOG(
                ERROR, "add named data failed, graphId [%d], name [%s]", namedData.graphId, namedData.name.c_str());
            return EC_ADD_NAMED_DATA;
        }
    }
    return EC_NONE;
}
}

