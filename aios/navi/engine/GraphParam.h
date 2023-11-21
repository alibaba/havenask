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

#include "navi/common.h"
#include "navi/engine/BizManager.h"
#include "navi/engine/GraphMetric.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/engine/GraphResult.h"
#include "navi/log/NaviLogger.h"

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace navi {

class NaviWorkerBase;
struct ForkGraphParam;

struct GraphParam
{
public:
    GraphParam();
    ~GraphParam();
public:
    bool init(SessionId id,
              NaviWorkerBase *worker_,
              const NaviLoggerPtr &logger_,
              const RunGraphParams &runParams_,
              const NaviSymbolTablePtr &naviSymbolTable,
              const NaviHostInfo *hostInfo);
    ErrorCode init(GraphId forkGraphId, const std::string &forkBizName,
                   const std::string &forkNode, const GraphParam &parent,
                   const NaviLoggerPtr &logger_,
                   const ForkGraphParam &forkParam, int64_t remainTimeMs);
public:
    NaviWorkerBase *worker = nullptr;
    BizManager *bizManager = nullptr;
    CreatorManager *creatorManager = nullptr;
    NaviLoggerPtr logger;
    std::shared_ptr<multi_call::QuerySession> querySession;
    RunGraphParams runParams;
    GraphResultPtr graphResult;
};

}
