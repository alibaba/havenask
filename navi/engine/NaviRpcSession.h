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
#ifndef NAVI_NAVIRPCSESSION_H
#define NAVI_NAVIRPCSESSION_H

#include "navi/engine/Graph.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/RpcParam.h"

namespace navi {

class NaviRpcSession : public NaviWorkerBase
{
public:
    NaviRpcSession(const NaviLoggerPtr &logger,
                   TaskQueue *taskQueue,
                   BizManager *bizManager,
                   RpcParam *rpcParam);
    ~NaviRpcSession();
private:
    NaviRpcSession(const NaviRpcSession &);
    NaviRpcSession &operator=(const NaviRpcSession &);
public:
    void run() override;
    void drop() override;
private:
    bool initGraphDef();
    bool initGraphDefFromConfig();
private:
    RpcParam *_rpcParam;
    GraphDef *_graphDef;
    Graph *_graph;
};

}

#endif //NAVI_NAVIRPCSESSION_H
