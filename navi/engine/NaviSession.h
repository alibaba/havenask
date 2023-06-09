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
#ifndef NAVI_NAVISESSION_H
#define NAVI_NAVISESSION_H

#include "navi/common.h"
#include "navi/engine/Biz.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/RunGraphParams.h"

namespace navi {

class NaviLogger;
class GraphDef;
class TraceAppender;
class Resource;
class NaviThreadPool;
class MemoryPoolResource;
class Graph;

class NaviSession : public NaviWorkerBase
{
public:
    NaviSession(const NaviLoggerPtr &logger,
                TaskQueue *taskQueue,
                BizManager *bizManager,
                GraphDef *graphDef,
                const ResourceMap *resourceMap,
                NaviUserResultClosure *closure = nullptr);
    ~NaviSession();
private:
    NaviSession(const NaviSession &);
    NaviSession &operator=(const NaviSession &);
public:
    void run() override;
    void drop() override;
    void checkGraph();
protected:
    void finalizeMetaInfoToResult() override;
protected:
    GraphDef *_graphDef;
    Graph *_graph;
};

NAVI_TYPEDEF_PTR(NaviSession);

}

#endif //NAVI_NAVISESSION_H
