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
#ifndef NAVI_NAVI_STREAM_SESSION_H
#define NAVI_NAVI_STREAM_SESSION_H

#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Graph.h"
#include <multi_call/stream/GigStreamBase.h>

namespace navi {

class NaviMessage;

typedef std::map<GraphId, std::string> MirrorGraphInfoMap;

class NaviStreamSession : public NaviWorkerBase
{
public:
    NaviStreamSession(TaskQueue *taskQueue,
                      BizManager *bizManager,
                      NaviMessage *request,
                      const ArenaPtr &arena,
                      const multi_call::GigStreamBasePtr &stream);
    ~NaviStreamSession();
private:
    NaviStreamSession(const NaviStreamSession &);
    NaviStreamSession &operator=(const NaviStreamSession &);
public:
    bool doInit(GraphParam *param) override;
    void run() override;
    void drop() override;
private:
    void startTrace(const std::shared_ptr<multi_call::QuerySession> &querySession) const;
    bool decompressGraph();
    void addMirrorGraph(MirrorGraphInfoMap &mirrorInfoMap);
    SubGraphDef *getNewSubGraphDef(GraphId graphId,
                                   std::unordered_map<GraphId, SubGraphDef *> &graphMap);
    void mirrorBorder(GraphId graphId, const BorderDef &borderDef,
                      SubGraphDef *subGraphDef);
    void initDomainMap(const MirrorGraphInfoMap &mirrorInfoMap,
                       const multi_call::GigStreamBasePtr &stream,
                       Graph *graph);
    bool isTimeout() const override;

private:
    ArenaPtr _arena;
    NaviMessage *_request;
    multi_call::GigStreamBasePtr _stream;
    GraphDef *_graphDef;
    Graph *_graph;
};

}

#endif //NAVI_NAVI_STREAM_SESSION_H
