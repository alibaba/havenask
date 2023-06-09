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
#ifndef NAVI_GRAPHDOMAIN_H
#define NAVI_GRAPHDOMAIN_H

#include <unordered_map>

#include "autil/ObjectTracer.h"
#include "navi/builder/BorderId.h"
#include "navi/common.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/SubGraphBorder.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

class Graph;
class SubGraphDef;
class Port;

enum GraphDomainType {
    GDT_USER = 0,
    GDT_LOCAL = 1,
    GDT_CLIENT = 2,
    GDT_SERVER = 3,
    GDT_FORK = 4,
};

enum GraphDomainHoldReason {
    GDHR_DOMAIN_HOLDER,
    GDHR_INIT,
    GDHR_NOTIFY_FINISH_ASYNC,
    GDHR_NOTIFY_SEND,
    GDHR_COUNT,
};

class GraphDomain : public autil::ObjectTracer<GraphDomain>
{
public:
    GraphDomain(Graph *graph, GraphDomainType type);
    virtual ~GraphDomain();
private:
    GraphDomain(const GraphDomain &);
    GraphDomain &operator=(const GraphDomain &);
public:
    virtual bool preInit();
    virtual bool postInit();
    virtual bool run() = 0;
    virtual void notifySend(NaviPartId partId);
    virtual void incMessageCount(NaviPartId partId);
    virtual void decMessageCount(NaviPartId partId);
    void notifyFinish(ErrorCode ec, bool notifyGraph);
    bool finished() const;
    GraphParam *getGraphParam() const;
    bool isLocalBorder(GraphId peer) const;
    GraphDomainType getType() const;
    friend std::ostream& operator<<(std::ostream& os,
                                    const GraphDomain &domain);
public:
    void addSubGraph(SubGraphDef *subGraphDef);
    void addSubBorder(const SubGraphBorderPtr &border);
    Port *getPort(PortId portId) const;
    const std::unordered_map<PortId, Port *> &getPortMap() const;
    bool acquire(GraphDomainHoldReason reason);
    NaviWorkerBase *release(GraphDomainHoldReason reason);
    const NaviObjectLogger &getLogger() const {
        return _logger;
    }
public:
    static const char *getGraphDomainType(GraphDomainType type);
protected:
    virtual void doFinish(ErrorCode ec);
protected:
    bool containGraph(GraphId graphId) const;
    bool initPeerInfo(SubGraphDef *subGraphDef);
private:
    void collectPort(const SubGraphBorderPtr &border);
    void finishGraph(ErrorCode ec, bool notifyGraph);
    SubGraphBorder *getPeerBorder(const BorderDef *borderDef) const;
protected:
    DECLARE_LOGGER();
    Graph *_graph;
    GraphParam *_param;
    PoolPtr _pool;
    GraphDomainType _type;
    std::vector<SubGraphDef *> _subGraphs;
    std::unordered_set<GraphId> _graphIdSet;
    std::atomic<bool> _terminated;
    autil::ThreadMutex _acquireLock;
    int64_t _acquireCount;
    SubGraphBorderMap _inputBorderMap;
    SubGraphBorderMap _outputBorderMap;
    SubGraphBorderMap _remoteInputBorderMap;
    SubGraphBorderMap _remoteOutputBorderMap;
    std::unordered_map<PortId, Port *> _portMap;
};

NAVI_TYPEDEF_PTR(GraphDomain);

typedef std::unordered_map<GraphId, GraphDomainPtr> GraphDomainMap;
NAVI_TYPEDEF_PTR(GraphDomainMap);

extern std::ostream& operator<<(std::ostream& os,
                                const GraphDomain &domain);

}

#endif //NAVI_GRAPHDOMAIN_H
