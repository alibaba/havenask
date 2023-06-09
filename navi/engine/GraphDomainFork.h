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
#ifndef NAVI_GRAPHDOMAINFORK_H
#define NAVI_GRAPHDOMAINFORK_H

#include "navi/engine/Edge.h"
#include "navi/engine/GraphDomain.h"

namespace navi {

class Node;
class Edge;

class GraphDomainFork : public GraphDomain
{
public:
    GraphDomainFork(Graph *graph, const BizPtr &biz, Node *node,
                    NaviPartId partCount, NaviPartId partId);
    ~GraphDomainFork();
private:
    GraphDomainFork(const GraphDomainFork &);
    GraphDomainFork &operator=(const GraphDomainFork &);
public:
    bool postInit() override;
    bool run() override;
    void notifySend(NaviPartId partId) override;
public:
    BizPtr getBiz() const;
    bool bindGraphEdgeList(const std::unordered_map<std::string, Edge *> &edges);
    Edge *getInputPeer(const std::string &portName) const;
    EdgeOutputInfo getOutputPeer(const std::string &portName) const;
    bool checkBind() const;
    void flushData() const;
private:
    bool initPortDataType();
    void initPartCount();
    Edge *getEdge(const std::unordered_map<std::string, Edge *> &edgeMap,
                  const std::string &port) const;
private:
    BizPtr _biz;
    Node *_node;
    NaviPartId _partCount;
    NaviPartId _partId;
    std::unordered_map<std::string, Edge *> _inputEdgeMap;
    std::unordered_map<std::string, EdgeOutputInfo> _outputEdgeMap;
};

}

#endif //NAVI_GRAPHDOMAINFORK_H
