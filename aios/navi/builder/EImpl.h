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
#include "navi/builder/GraphDesc.h"

namespace navi {

class GraphBuilder;
class PImpl;

class EImpl
{
public:
    EImpl();
    virtual ~EImpl();
    EImpl(const EImpl &) = delete;
    EImpl &operator=(const EImpl &) = delete;
public:
    bool init(GraphBuilder *builder, GraphId peerGraphId, P from);
    virtual SingleE *addDownStream(P to) = 0;
    virtual N split(const std::string &splitKernel) = 0;
    virtual N merge(const std::string &mergeKernel) = 0;
public:
    static EImpl *createEdge(GraphBuilder *builder, GraphId peerGraphId, P from);
    static std::string getPortName(P p);
protected:
    GraphBuilder *_builder;
    GraphId _fromGraphId;
    P _from;
    std::string _fromPort;
    GraphId _toGraphId;
    NImplBase *_split;
    NImplBase *_merge;
};

}

