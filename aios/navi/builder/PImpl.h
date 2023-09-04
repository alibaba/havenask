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

#include <unordered_set>
#include "navi/builder/GraphDesc.h"
#include "navi/common.h"

namespace navi {

class NImplBase;

class PImpl
{
public:
    PImpl(NImplBase *node, const std::string &port, bool input);
    ~PImpl();
    PImpl(const PImpl &) = delete;
    PImpl &operator=(const PImpl &) = delete;
public:
    NImplBase *getNode() const;
    const std::string &getNodeName() const;
    const std::string &getPort() const;
    GraphId graphId() const;
    E from(IndexType thisIndex, P peer);
    E to(IndexType thisIndex, P peer);
    E fromForkNodeInput(IndexType thisIndex, const std::string &input, IndexType index);
    E toForkNodeOutput(IndexType thisIndex, const std::string &output, IndexType index);
    E asGraphOutput(IndexType thisIndex, const std::string &name);
    void setConnected(IndexType inputIndex, P from);
    void setRealPort(P real);
    P getRealPort() const;
public:
    P asGroup();
    P next(IndexType index);
    P index(IndexType index);
    P autoNext();
    bool hasConnected() const;
private:
    static uint64_t getEdgeKey(IndexType index, GraphId graphId) {
        return (((uint64_t)index) << 32) + ((uint32_t)graphId);
    }
public:
    friend std::ostream &operator<<(std::ostream &os, const P &p);
private:
    NImplBase *_node;
    const std::string &_nodeName;
    std::string _port;
    bool _input;
    bool _group;
    IndexType _currentIndex;
    std::unordered_map<IndexType, P> _connectedInputSet;
    std::unordered_map<uint64_t, EImpl *> _edgeMap;
    P _realPort;
};

}
