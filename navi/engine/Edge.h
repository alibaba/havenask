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
#ifndef NAVI_EDGE_H
#define NAVI_EDGE_H

#include "navi/common.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/proto/GraphDef.pb.h"
#include "autil/Lock.h"

namespace navi {

class NaviLogger;
class Node;
class ReadyBitMap;
class EdgeOutputInfo;

struct OutputSlot {
public:
    OutputSlot()
        : outputDef(nullptr)
        , outputNode(nullptr)
        , outputIndex(INVALID_INDEX)
        , outputReadyMap(nullptr)
        , downStream(nullptr)
    {
    }
public:
    const NodePortDef *outputDef;
    Node *outputNode;
    PortIndex outputIndex;
    ReadyBitMap *outputReadyMap;
    Edge *downStream;
};

class Edge
{
public:
    Edge(const NaviLoggerPtr &logger, const PoolPtr &pool);
    ~Edge();
private:
    Edge(const Edge &);
    Edge& operator=(const Edge &);
public:
    bool init(const LocalSubGraph &graph, const NodePortDef &inputDef);
    bool addOutput(const LocalSubGraph &graph, const NodePortDef &outputDef,
                   bool require);
    void postInit(const PoolPtr &pool);
    const NodePortDef *getInputDef() const;
    const NodePortDef *getOutputDef(IndexType slotId) const;
    Node *getInputNode() const;
    Node *getOutputNode() const;
    bool isEof() const;
    void setEof(const Node *callNode);
    bool setData(const Node *callNode, const DataPtr &data, bool eof);
    bool getData(IndexType index, DataPtr &data, bool &eof,
                 const Node *callNode);
    void setOutputSlotEof(IndexType slotId, const Node *callNode);
    void setInputIndex(const PortIndex &inputIndex) {
        _inputIndex = inputIndex;
    }
    const PortIndex &getInputIndex() const {
        return _inputIndex;
    }
    const std::vector<OutputSlot> &getOutputSlots() const {
        return _outputSlots;
    }
    size_t getOutputDegree() const {
        return _outputSlots.size();
    }
    void bindInputNode(const EdgeOutputInfo &upStreamOutputInfo);
    void bindOutputNode(IndexType slotId, Edge *downStream);
    bool hasDownStream(IndexType slotId) const;
    bool hasUpStream() const;
    void flushData();
    bool checkType(const CreatorManager *creatorManager) const;
    std::string getOutputName(IndexType index) const;
    std::string getDebugName(IndexType index) const;
    bool overrideData(const std::vector<DataPtr> &datas);
private:
    bool checkDataType(const DataPtr &data) const;
    bool doSetData(const Node *callNode, const DataPtr &data, bool eof);
    void doGetData(DataPtr &data, bool &eof);
    bool clearData(const Node *callNode);
    bool flushOverrideData();
    bool isOverride() const;
    void initSlotReadyMap() {
        _slotReadyMap->setFinish(false);
        _slotReadyMap->setOptional(false);
        _slotReadyMap->setReady(true);
    }
    void setSlotHasData() const {
        _slotReadyMap->setReady(false);
    }
private:
    bool slotHasData(IndexType index) const {
        return !_slotReadyMap->isReady(index);
    }
    void clearSlotData(IndexType index) const {
        _slotReadyMap->setReady(index, true);
    }
    bool allSlotEmpty() const {
        return _slotReadyMap->isOk();
    }
    void finishSlotData(IndexType index) const {
        _slotReadyMap->setFinish(index, true);
    }
public:
    bool isSlotFinished(IndexType index) const {
        return _slotReadyMap->isFinish(index);
    }
private:
    bool isAllSlotFinished() const {
        return _slotReadyMap->isFinish();
    }
    void clearSlot(IndexType slotId, bool setFinish, const Node *callNode);
    void notifyData(bool eof, const Node *callNode) const;
private:
    DECLARE_LOGGER();
    const NodePortDef *_inputDef;
    Node *_inputNode;
    PortIndex _inputIndex;
    ReadyBitMap *_inputReadyMap;
    std::string _typeId;
    mutable autil::ThreadMutex _lock;
    bool _eof;
    DataPtr _data;
    std::vector<OutputSlot> _outputSlots;
    ReadyBitMap *_slotReadyMap;
    IndexType _typeIdOutputIndex;
    Edge *_upStream;
    IndexType _upStreamIndex;
    std::vector<DataPtr> _overrideDatas;
    size_t _overrideIndex;
private:
    static constexpr size_t INVALID_OVERRIDE_INDEX = -1;
};

NAVI_TYPEDEF_PTR(Edge);

class EdgeOutputInfo {
public:
    EdgeOutputInfo()
        : EdgeOutputInfo(nullptr, INVALID_INDEX)
    {
    }
    EdgeOutputInfo(Edge *edge,
                   IndexType slotId)
        : _edge(edge)
        , _slotId(slotId)
    {
    }
public:
    bool isValid() const {
        return _edge != nullptr && _slotId != INVALID_INDEX;
    }
    Edge *getEdge() const {
        return _edge;
    }
    IndexType getSlotId() const {
        return _slotId;
    }
    bool hasDownStream() const {
        return _edge->hasDownStream(_slotId);
    }
    bool finished() const {
        return _edge->isSlotFinished(_slotId);
    }
    bool getData(DataPtr &data, bool &eof, const Node *callNode) const {
        assert(isValid());
        return _edge->getData(_slotId, data, eof, callNode);
    }
    std::string getDebugName() const {
        assert(isValid());
        return _edge->getDebugName(_slotId);
    }
    const std::string &getInputNodeName() const {
        return _edge->getInputDef()->node_name();
    }
    const std::string &getOutputPortName() const {
        return _edge->getOutputDef(_slotId)->port_name();
    }
    void setEof(const Node *callNode) const {
        _edge->setOutputSlotEof(_slotId, callNode);
    }
private:
    Edge *_edge;
    IndexType _slotId;
};

}

#endif //NAVI_EDGE_H
