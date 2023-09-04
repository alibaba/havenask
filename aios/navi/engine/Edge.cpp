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
#include "navi/engine/Edge.h"
#include "navi/engine/Node.h"
#include "navi/log/NaviLogger.h"

namespace navi {

Edge::Edge(const NaviLoggerPtr &logger)
    : _logger(this, "edge", logger)
    , _inputDef(nullptr)
    , _inputNode(nullptr)
    , _inputReadyMap(nullptr)
    , _forceEof(false)
    , _eof(false)
    , _slotReadyMap(nullptr)
    , _typeIdOutputIndex(INVALID_INDEX)
    , _upStream(nullptr)
    , _upStreamIndex(INVALID_INDEX)
    , _overrideIndex(INVALID_OVERRIDE_INDEX)
{
}

Edge::~Edge() {
    ReadyBitMap::freeReadyBitMap(_slotReadyMap);
}

bool Edge::init(const LocalSubGraph &graph, const NodePortDef &inputDef) {
    const auto &nodeName = inputDef.node_name();
    const auto &portName = inputDef.port_name();
    _logger.addPrefix("%s:%s", nodeName.c_str(), portName.c_str());
    Node *inputNode = nullptr;
    if (NAVI_FORK_NODE != nodeName) {
        inputNode = graph.getNode(nodeName);
        if (!inputNode) {
            NAVI_LOG(ERROR, "input node [%s] not found", nodeName.c_str());
            return false;
        }
        if (!inputNode->addOutput(this, portName, _inputIndex, _typeId)) {
            return false;
        }
    }
    _inputNode = inputNode;
    _inputDef = &inputDef;
    return true;
}

bool Edge::addOutput(const LocalSubGraph &graph,
                     const NodePortDef &outputDef,
                     bool require)
{
    const auto &nodeName = outputDef.node_name();
    if (NAVI_FORK_NODE == nodeName) {
        IndexType index = _outputSlots.size();
        EdgeOutputInfo edgeOutputInfo(this, index);
        _outputSlots.push_back(OutputSlot());
        auto &slot = _outputSlots.back();
        slot.outputDef = &outputDef;
        return true;
    }
    auto outputNode = graph.getNode(nodeName);
    if (!outputNode) {
        NAVI_LOG(ERROR, "edge output node [%s] not found", nodeName.c_str());
        return false;
    }
    std::string outputTypeId;
    IndexType index = _outputSlots.size();
    EdgeOutputInfo edgeOutputInfo(this, index);
    _outputSlots.push_back(OutputSlot());
    auto &slot = _outputSlots.back();
    slot.outputDef = &outputDef;
    slot.outputNode = outputNode;
    if (!outputNode->addInput(outputDef.port_name(), require, edgeOutputInfo,
                              slot.outputIndex, outputTypeId))
    {
        return false;
    }
    if (_typeId.empty()) {
        _typeId = outputTypeId;
        _typeIdOutputIndex = index;
    } else if (!outputTypeId.empty() && _typeId != outputTypeId) {
        if (INVALID_INDEX != _typeIdOutputIndex) {
            const auto &typeIdSlot = _outputSlots[_typeIdOutputIndex];
            NAVI_LOG(ERROR,
                     "downstream data type mismatch, input kernel[%s], output "
                     "node 1 [node %s kernel %s] port[%s] type[%s], output node 2 "
                     "[node %s kernel %s] port[%s] type[%s]",
                     getInputNode()->getKernelName().c_str(),
                     typeIdSlot.outputDef->node_name().c_str(),
                     typeIdSlot.outputNode->getKernelName().c_str(),
                     typeIdSlot.outputDef->port_name().c_str(), _typeId.c_str(),
                     outputDef.node_name().c_str(),
                     outputNode->getKernelName().c_str(),
                     outputDef.port_name().c_str(), outputTypeId.c_str());
        } else {
            NAVI_LOG(ERROR,
                     "edge data type mismatch, input kernel[%s] type[%s], "
                     "output [node %s kernel %s] port[%s] type[%s]",
                     getInputNode()->getKernelName().c_str(), _typeId.c_str(),
                     outputDef.node_name().c_str(),
                     outputNode->getKernelName().c_str(),
                     outputDef.port_name().c_str(), outputTypeId.c_str());
        }
        return false;
    }
    return true;
}

bool Edge::checkType(const CreatorManager *creatorManager) const {
    if (!_typeId.empty() && !creatorManager->getType(_typeId)) {
        NAVI_LOG(ERROR, "port data type[%s] not registered", _typeId.c_str());
        return false;
    }
    return true;
}

void Edge::postInit() {
    if (_inputNode) {
        _inputReadyMap = _inputNode->getOutputBitMap(_inputIndex);
    }
    for (auto &slot : _outputSlots) {
        if (!slot.outputNode) {
            // fork slot
            continue;
        }
        slot.outputReadyMap =
            slot.outputNode->getInputBitMap(slot.outputIndex);
    }
    _slotReadyMap = ReadyBitMap::createReadyBitMap(_outputSlots.size());
    initSlotReadyMap();
}

const NodePortDef *Edge::getInputDef() const {
    if (_upStream) {
        return _upStream->getInputDef();
    } else {
        return _inputDef;
    }
}

const NodePortDef *Edge::getOutputDef(IndexType slotId) const {
    return _outputSlots[slotId].outputDef;
}

Node *Edge::getInputNode() const {
    if (_upStream) {
        return _upStream->getInputNode();
    }
    return _inputNode;
}

bool Edge::isEof() const {
    if (_upStream) {
        return _upStream->isEof();
    }
    return _inputReadyMap->isFinish(_inputIndex.index);
}

void Edge::bindInputNode(const EdgeOutputInfo &upStreamOutputInfo) {
    auto upStreamEdge = upStreamOutputInfo.getEdge();
    auto upStreamSlotId = upStreamOutputInfo.getSlotId();
    auto &slot = upStreamEdge->_outputSlots[upStreamSlotId];
    slot.downStream = this;
    slot.outputNode = _inputNode;
    slot.outputIndex = _inputIndex;
    slot.outputReadyMap = _inputReadyMap;
    _upStream = upStreamEdge;
    _upStreamIndex = upStreamSlotId;
}

void Edge::bindOutputNode(IndexType slotId, Edge *downStream) {
    downStream->_upStream = this;
    downStream->_upStreamIndex = slotId;
    auto &slot = _outputSlots[slotId];
    slot.downStream = downStream;
}

bool Edge::hasDownStream(IndexType slotId) const {
    return _outputSlots[slotId].downStream != nullptr;
}

bool Edge::hasUpStream() const {
    return _upStream != nullptr;
}

EdgeOutputInfo Edge::getUpStreamOutputInfo() const {
    return EdgeOutputInfo(_upStream, _upStreamIndex);
}

Edge *Edge::getDownStream(IndexType slotId) const {
    return _outputSlots[slotId].downStream;
}

void Edge::flushData() {
    DataPtr data;
    bool eof = false;
    doGetData(data, eof);
    assert(_upStream);
    if (_upStream->slotHasData(_upStreamIndex)) {
        notifyData(eof, nullptr);
    }
}

bool Edge::checkDataType(const DataPtr &data) const {
    if (data && !_typeId.empty()) {
        const auto &dataType = data->getTypeId();
        if (_typeId != dataType) {
            NAVI_LOG(ERROR, "edge data type mismatch, expect[%s], actual[%s]",
                     _typeId.c_str(), dataType.c_str());
            return false;
        }
    }
    return true;
}

void Edge::setEof(const Node *callNode) {
    setData(callNode, nullptr, true);
}

void Edge::setForceEofFlag() {
    _forceEof = true;
    auto slotCount = _outputSlots.size();
    for (size_t i = 0; i < slotCount; i++) {
        const auto &slot = _outputSlots[i];
        if (slot.downStream) {
            slot.downStream->setForceEofFlag();
        }
    }
}

void Edge::forceEof() {
    setForceEofFlag();
    notifyData(true, nullptr);
}

bool Edge::setData(const Node *callNode, const DataPtr &data, bool eof) {
    if (isOverride()) {
        return true;
    }
    return doSetData(callNode, data, eof);
}

bool Edge::doSetData(const Node *callNode, const DataPtr &data, bool eof) {
    assert(!_upStream);
    assert(_inputNode);
    if (!checkDataType(data)) {
        return false;
    }
    if (_eof) {
        NAVI_LOG(ERROR, "data after eof!");
        return false;
    }
    if (!allSlotEmpty()) {
        NAVI_LOG(ERROR, "data override!");
        return false;
    }
    NAVI_LOG(SCHEDULE1, "set data [%p] eof [%d], call node: [%p, node %s kernel %s]",
             data.get(), eof, callNode,
             callNode ? callNode->getName().c_str() : "",
             callNode ? callNode->getKernelName().c_str() : "");
    _data = data;
    if (eof) {
        _eof = true;
    }
    if (!isOverride()) {
        if (eof) {
            _inputReadyMap->setFinish(_inputIndex.index, true);
        }
        _inputReadyMap->setReady(_inputIndex.index, false);
        _inputNode->incNodeSnapshot();
    }
    if (isAllSlotFinished()) {
        return true;
    }
    notifyData(_eof, callNode);
    return true;
}

// for root edge
void Edge::notifyData(bool eof, const Node *callNode) const {
    setSlotHasData();
    auto slotCount = _outputSlots.size();
    for (size_t i = 0; i < slotCount; i++) {
        if (isSlotFinished(i)) {
            continue;
        }
        const auto &slot = _outputSlots[i];
        if (slot.downStream) {
            slot.downStream->notifyData(eof, callNode);
        }
        if (slot.outputReadyMap) {
            slot.outputReadyMap->setReady(slot.outputIndex.index, true);
            if (eof || _forceEof) {
                slot.outputReadyMap->setFinish(slot.outputIndex.index, true);
            }
            NAVI_LOG(SCHEDULE2,
                     "notify with data[%p] eof[%d], idx[%ld,%s] readyMap[%s]",
                     _data.get(),
                     eof,
                     i,
                     autil::StringUtil::toString(slot.outputIndex).c_str(),
                     autil::StringUtil::toString(slot.outputReadyMap).c_str());
            slot.outputNode->incNodeSnapshot();
            slot.outputNode->schedule(callNode);
        }
    }
}

// for downstream
bool Edge::getData(IndexType index, DataPtr &data, bool &eof,
                   const Node *callNode)
{
    if (isSlotFinished(index)) {
        NAVI_LOG(
            SCHEDULE1,
            "get data failed, finished by downstream, index [%d], output [%s]",
            index, getOutputName(index).c_str());
        return false;
    }
    const auto &slot = _outputSlots[index];
    if (!slot.downStream && slot.outputReadyMap) {
        if (!slot.outputReadyMap->isReady(slot.outputIndex.index)) {
            NAVI_LOG(SCHEDULE1,
                     "get data failed, data not ready, index [%d], output [%s]",
                     index, getOutputName(index).c_str());
            return false;
        }
        slot.outputReadyMap->setReady(slot.outputIndex.index, false);
        slot.outputNode->incNodeSnapshot();
    }
    doGetData(data, eof);
    eof |= _forceEof;
    clearSlot(index, false, callNode);
    NAVI_LOG(SCHEDULE1,
             "getData success, data [%p], output [%s], index [%d], eof [%d], forceEof [%d]",
             data.get(), getOutputName(index).c_str(), index, eof, _forceEof);
    return true;
}

void Edge::doGetData(DataPtr &data, bool &eof) {
    if (!_upStream) {
        data = _data;
        eof = getDataEof();
    } else {
        _upStream->doGetData(data, eof);
    }
}

// for downstream
void Edge::setOutputSlotEof(IndexType slotId, const Node *callNode) {
    if (isSlotFinished(slotId)) {
        return;
    }
    const auto &slot = _outputSlots[slotId];
    if (!slot.downStream && slot.outputReadyMap) {
        slot.outputReadyMap->setReady(slot.outputIndex.index, true);
        slot.outputReadyMap->setFinish(slot.outputIndex.index, true);
    }
    clearSlot(slotId, true, callNode);
}

void Edge::clearSlot(IndexType slotId, bool setFinish, const Node *callNode) {
    bool allDataClear = false;
    bool allSlotFinish =  false;
    {
        autil::ScopedLock lock(_lock);
        auto oldDataClear = allSlotEmpty();
        clearSlotData(slotId);
        if (setFinish) {
            auto oldAllSlotFinish = isAllSlotFinished();
            finishSlotData(slotId);
            allSlotFinish = !oldAllSlotFinish && isAllSlotFinished();
        }
        allDataClear = !oldDataClear && allSlotEmpty();
    }
    if (!_upStream) {
        if (allSlotFinish) {
            _inputReadyMap->setFinish(_inputIndex.index, true);
        }
        if (allDataClear || allSlotFinish) {
            clearData(callNode);
        }
    } else {
        if (allDataClear || allSlotFinish) {
            _upStream->clearSlot(_upStreamIndex, allSlotFinish, callNode);
        }
        if (setFinish && allSlotFinish && _inputNode && _inputReadyMap) {
            // fork output edge
            _inputReadyMap->setFinish(_inputIndex.index, true);
            _inputNode->incNodeSnapshot();
            _inputNode->schedule(callNode);
        }
    }
}

bool Edge::clearData(const Node *callNode) {
    NAVI_LOG(SCHEDULE1, "clear data, call node: [%p, node %s kernel %s]", callNode,
             callNode ? callNode->getName().c_str() : "",
             callNode ? callNode->getKernelName().c_str() : "");
    _data.reset();
    if (!isOverride()) {
        _inputReadyMap->setReady(_inputIndex.index, true);
        NAVI_LOG(SCHEDULE1, "schedule call by node [%s] getData",
                 callNode ? callNode->getName().c_str() : "");
        _inputNode->incNodeSnapshot();
        _inputNode->schedule(callNode);
    } else {
        if (!getDataEof() && !flushOverrideData()) {
            return false;
        }
    }
    return true;
}

bool Edge::getDataEof() const {
    return _eof || _forceEof;
}

std::string Edge::getOutputName(IndexType index) const {
    assert(index < _outputSlots.size());
    auto outputDef = _outputSlots[index].outputDef;
    return outputDef->node_name() + ":" + outputDef->port_name();
}

std::string Edge::getDebugName(IndexType index) const {
    assert(index < _outputSlots.size());
    const NodePortDef *inputDef = getInputDef();
    return inputDef->node_name() + ":" + inputDef->port_name() + " -> " +
           getOutputName(index);
}

bool Edge::overrideData(const std::vector<DataPtr> &datas) {
    assert(!_upStream);
    if (isOverride()) {
        NAVI_LOG(ERROR, "multiple edge data override specified");
        return false;
    }
    _overrideIndex = 0;
    for (const auto &data : datas) {
        _overrideDatas.push_back(data);
    }
    _inputReadyMap->setFinish(_inputIndex.index, true);
    _inputReadyMap->setReady(_inputIndex.index, true);
    _inputNode->incNodeSnapshot();
    return flushOverrideData();
}

bool Edge::flushOverrideData() {
    assert(isOverride());
    auto dataCount = _overrideDatas.size();
    if (_overrideIndex > dataCount) {
        NAVI_LOG(ERROR, "override index overflow, dataCount: %lu, index: %lu",
                 dataCount, _overrideIndex);
        return false;
    }
    DataPtr data;
    auto index = _overrideIndex++;
    if (index < dataCount) {
        data = _overrideDatas[index];
    }
    auto eof = _overrideIndex >= dataCount;
    if (eof) {
        _overrideDatas.clear();
    }
    NAVI_LOG(SCHEDULE1, "override output data, index [%lu] data [%p] eof [%d]",
             index, data.get(), eof);
    return doSetData(nullptr, data, eof);
}

bool Edge::isOverride() const {
    return INVALID_OVERRIDE_INDEX != _overrideIndex;
}

void Edge::startUpStreamSchedule(const Node *callNode) const {
    if (_upStream) {
        _upStream->startUpStreamSchedule(callNode);
    } else {
        if (_inputNode) {
            _inputNode->incNodeSnapshot();
            _inputNode->startSchedule(callNode);
        }
    }
}

}
