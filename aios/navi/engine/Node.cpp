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
#include "navi/engine/Node.h"
#include "autil/TimeUtility.h"
#include "navi/engine/Biz.h"
#include "navi/engine/Edge.h"
#include "navi/engine/Graph.h"
#include "navi/engine/GraphDomainFork.h"
#include "navi/engine/InputPortGroup.h"
#include "navi/engine/LocalPortBase.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/OutputPortGroup.h"
#include "navi/engine/ScopeTerminatorKernel.h"
#include "navi/log/NaviLogger.h"
#include "navi/ops/ResourceData.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/ReadyBitMap.h"
#include <iostream>
#include <limits>
#include "aios/network/gig/multi_call/common/common.h"
#include "lockless_allocator/LocklessApi.h"

namespace navi {

class ComputeScope {
public:
    ComputeScope(KernelMetric *metric,
                 GraphEventType type,
                 NaviPartId partId,
                 NaviWorkerBase *worker,
                 const ScheduleInfo &schedInfo)
        : _metric(metric)
        , _type(type)
        , _partId(partId)
        , _worker(worker)
        , _schedInfo(schedInfo)
        , _beginTime(CommonUtil::getTimelineTimeNs())
        , _endTime(0)
        , _collectPerf(worker->collectPerf())
        , _prevPoolMallocCount(mySwapPoolMallocCount(0))
        , _poolMallocCount(0)
        , _prevPoolMallocSize(mySwapPoolMallocSize(0))
        , _poolMallocSize(0)
    {
        if (_collectPerf) {
            _worker->beginSample();
            getrusage(RUSAGE_THREAD, &_usageBegin);
        }
        _metric->incScheduleCount();
    }
    float finish() {
        stop();
        return (_endTime - _beginTime) / 1000.0f;
    }
    void stop() {
        if (_endTime == 0) {
            if (_collectPerf) {
                getrusage(RUSAGE_THREAD, &_usageEnd);
                _result = _worker->endSample();
            }
            _endTime = CommonUtil::getTimelineTimeNs();
            _poolMallocCount = mySwapPoolMallocCount(_prevPoolMallocCount);
            _poolMallocSize = mySwapPoolMallocSize(_prevPoolMallocSize);
        }
    }
    size_t mySwapPoolMallocSize(size_t size) {
        return swapPoolMallocSize(size);
    }

    size_t mySwapPoolMallocCount(size_t count) {
        return swapPoolMallocCount(count);
    }
    ~ComputeScope() {
        stop();
        _metric->reportTime((int32_t)_type, _partId, _beginTime, _endTime,
                            _usageBegin, _usageEnd, _schedInfo, _result,
                            _poolMallocCount, _poolMallocSize);
    }
private:
    MallocPoolScope mallocScope;
    KernelMetric *_metric;
    GraphEventType _type;
    NaviPartId _partId;
    NaviWorkerBase *_worker;
    const ScheduleInfo &_schedInfo;
    int64_t _beginTime;
    int64_t _endTime;
    bool _collectPerf;
    struct rusage _usageBegin;
    struct rusage _usageEnd;
    NaviPerfResultPtr _result;
    size_t _prevPoolMallocCount;
    size_t _poolMallocCount;
    size_t _prevPoolMallocSize;
    size_t _poolMallocSize;
};

class DataDestructItem : public NaviNoDropWorkerItem
{
public:
    DataDestructItem(NaviWorkerBase *worker,
                     DataPtr &&data)
        : NaviNoDropWorkerItem(worker)
        , _data(data)
    {
    }
public:
    void doProcess() override {
        if (_data) {
            _data.reset();
        }
    }
private:
    DataPtr _data;
};

class KernelDestructItem : public NaviNoDropWorkerItem
{
public:
    KernelDestructItem(NaviWorkerBase *worker,
                       LocalSubGraph *graph,
                       KernelMetric *metric,
                       Kernel *kernel)
        : NaviNoDropWorkerItem(worker)
        , _graph(graph)
        , _metric(metric)
        , _kernel(kernel)
    {
    }
public:
    void doProcess() override {
        if (_kernel) {
            ComputeScope scope(_metric, GET_KERNEL_DELETE_KERNEL,
                               _graph->getPartId(), _worker, _schedInfo);
            delete _kernel;
            _kernel = nullptr;
        }
    }
private:
    LocalSubGraph *_graph;
    KernelMetric *_metric;
    Kernel *_kernel;
};

Node::Node(const NaviLoggerPtr &logger, Biz *biz,
           KernelMetric *metric, LocalSubGraph *graph)
    : _logger(this, "node", logger)
    , _scope(0)
    , _biz(biz)
    , _graph(graph)
    , _port(nullptr)
    , _metric(metric)
    , _inputReadyMap(nullptr)
    , _inputGroupReadyMap(nullptr)
    , _outputReadyMap(nullptr)
    , _controlOutputReadyMap(nullptr)
    , _controlInputGroupIndex(INVALID_INDEX)
    , _resourceInputGroupIndex(INVALID_INDEX)
    , _outputSnapshotVec(nullptr)
    , _outputDegree(0)
    , _scheduleStat(SS_INIT)
    , _def(nullptr)
    , _kernelCreator(nullptr)
    , _kernel(nullptr)
    , _kernelBaseAddr(nullptr)
    , _dependResourceMap(nullptr)
    , _creatorStats(nullptr)
    , _forceStopNode(nullptr)
    , _forkGraphDef(nullptr)
    , _forkGraph(nullptr)
{
    _hasInput = false;
    _hasOutput = false;
    _skipConfig = false;
    _skipInit = false;
    _stopAfterInit = false;
    _skipDeleteKernel = false;
    _isResourceNode = false;
    _inlineConfigAndInit = false;
    _isInputBorder = false;
    _isOutputBorder = false;
    _kernelInited = false;
    _frozen = false;
    _stopSchedule = false;
    _forceStop = false;
    atomic_set(&_nodeSnapshot, 0);
}

Node::~Node() {
    delete _forkGraph;
    multi_call::freeProtoMessage(_forkGraphDef);
    deleteKernel(true);
    ReadyBitMap::freeReadyBitMap(_inputReadyMap);
    ReadyBitMap::freeReadyBitMap(_inputGroupReadyMap);
    ReadyBitMap::freeReadyBitMap(_outputReadyMap);
    ReadyBitMap::freeReadyBitMap(_controlOutputReadyMap);
    ReadyBitMap::freeReadyBitMap(_outputSnapshotVec);
    for (auto group : _inputGroups) {
        delete group;
    }
    for (auto group : _outputGroups) {
        delete group;
    }
}

bool Node::init(const NodeDef &nodeDef) {
    if (!initDef(nodeDef)) {
        return false;
    }
    NaviLoggerScope scope(_logger);
    _kernelCreator = _biz->getKernelCreator(getKernelName());
    if (!_kernelCreator) {
        auto ec = EC_CREATE_KERNEL;
        _graph->setErrorCode(ec);
        NAVI_LOG(ERROR, "kernel not supported, ec[%s]",
                 CommonUtil::getErrorString(ec));
        return false;
    }
    _inputs.resize(_kernelCreator->inputSize());
    _outputs.resize(_kernelCreator->outputSize());
    _controlOutputs.resize(_kernelCreator->controlOutputSize());
    _inputReadyMap = ReadyBitMap::createReadyBitMap(_inputs.size());
    _inputGroupReadyMap = ReadyBitMap::createReadyBitMap(_kernelCreator->inputGroupSize() + 2);
    _outputReadyMap = ReadyBitMap::createReadyBitMap(_outputs.size());
    _controlOutputReadyMap =
        ReadyBitMap::createReadyBitMap(_controlOutputs.size());
    initIOGroup();
    if (!_outputs.empty() || !_outputGroups.empty()) {
        setHasOutput();
    }
    if (!initCreatorStats()) {
        return false;
    }
    if (!initDependResourceMap()) {
        return false;
    }
    return true;
}

bool Node::initDef(const NodeDef &nodeDef) {
    _def = &nodeDef;
    _name = _def->name();
    _kernelName = _def->kernel_name();
    _scope = _def->scope();
    if (_name.empty()) {
        NAVI_LOG(ERROR, "node name can't be empty, def: %s",
                 _def->DebugString().c_str());
        return false;
    }
    _isResourceNode = (_kernelName == RESOURCE_CREATE_KERNEL);
    if (_isResourceNode) {
        _inlineConfigAndInit = true;
    }
    initBuildinAttrs();
    _logger.addPrefix("graph %d %s kernel %s", _graph->getGraphId(), _name.c_str(),
                      _kernelName.c_str());
    return true;
}

void Node::initBuildinAttrs() {
    if (!_def->has_buildin_attrs()) {
        return;
    }
    const auto &buildinAttrs = _def->buildin_attrs();
    _skipConfig = buildinAttrs.skip_config();
    _skipInit = buildinAttrs.skip_init();
    _stopAfterInit = buildinAttrs.stop_after_init();
    _skipDeleteKernel = buildinAttrs.skip_delete_kernel();
}

void Node::initIOGroup() {
    const auto *kernelDef = _kernelCreator->def();
    for (auto i = 0; i < kernelDef->input_groups().size(); ++i) {
        const auto &inputGroupDef = kernelDef->input_groups(i);
        auto group = new InputPortGroup(inputGroupDef.type(), inputGroupDef.group_type());
        _inputGroups.push_back(group);
        NAVI_LOG(SCHEDULE2, "add IO inputGroup[%s]", autil::StringUtil::toString(group).c_str());
    }
    for (auto i = 0; i < kernelDef->output_groups().size(); ++i) {
        auto group = new OutputPortGroup();
        _outputGroups.push_back(group);
    }
}

bool Node::initCreatorStats() {
    if (!isResourceCreateKernel()) {
        _creatorStats = &_kernelCreator->getCreatorStats();
    } else {
        const auto &binaryAttrMap = _def->binary_attrs();
        auto it = binaryAttrMap.find(RESOURCE_ATTR_NAME);
        if (binaryAttrMap.end() == it) {
            NAVI_LOG(ERROR, "can't find resource name in node binary attr");
            return false;
        }
        auto resourceCreator = _biz->getResourceCreator(it->second);
        if (!resourceCreator) {
            return true;
        }
        _creatorStats = &resourceCreator->getCreatorStats();
    }
    return true;
}

bool Node::getBinaryAttr(const std::string &attr, std::string &value) const {
    const auto &binaryAttrMap = _def->binary_attrs();
    auto it = binaryAttrMap.find(attr);
    if (binaryAttrMap.end() == it) {
        NAVI_LOG(SCHEDULE3, "can't find attr [%s] in node binary attr", attr.c_str());
        return false;
    }
    value = it->second;
    return true;
}

bool Node::postInit() {
    if (!initInputSnapshotVec()) {
        return false;
    }
    if (!initOutputSnapshotVec()) {
        return false;
    }
    initOutputDegree();
    if (!initResourceIndexMap()) {
        return false;
    }
    return true;
}

bool Node::initInputSnapshotVec() {
    IndexType start = _inputs.size();
    uint32_t count = _inputGroups.size();
    for (uint32_t index = 0; index < count; index++) {
        auto group = _inputGroups[index];
        if (!group->postInit()) {
            NAVI_LOG(ERROR, "invalid input group index");
            return false;
        }
        group->setInputIndexStart(start);
        start += group->getInputCount();
        _inputGroupReadyMap->setOptional(index, (group->getGroupType() == IT_OPTIONAL));
        _inputGroupReadyMap->setFinish(index, false);
    }
    _inputSnapshotVec.resize(start);
    return true;
}

bool Node::initOutputSnapshotVec() {
    IndexType start = _outputs.size();
    for (auto group : _outputGroups) {
        if (!group->postInit()) {
            NAVI_LOG(ERROR, "invalid output group index");
            return false;
        }
        group->setOutputIndexStart(start);
        start += group->getOutputCount();
    }
    _outputSnapshotVec = ReadyBitMap::createReadyBitMap(start);
    _outputSnapshotVec->setReady(false);
    _outputSnapshotVec->setFinish(false);
    return true;
}

void Node::initOutputDegree() {
    size_t outputDegree = 0;
    for (auto edge : _outputs) {
        if (edge) {
            outputDegree += edge->getOutputDegree();
        }
    }
    for (auto group : _outputGroups) {
        outputDegree += group->getOutputDegree();
    }
    _outputDegree = outputDegree;
}

bool Node::initResourceIndexMap() {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return true;
    }
    auto resourceGroup = _inputGroups[_resourceInputGroupIndex];
    const auto &inputVec = resourceGroup->getInputVec();
    auto inputCount = inputVec.size();
    for (IndexType index = 0; index < inputCount; index++) {
        const auto &edgeInfo = inputVec[index].second;
        auto inputNode = edgeInfo.getInputNode();
        if (!inputNode) {
            NAVI_LOG(ERROR, "unknown error, null resource input node");
            return false;
        }
        std::string resourceName;
        if (!inputNode->getBinaryAttr(RESOURCE_ATTR_NAME, resourceName)) {
            _flushIndexVec.push_back(index);
            continue;
        }
        _resourceIndexMap.emplace(resourceName, index);
    }
    return true;
}

void Node::startNormalResourceInput() const {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return;
    }
    const auto &replaceInfoMap = _graph->getReplaceInfoMap();
    for (const auto &depend : *_dependResourceMap) {
        const auto &resourceName = replaceInfoMap.getReplaceRTo(depend.first);
        if (_isResourceNode || (RS_KERNEL != _biz->getResourceStage(resourceName))) {
            startResourceInput(resourceName);
        }
    }
    auto resourceGroup = _inputGroups[_resourceInputGroupIndex];
    for (auto index : _flushIndexVec) {
        resourceGroup->startUpStreamSchedule(index, this);
    }
}

void Node::startKernelResourceInput() const {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return;
    }
    const auto &replaceInfoMap = _graph->getReplaceInfoMap();
    for (const auto &depend : *_dependResourceMap) {
        const auto &resourceName = replaceInfoMap.getReplaceRTo(depend.first);
        if (RS_KERNEL == _biz->getResourceStage(resourceName)) {
            startResourceInput(resourceName);
        }
    }
}

void Node::startResourceInput(const std::string &resourceName) const {
    auto resourceGroup = _inputGroups[_resourceInputGroupIndex];
    auto it = _resourceIndexMap.find(resourceName);
    if (_resourceIndexMap.end() != it) {
        NAVI_LOG(SCHEDULE3, "start input resource [%s] index [%u]", resourceName.c_str(), it->second);
        resourceGroup->startUpStreamSchedule(it->second, this);
    }
}

bool Node::createSelectResult() {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return true;
    }
    bool ret = true;
    const auto &selectBindVec = _kernelCreator->getBindInfos().selectBindVec;
    auto total = selectBindVec.size();
    for (size_t i = 0; i < total; i++) {
        const auto &selector = selectBindVec[i];
        auto tmpCtx = _ctx;
        int32_t index = -1;
        try {
            index = selector.selector(tmpCtx);
        } catch (const autil::legacy::ExceptionBase &e) {
            NAVI_LOG(
                ERROR, "compute select resource failed, selector [%lu], exception: [%s]", i, e.GetMessage().c_str());
            ret = false;
            continue;
        }
        int32_t candidateCount = selector.candidates.size();
        if (index >= candidateCount) {
            NAVI_LOG(ERROR,
                     "selector index [%d] overflow, total count [%d], selectResource [%lu]",
                     index,
                     candidateCount,
                     i);
            ret = false;
            continue;
        }
        NAVI_LOG(SCHEDULE3,
                 "selected resource [%d] [%s] ok, selector [%lu]",
                 index,
                 index >= 0 ? selector.candidates[index].c_str() : "",
                 i);
        _selectorResult.push_back(index);
    }
    return ret;
}

void Node::startSelectResourceInput() const {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return;
    }
    const auto &dependResourceMap = _kernelCreator->getDependResourcesWithoutSelect();
    auto resourceGroup = _inputGroups[_resourceInputGroupIndex];
    const auto &selectBindVec = _kernelCreator->getBindInfos().selectBindVec;
    auto total = selectBindVec.size();
    for (size_t i = 0; i < total; i++) {
        const auto &selector = selectBindVec[i];
        auto index = _selectorResult[i];
        auto candidateCount = selector.candidates.size();
        for (int32_t j = 0; j < candidateCount; j++) {
            bool selected = (j == index);
            const auto &candidate = selector.candidates[j];
            auto it = _resourceIndexMap.find(candidate);
            if (_resourceIndexMap.end() == it) {
                continue;
            }
            auto offset = it->second;
            if (selected) {
                resourceGroup->startUpStreamSchedule(offset, this);
            } else {
                if (dependResourceMap.end() == dependResourceMap.find(candidate)) {
                    resourceGroup->setEof(offset, this);
                }
            }
        }
    }
}

ResourcePtr Node::buildR(const std::string &name, KernelConfigContext *ctx, ResourceMap &inputResourceMap) {
    const auto &enableResourceSet = _kernelCreator->getEnableBuildResourceSet();
    if (enableResourceSet.end() == enableResourceSet.find(name)) {
        NAVI_LOG(ERROR, "can't build resource [%s], not enabled", name.c_str());
        return nullptr;
    }
    return _graph->buildKernelResourceRecur(name, ctx, _resourceMap, this, inputResourceMap);
}

bool Node::initDependResourceMap() {
    if (isResourceCreateKernel()) {
        std::string resourceName;
        if (!getBinaryAttr(RESOURCE_ATTR_NAME, resourceName)) {
            return false;
        }
        _dependResourceMap = _biz->getResourceDependResourceMap(resourceName);
    } else {
        _dependResourceMap = &_kernelCreator->getDependResourcesWithoutSelect();
    }
    return true;
}

ReadyBitMap *Node::getInputBitMap(PortIndex inputIndex) {
    if (inputIndex.isGroup()) {
        assert(inputIndex.group < _inputGroups.size());
        return _inputGroups[inputIndex.group]->getReadyMap();
    } else {
        return _inputReadyMap;
    }
}

NodeAsyncInfoPtr Node::getAsyncInfo() const {
    autil::ScopedLock lock(_asyncMutex);
    return _asyncInfo;
}

void Node::setAsyncInfo(const NodeAsyncInfoPtr &asyncInfo) {
    autil::ScopedLock lock(_asyncMutex);
    _asyncInfo = asyncInfo;
}

AsyncPipePtr __attribute__((weak)) Node::createAsyncPipe(ActivateStrategy activateStrategy) {
    return doCreateAsyncPipe(activateStrategy);
}


AsyncPipePtr Node::doCreateAsyncPipe(ActivateStrategy activateStrategy) {
    autil::ScopedLock lock(_forkLock);
    if (_graph->terminated()) {
        NAVI_LOG(ERROR, "create async pipe failed, graph terminated");
        return nullptr;
    }
    auto oldInfo = getAsyncInfo();
    auto asyncInfo = std::make_shared<NodeAsyncInfo>(this, oldInfo);
    auto pipe = asyncInfo->addOne(activateStrategy);
    setAsyncInfo(asyncInfo);
    return pipe;
}

void Node::notifyPipeTerminate() {
    autil::ScopedLock lock(_forkLock);
    if (_graph->terminated()) {
        return;
    }
    auto oldInfo = getAsyncInfo();
    if (!oldInfo) {
        return;
    }
    auto asyncInfo = std::make_shared<NodeAsyncInfo>(this, oldInfo);
    if (asyncInfo->consolidate()) {
        setAsyncInfo(asyncInfo);
    } else {
        setAsyncInfo(nullptr);
    }
}

ReadyBitMap *Node::getOutputBitMap(PortIndex outputIndex) {
    if (outputIndex.isGroup()) {
        assert(outputIndex.group < _outputGroups.size());
        return _outputGroups[outputIndex.group]->getReadyMap();
    } else if (outputIndex.isControl()) {
        return _controlOutputReadyMap;
    } else {
        return _outputReadyMap;
    }
}

bool Node::bindPort(Port *port) {
    auto localPort = static_cast<LocalPortBase *>(port);
    auto &partInfo = localPort->getPartInfo();
    if (IOT_INPUT == localPort->getIoType()) {
        assert(0 < _inputGroups.size());
        auto group = _inputGroups[0];
        if (!group->postInit(partInfo)) {
            return false;
        }
        localPort->setNode(this, group->getReadyMap());
        setHasInput();
        _isInputBorder = true;
    } else {
        assert(0 < _outputGroups.size());
        auto group = _outputGroups[0];
        if (!group->postInit(partInfo)) {
            return false;
        }
        localPort->setNode(this, group->getReadyMap());
        setHasOutput();
        _isOutputBorder = true;
    }
    _port = port;
    return true;
}

bool Node::addInput(const std::string &portName,
                    bool require,
                    const EdgeOutputInfo &edgeOutputInfo,
                    PortIndex &inputIndex,
                    std::string &dataType)
{
    std::string port;
    IndexType index = 0;
    if (!CommonUtil::parseGroupPort(portName, port, index)) {
        NAVI_LOG(ERROR,
                 "parse portName [%s] failed, kernel def[%s]",
                 portName.c_str(),
                 _kernelCreator->def()->DebugString().c_str());
        return false;
    }
    if (NODE_CONTROL_INPUT_PORT == port) {
        auto controlGroup = addControlInputGroup();
        auto offset = controlGroup->getInputCount();
        controlGroup->addInput(offset, edgeOutputInfo);
        setHasInput();
        inputIndex.group = _controlInputGroupIndex;
        inputIndex.index = offset;
        return true;
    }
    if (NODE_RESOURCE_INPUT_PORT == port) {
        auto resourceGroup = addResourceInputGroup();
        auto offset = resourceGroup->getInputCount();
        resourceGroup->addInput(offset, edgeOutputInfo);
        setHasInput();
        inputIndex.group = _resourceInputGroupIndex;
        inputIndex.index = offset;
        dataType = "";
        return true;
    }
    assert(edgeOutputInfo.isValid());
    auto portInfo = _kernelCreator->getInputPortInfo(port, index);
    if (!portInfo.isValid()) {
        NAVI_LOG(ERROR,
                 "no input port named [%s], check port name or group index, "
                 "kernel def[%s]",
                 portName.c_str(),
                 _kernelCreator->def()->DebugString().c_str());
        return false;
    }
    if (portInfo.isGroup()) {
        inputIndex = doAddInputGroup(portInfo, edgeOutputInfo);
    } else {
        inputIndex = doAddInput(portInfo, edgeOutputInfo, require);
    }
    auto portDef = static_cast<const InputDef *>(portInfo.def);
    dataType = portDef->data_type().name();
    if (inputIndex.isValid()) {
        setHasInput();
        return true;
    } else {
        return false;
    }
}

PortIndex Node::doAddInput(const PortInfo &portInfo,
                           const EdgeOutputInfo &edgeOutputInfo,
                           bool edgeRequire)
{
    auto portIndex = portInfo.index;
    auto inputDef = (InputDef *)portInfo.def;
    assert(inputDef);
    assert(portIndex.index < _inputs.size());

    auto &outputInfo = _inputs[portIndex.index];
    if (outputInfo.isValid()) {
        NAVI_LOG(ERROR, "multiple input, edge [%s] and edge [%s]",
                 outputInfo.getDebugName().c_str(),
                 edgeOutputInfo.getDebugName().c_str());
        return PortIndex();
    }
    outputInfo = edgeOutputInfo;
    auto activeStrategy = getInputActivateStrategy(inputDef, edgeRequire);
    _inputReadyMap->setOptional(portIndex.index, (activeStrategy == AS_OPTIONAL));
    _inputReadyMap->setFinish(portIndex.index, false);
    return portIndex;
}

PortIndex Node::doAddInputGroup(const PortInfo &portInfo,
                                const EdgeOutputInfo &edgeOutputInfo)
{
    auto groupIndex = portInfo.index;
    assert(groupIndex.group < _inputGroups.size());
    auto retIndex = _inputGroups[groupIndex.group]->addInput(groupIndex.index,
                                                             edgeOutputInfo);
    NAVI_LOG(SCHEDULE2, "add input group[%d,%d], retIndex[%d]", groupIndex.group, groupIndex.index, retIndex);
    groupIndex.index = retIndex;
    return groupIndex;
}

InputPortGroup *Node::addControlInputGroup() {
    if (INVALID_INDEX == _controlInputGroupIndex) {
        auto group = new InputPortGroup(IT_REQUIRE, IT_REQUIRE);
        _controlInputGroupIndex = _inputGroups.size();
        _inputGroups.push_back(group);
        NAVI_LOG(SCHEDULE2, "add control inputGroup[%s]", autil::StringUtil::toString(group).c_str());
    }
    return _inputGroups[_controlInputGroupIndex];
}

InputPortGroup *Node::addResourceInputGroup() {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        auto group = new InputPortGroup(IT_REQUIRE, IT_REQUIRE);
        _resourceInputGroupIndex = _inputGroups.size();
        _inputGroups.push_back(group);
        NAVI_LOG(SCHEDULE2, "add resource inputGroup[%s]", autil::StringUtil::toString(group).c_str());
    }
    return _inputGroups[_resourceInputGroupIndex];
}

void Node::setHasInput() {
    _hasInput = true;
}

bool Node::hasInput() const {
    return _hasInput || (getAsyncInfo() != nullptr);
}

void Node::setHasOutput() {
    _hasOutput = true;
}

bool Node::hasOutput() const {
    return _hasOutput;
}

bool Node::addOutput(Edge *output, const std::string &portName,
                     PortIndex &outputIndex, std::string &dataType)
{
    if (!output) {
        NAVI_LOG(ERROR, "input is null");
        return false;
    }
    auto portInfo = _kernelCreator->getOutputPortInfo(portName);
    if (!portInfo.isValid()) {
        NAVI_LOG(ERROR, "no output port named [%s], kernel def[%s]",
                 portName.c_str(),
                 _kernelCreator->def()->DebugString().c_str());
        return false;
    }
    if (portInfo.isGroup()) {
        outputIndex = doAddOutputGroup(portInfo, output);
    } else {
        outputIndex = doAddOutput(portInfo, output);
    }
    auto portDef = static_cast<const OutputDef *>(portInfo.def);
    dataType = portDef->data_type().name();
    if (outputIndex.isValid()) {
        return true;
    } else {
        return false;
    }
}

PortIndex Node::doAddOutput(const PortInfo &portInfo, Edge *output) {
    auto portIndex = portInfo.index;
    if (portIndex.control) {
        assert(portIndex.index < _controlOutputs.size());
        if (_controlOutputs[portIndex.index]) {
            NAVI_LOG(
                ERROR,
                "got multiple control link, control output port index [%u]",
                portIndex.index);
            return PortIndex();
        }
        _controlOutputs[portIndex.index] = output;
        return portIndex;
    } else {
        assert(portIndex.index < _outputs.size());
        assert(!_outputs[portIndex.index]);
        _outputs[portIndex.index] = output;
        _outputReadyMap->setReady(portIndex.index, true);
        _outputReadyMap->setOptional(portIndex.index, false);
        _outputReadyMap->setFinish(portIndex.index, false);
        return portIndex;
    }
}

PortIndex Node::doAddOutputGroup(const PortInfo &portInfo, Edge *output) {
    auto groupIndex = portInfo.index;
    assert(groupIndex.group < _outputGroups.size());
    _outputGroups[groupIndex.group]->addOutput(groupIndex.index, output);
    return groupIndex;
}

ActivateStrategy Node::getInputActivateStrategy(
        const InputDef *inputDef, bool edgeRequire)
{
    switch (inputDef->type()) {
    case IT_REQUIRE:
    case IT_REQUIRE_WHEN_CONNECTED:
        return AS_REQUIRED;
    case IT_OPTIONAL:
        if (edgeRequire) {
            return AS_REQUIRED;
        } else {
            return AS_OPTIONAL;
        }
    default:
        assert(false);
        return AS_REQUIRED;
    }
}

bool Node::getPortTypeStr(const std::string &portName,
                          IoType ioType,
                          std::string &typeStr) const
{
    if (!_kernelCreator) {
        return false;
    }
    return _kernelCreator->getPortTypeStr(portName, ioType, typeStr);
}

bool Node::checkConnection() const {
    if (!_kernelCreator) {
        return false;
    }
    auto ret = true;
    ret = checkInput() && ret;
    ret = checkInputGroup() && ret;
    return ret;
}

bool Node::checkInput() const {
    for (IndexType i = 0; i < _inputs.size(); i++) {
        if (!_inputs[i].isValid()) {
            const auto &portName =
                _kernelCreator->getInputPortName(PortIndex(i, INVALID_INDEX));
            if (portName.empty()) {
                NAVI_LOG(ERROR, "unknown error, input index [%d] overflow", i);
                return false;
            }
            NAVI_LOG(ERROR, "input port[%s] not connected", portName.c_str());
            return false;
        }
    }
    return true;
}

bool Node::checkInputGroup() const {
    for (IndexType i = 0; i < _inputGroups.size(); i++) {
        auto bitMap = _inputGroups[i]->getReadyMap();
        if (0 == bitMap->size()) {
            const auto &portName =
                _kernelCreator->getInputPortName(PortIndex(INVALID_INDEX, i));
            if (portName.empty()) {
                NAVI_LOG(ERROR,
                         "unknown error, input group index [%d] overflow", i);
                return false;
            }
            NAVI_LOG(ERROR, "input group[%s] not connected", portName.c_str());
            return false;
        }
    }
    return true;
}

void Node::finishPendingOutput() {
    for (IndexType i = 0; i < _outputs.size(); i++) {
        if (!_outputs[i]) {
            const auto &portName = _kernelCreator->getOutputPortName(i);
            NAVI_LOG(SCHEDULE1, "finish pending output, port [%s]",
                     portName.c_str());
            _outputReadyMap->setFinish(i, true);
        }
    }
}

bool Node::initResourceMap() {
    const auto &dependResourceMap = _kernelCreator->getDependResourcesWithoutSelect();
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        if (!dependResourceMap.empty()) {
            NAVI_LOG(ERROR, "lack resource input");
            return false;
        } else {
            return true;
        }
    }
    InputSnapshot *begin = nullptr;
    InputSnapshot *end = nullptr;
    if (!snapshotGroupInput(_resourceInputGroupIndex, begin, end)) {
        NAVI_LOG(ERROR, "read resource input failed index [%u] size [%lu]",
                 _resourceInputGroupIndex, _inputGroups.size());
        return false;
    }
    for (; begin != end; begin++) {
        DataPtr data(std::move(begin->data.data));
        auto resourceData = std::dynamic_pointer_cast<ResourceData>(data);
        if (!resourceData) {
            continue;
        }
        const auto &resource = resourceData->_resource;
        if (!resource) {
            continue;
        }
        _resourceMap.addResource(resource);
    }
    _graph->getReplaceInfoMap().replace(_dependResourceMap, _resourceMap);
    for (const auto &info : dependResourceMap) {
        if (!checkRequireResource(info.first, info.second)) {
            return false;
        }
    }
    const auto &selectBindVec = _kernelCreator->getBindInfos().selectBindVec;
    for (int i = 0; i < selectBindVec.size(); i++) {
        const auto &selectInfo = selectBindVec[i];
        auto selectIndex = _selectorResult[i];
        if (selectIndex < 0) {
            continue;
        }
        const auto &selected = selectInfo.candidates[selectIndex];
        if (!checkRequireResource(selected, selectInfo.require)) {
            NAVI_LOG(ERROR, "lack selected resource, select index [%d], selector [%d]", selectIndex, i);
            return false;
        }
    }
    return true;
}

bool Node::checkRequireResource(const std::string &resourceName, bool require) const {
    bool hasResource = _resourceMap.hasResource(resourceName);
    if (require && !hasResource) {
        NAVI_LOG(ERROR, "required resource not exist [%s]", resourceName.c_str());
        return false;
    }
    return true;
}

const std::string &Node::getConfigPath() const {
    return _biz->getConfigPath();
}

Port *Node::getPort() const {
    return _port;
}

const ResourceMap &Node::getResourceMap() const {
    return _resourceMap;
}

bool Node::createKernel(const ScheduleInfo &schedInfo) {
    ComputeScope computeScope(_metric, GET_KERNEL_CREATE, _graph->getPartId(),
                              _graph->getWorker(), schedInfo);
    NaviLoggerScope scope(getLogger());
    NAVI_LOG(SCHEDULE1, "create kernel begin");
    void *baseAddr = nullptr;
    auto kernel = _kernelCreator->create(baseAddr);
    if (!kernel) {
        NAVI_LOG(ERROR, "create kernel failed, ec[%s]",
                 CommonUtil::getErrorString(EC_CREATE_KERNEL));
        _graph->setErrorCode(EC_CREATE_KERNEL);
        return false;
    }
    _kernel = kernel;
    _kernelBaseAddr = baseAddr;
    kernel->setNodeDef(_def);
    auto nodeType = _def->type();
    if (NT_SCOPE_TERMINATOR == nodeType) {
        if (!dynamic_cast<ScopeTerminatorKernel *>(kernel)) {
            NAVI_LOG(ERROR, "scope kernel must inherit ScopeTerminatorKernel");
            _graph->setErrorCode(EC_CREATE_KERNEL);
            return false;
        }
    }
    if (!initAttribute()) {
        _graph->setErrorCode(EC_INVALID_ATTRIBUTE);
        return false;
    }
    if (_creatorStats) {
        _creatorStats->updateCreateLatency(computeScope.finish());
    }
    NAVI_LOG(SCHEDULE1, "create kernel success");
    return true;
}

bool Node::initKernel(const ScheduleInfo &schedInfo) {
    _kernelInited = true;
    if (unlikely(_skipInit)) {
        NAVI_LOG(DEBUG, "kernel init skipped");
        return true;
    }
    NAVI_LOG(SCHEDULE1, "init kernel begin");
    if (!isResourceCreateKernel() && !initResourceMap()) {
        _graph->setErrorCode(EC_LACK_RESOURCE);
        return false;
    }
    if (!bindResource()) {
        _graph->setErrorCode(EC_LACK_RESOURCE);
        return false;
    }
    if (!bindNamedData()) {
        _graph->setErrorCode(EC_BIND_NAMED_DATA);
        return false;
    }
    ErrorCode ec = EC_NONE;
    {
        ComputeScope scope(_metric, GET_KERNEL_INIT, _graph->getPartId(),
                           _graph->getWorker(), schedInfo);
        KernelInitContext ctx(this);
        try {
            ec = _kernel->init(ctx);
        } catch (const autil::legacy::ExceptionBase &e) {
            NAVI_KERNEL_LOG(ERROR, "kernel init failed, exception: [%s]", e.GetMessage().c_str());
            ec = EC_ABORT;
        }
        if (EC_NONE == ec) {
            if (_creatorStats) {
                _creatorStats->updateInitLatency(scope.finish());
            }
        }
    }
    if (EC_NONE != ec) {
        NAVI_LOG(ERROR, "kernel init failed");
        _graph->setErrorCode(ec);
        return false;
    }
    NAVI_LOG(SCHEDULE1, "init kernel success");
    return true;
}

bool Node::initAttribute() {
    if (unlikely(_skipConfig)) {
        NAVI_LOG(DEBUG, "kernel config skipped");
        return true;
    }
    if (!initConfigContext()) {
        return false;
    }
    if (!createSelectResult()) {
        return false;
    }
    NAVI_MEMORY_BARRIER();
    startKernelResourceInput();
    startSelectResourceInput();
    auto ctx = _ctx;
    try {
        return _kernel->config(ctx);
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_LOG(
            ERROR,
            "biz[%s], init kernel attribute failed, json attr [%s], msg [%s]",
            _biz->getName().c_str(), autil::StringUtil::toString(ctx).c_str(),
            e.GetMessage().c_str());
        return false;
    }
}

bool Node::initConfigContext() {
    auto *jsonConfig = _biz->getKernelConfig(getKernelName());
    if (!NaviConfig::parseToDocument(_def->json_attrs(), _jsonAttrsDocument)) {
        NAVI_LOG(ERROR, "invalid kernel attr, biz [%s]",
                 _biz->getName().c_str());
        return false;
    }
    _ctx = KernelConfigContext(getConfigPath(), &_jsonAttrsDocument, jsonConfig, _def, _kernelCreator->getJsonAttrs());
    return true;
}

const std::vector<int32_t> &Node::getSelectorResult() const {
    return _selectorResult;
}

KernelConfigContext Node::getConfigContext() const {
    return _ctx;
}

bool Node::getConfigContext(const std::string &dependName, KernelConfigContext &ctx) {
    const auto &configKeyMap = _kernelCreator->getConfigKeyMap();
    auto it = configKeyMap.find(dependName);
    if (configKeyMap.end() == it) {
        ctx = _ctx;
        return true;
    }
    const auto &configKey = it->second;
    if (!_ctx.hasKey(configKey)) {
        NAVI_LOG(ERROR, "lack config key [%s], depend resource [%s]", configKey.c_str(), dependName.c_str());
        return false;
    }
    ctx = _ctx.enter(configKey);
    return true;
}

bool Node::getBizPartInfo(const std::string &bizName, NaviPartId &partCount, std::vector<NaviPartId> &partIds) const {
    return _graph->getParam()->bizManager->getBizPartInfo(bizName, partCount, partIds);
}

bool Node::bindResource() {
    const auto &bindInfos = _kernelCreator->getBindInfos();
    const auto &dynamicInfoMap = _kernelCreator->getDynamicGroupInfoMap();
    if (!ResourceManager::bindResource(
            getKernelName(), _kernelBaseAddr, getResourceMap(), dynamicInfoMap, bindInfos, _selectorResult, "kernel")) {
        NAVI_LOG(ERROR, "bind kernel resource failed");
        return false;
    }
    return true;
}

bool Node::bindNamedData() {
    const auto &bindInfos = _kernelCreator->getBindInfos();
    const auto &runGraphParams = _graph->getParam()->runParams;
    auto graphId = _graph->getGraphId();
    auto namedDataMap = runGraphParams.getSubNamedDataMap(graphId);
    if (!ResourceManager::bindNamedData(getKernelName(), *_kernel, namedDataMap,
                                        bindInfos, "kernel"))
    {
        NAVI_LOG(ERROR, "bind kernel named data failed");
        return false;
    }
    return true;
}

void Node::deleteKernel(bool inDestruct) {
    if (inDestruct) {
        delete _kernel;
    } else {
        if (_kernel) {
            auto worker = _graph->getWorker();
            auto item =
                new KernelDestructItem(worker, _graph, _metric, _kernel);
            _kernel = nullptr;
            worker->schedule(item);
        }
    }
}

bool Node::inputOk() const {
    auto asyncInfo = getAsyncInfo();
    if (asyncInfo) {
        if (asyncInfo->independentReady()) {
            return true;
        }
        if (!asyncInfo->isOk()) {
            return false;
        }
    }
    if (!_inputReadyMap->isOk()) {
        return false;
    }
    uint32_t inputGroupCount = _inputGroups.size();
    for (uint32_t index = 0; index < inputGroupCount; index++) {
        auto group = _inputGroups[index];
        _inputGroupReadyMap->setReady(index, group->isOk());
    }
    if (!_inputGroupReadyMap->isOk()) {
        return false;
    }
    if (hasInput()) {
        return inputFinish() || inputHasReady();
    } else {
        return true;
    }
}

bool Node::resourceInputOk() const {
    if (INVALID_INDEX == _resourceInputGroupIndex) {
        return true;
    }
    auto readyMap = _inputGroups[_resourceInputGroupIndex]->getReadyMap();
    return readyMap->isOk();
}

bool Node::inputFinish() const {
    if (!_inputReadyMap->isFinish()) {
        return false;
    }
    for (const auto &inputGroup : _inputGroups) {
        const auto &readyMap = inputGroup->getReadyMap();
        if (!readyMap->isFinish()) {
            return false;
        }
    }
    auto asyncInfo = getAsyncInfo();
    if (asyncInfo && !asyncInfo->isFinish()) {
        return false;
    }
    return true;
}

bool Node::inputHasReady() const {
    if (_inputReadyMap->hasReady()) {
        return true;
    }
    for (const auto &inputGroup : _inputGroups) {
        if (inputGroup->hasReady()) {
            return true;
        }
    }
    auto asyncInfo = getAsyncInfo();
    if (asyncInfo && asyncInfo->hasReady()) {
        return true;
    }
    return false;
}

bool Node::inputFinishConsumed() const {
    for (const auto &snapshot : _inputSnapshotVec) {
        if (!snapshot.data.eof) {
            return false;
        }
    }
    auto asyncInfo = getAsyncInfo();
    if (asyncInfo && !asyncInfo->isFinishConsumed()) {
        return false;
    }
    return true;
}

bool Node::outputOk() const {
    if (!_outputReadyMap->isOk()) {
        return false;
    }
    for (const auto &outputGroup : _outputGroups) {
        if (!outputGroup->isOk()) {
            return false;
        }
    }
    return true;
}

bool Node::outputFinish() const {
    if (!_outputReadyMap->isFinish()) {
        return false;
    }
    for (const auto &outputGroup : _outputGroups) {
        const auto &readyMap = outputGroup->getReadyMap();
        if (!readyMap->isFinish()) {
            return false;
        }
    }
    return true;
}

bool Node::scheduleLock(const Node *callNode) {
    incNodeSnapshot();
    NAVI_MEMORY_BARRIER();
    while (1) {
        if (_scheduleStat == SS_INIT) {
            NAVI_LOG(SCHEDULE2,
                     "node not started, schedule by [%p] [%s]",
                     callNode,
                     callNode ? callNode->getName().c_str() : "");
            return false;
        }
        if (_scheduleStat == SS_FINISH) { // schedule is checked after compute
            NAVI_LOG(SCHEDULE2, "schedule by [%p] [%s], failed for finish",
                     callNode, callNode ? callNode->getName().c_str() : "");
            return false;
        }
        if (_scheduleStat == SS_RUNNING) { // schedule is checked after compute
            NAVI_LOG(SCHEDULE2, "schedule by [%p] [%s], failed for running",
                     callNode, callNode ? callNode->getName().c_str() : "");
            return false;
        }
        if (_scheduleStat == SS_SCHEDULING) {
            NAVI_LOG(SCHEDULE2, "failed for scheduling, snapshot [%ld]", readNodeSnapshot());
            return false;
        }
        if (cmpxchg(&_scheduleStat, SS_IDLE, SS_SCHEDULING)) {
            return true;
        }
    }
}

bool Node::schedule(const Node *callNode, bool ignoreFrozen) {
    while (true) {
        bool resched = true;
        auto ret = doSchedule(callNode, ignoreFrozen, resched);
        if (ret) {
            return ret;
        }
        if (resched) {
            continue;
        }
        return ret;
    }
}

bool Node::startSchedule(const Node *callNode, bool ignoreFrozen) {
    if (cmpxchg(&_scheduleStat, SS_INIT, SS_IDLE)) {
        NAVI_LOG(SCHEDULE3, "start schedule by [%s]", callNode ? callNode->getName().c_str() : "");
        for (const auto &edgeOutputInfo : _inputs) {
            edgeOutputInfo.startUpStreamSchedule(this);
        }
        IndexType groupCount = _inputGroups.size();
        for (IndexType index = 0; index < groupCount; index++) {
            if (_resourceInputGroupIndex == index) {
                continue;
            }
            _inputGroups[index]->startUpStreamSchedule(this);
        }
        startNormalResourceInput();
        return schedule(callNode, ignoreFrozen);
    }
    return true;
}

/*
lock  -> checkReady  -> running -> checkFinish ->reschedule
finish must be checked after compute
 */
bool Node::doSchedule(const Node *callNode, bool ignoreFrozen, bool &resched) {
    if (!ignoreFrozen && _frozen) {
        resched = false;
        return false;
    }
    if (!scheduleLock(callNode)) {
        resched = false;
        return false;
    }
    auto snapshot = readNodeSnapshot();
    _metric->incTryScheduleCount();
    NAVI_MEMORY_BARRIER();
    // critical region under SS_SCHEDULING stat;
    assert(SS_SCHEDULING == _scheduleStat);
    if (checkFinish(EC_NONE)) {
        resched = false;
        return false;
    }
    bool needCreateKernel = !_kernel;
    bool needInitKernel = _kernel && !_kernelInited;
    bool ret = false;
    if (_forkGraph) {
        ret = false;
    } else if (_inlineConfigAndInit) {
        ret = resourceInputOk() && inputOk() && outputOk();
    } else if (needCreateKernel) {
        ret = true;
    } else if (needInitKernel) {
        ret = resourceInputOk();
    } else {
        ret = (inputOk() && outputOk());
    }
    NAVI_LOG(SCHEDULE2,
             "schedule by node [%p] [%s], ret [%d], forGraph [%p], needCreate [%d], needInit "
             "[%d], resourceInputOk [%d], inputOk [%d], outputOk [%d], "
             "inputFinish [%d], outputFinish [%d], inputHasReady [%d], hasInput[%d], "
             "hasOutput[%d], stat [%ld], snapshot: %ld, computeId: %ld",
             callNode, callNode ? callNode->getName().c_str() : "", ret,
             _forkGraph, needCreateKernel, needInitKernel, resourceInputOk(), inputOk(),
             outputOk(), inputFinish(), outputFinish(), inputHasReady(), hasInput(),
             hasOutput(), _scheduleStat, snapshot, getComputeId());
    NAVI_LOG(SCHEDULE2,
             "inputReadyMap[%s] inputGroup[%s]",
             autil::StringUtil::toString(*_inputReadyMap).c_str(),
             autil::StringUtil::toString(_inputGroups).c_str());
    if (ret) {
        _scheduleStat = SS_RUNNING;
        if (!_graph->schedule(this)) {
            _scheduleStat = SS_IDLE;
        }
        resched = false;
    } else {
        cmpxchg(&_scheduleStat, SS_SCHEDULING, SS_IDLE);
        auto newSnapshot = readNodeSnapshot();
        resched = (newSnapshot != snapshot);
        NAVI_LOG(SCHEDULE2,
                 "schedule by [%p] [%s], failed, old snapshot [%ld], new snapshot [%ld], resched [%d]",
                 callNode,
                 callNode ? callNode->getName().c_str() : "",
                 snapshot,
                 newSnapshot,
                 resched);
    }
    return ret;
}

void Node::stopSchedule() {
    _stopSchedule = true;
}

void Node::forceStop(Node *node) {
    _forceStopNode = node;
    _forceStop = true;
    schedule(this);
}

bool Node::checkFinish(ErrorCode ec) {
    bool finish = false;
    if (_stopSchedule) {
        finish = true;
    } else if (EC_NONE != ec) {
        finish = true;
    } else if (hasOutput()) {
        finish = outputFinish();
    } else {
        finish = inputFinish() && inputFinishConsumed();
    }
    if (!finish && _forceStop) {
        NAVI_LOG(SCHEDULE2,
                 "node [%s] force stopped by node [%s]",
                 _name.c_str(),
                 _forceStopNode ? _forceStopNode->_name.c_str() : "");
        finish = true;
    }
    NAVI_LOG(SCHEDULE2,
             "check finish [%d], forceStop [%d], stopSchedule [%d], computeId: %ld, hasOutput [%d], outputFinish "
             "[%d], inputFinish [%d], inputFinishConsumed [%d]",
             finish,
             _forceStop,
             _stopSchedule,
             getComputeId(),
             hasOutput(),
             outputFinish(),
             inputFinish(),
             inputFinishConsumed());
    if (finish) {
        _scheduleStat = SS_FINISH;
        // the last edge is finish edge, see creator
        auto finishEdge = _controlOutputs[CONTROL_PORT_FINISH_INDEX];
        if (finishEdge) {
            finishEdge->setEof(this);
        }
        propagateEof();
        if (likely(!_skipDeleteKernel)) {
            deleteKernel();
        }
    }
    return finish;
}

void Node::propagateEof() const {
    for (const auto &edgeOutputInfo : _inputs) {
        edgeOutputInfo.setEof(this);
    }
    for (auto inputGroup : _inputGroups) {
        inputGroup->setEof(this);
    }
    if (_forceStop) {
        for (auto edge : _outputs) {
            if (edge) {
                edge->forceEof();
            }
        }
        for (auto group : _outputGroups) {
            group->forceEof();
        }
        if (_port && _port->getIoType() == IOT_OUTPUT) {
            _port->setEofAll();
        }
    }
}

bool Node::isInline() const {
    if (unlikely(!_creatorStats)) {
        return true;
    }
    if (!_kernel) {
        auto createLatency = _creatorStats->getCreateAvgLatency();
        return createLatency < INLINE_LATENCY_THRESHOLD_US;
    } else if (!_kernelInited) {
        auto initLatency = _creatorStats->getInitAvgLatency();
        return initLatency < INLINE_LATENCY_THRESHOLD_US;
    } else {
        return !_kernel->isExpensive();
    }
}

size_t Node::outputDegree() const {
    return _outputDegree;
}

void Node::compute(const ScheduleInfo &schedInfo)
{
    NaviLoggerScope scope(getLogger());
    // critical region under SS_RUNNING stat;
    assert(SS_RUNNING == _scheduleStat);
    auto *graph = _graph->getGraph();
    if (graph->isTimeout()) {
        NAVI_LOG(ERROR, "compute failed, graph[%p] timeout", graph);
        graph->notifyTimeout();
        return;
    }
    auto snapshot = readNodeSnapshot();
    auto computeId = getComputeId() + 1;
    ErrorCode ec = EC_NONE;
    bool needCreateKernel = !_kernel;
    bool needInitKernel = _kernel && !_kernelInited;
    if (_inlineConfigAndInit) {
        needInitKernel = !_kernelInited;
        if (needCreateKernel && !createKernel(schedInfo)) {
            ec = EC_CREATE_KERNEL;
            NAVI_LOG(ERROR, "create kernel failed, error [%d], abort kernel", ec);
        } else if (needInitKernel && !initKernel(schedInfo)) {
            ec = EC_INIT_GRAPH;
            NAVI_LOG(ERROR, "init kernel failed, error [%d], abort kernel", ec);
        } else {
            NAVI_LOG(SCHEDULE1, "begin compute, snapshot: %ld, compute id: %ld",
                     snapshot, computeId);
            ec = doCompute(schedInfo);
            NAVI_LOG(SCHEDULE1, "end compute, compute id: %ld", computeId);
        }
    } else {
        if (needCreateKernel) {
            if (!createKernel(schedInfo)) {
                ec = EC_CREATE_KERNEL;
                NAVI_LOG(ERROR, "create kernel failed, error [%d], abort kernel", ec);
            }
        } else if (needInitKernel) {
            if (!initKernel(schedInfo)) {
                ec = EC_INIT_GRAPH;
                NAVI_LOG(ERROR, "init kernel failed, error [%d], abort kernel", ec);
            }
        } else {
            NAVI_LOG(SCHEDULE1, "begin compute, snapshot: %ld, compute id: %ld", snapshot, computeId);
            ec = doCompute(schedInfo);
            NAVI_LOG(SCHEDULE1, "end compute, compute id: %ld", computeId);
        }
    }
    if (EC_NONE != ec) {
        NAVI_LOG(ERROR,
                 "compute error, error code [%s], compute id: %ld, abort kernel",
                 CommonUtil::getErrorString(ec), computeId);
        _graph->notifyFinish(this, ec);
        checkFinish(ec);
        return;
    }
    if (_stopAfterInit && needInitKernel) {
        setFrozen(true);
    }
    // critical region
    if (!checkFinish(ec)) {
        // block other thread and check snapshot id
        cmpxchg(&_scheduleStat, SS_RUNNING, SS_IDLE);
        auto newSnapshot = readNodeSnapshot();
        auto needSchedule =
            (newSnapshot != snapshot) || needCreateKernel || needInitKernel;
        if (needSchedule) {
            NAVI_LOG(
                SCHEDULE2,
                "reschedule, prev snapshot [%ld], current snapshot [%ld]",
                snapshot, newSnapshot);
            schedule(this);
        } else {
            NAVI_LOG(SCHEDULE2, "no need reschedule");
        }
    } else {
        auto nodeType = _def->type();
        if (NT_SCOPE_TERMINATOR == nodeType) {
            _graph->terminateScope(this, getScope(), ec);
        }
    }
}

ErrorCode Node::doCompute(const ScheduleInfo &schedInfo) {
    clearControlInput();
    KernelComputeContext ctx(this);
    ErrorCode ec = EC_NONE;
    {
        ComputeScope scope(_metric, GET_KERNEL_COMPUTE, _graph->getPartId(),
                           _graph->getWorker(), schedInfo);
        try {
            ec = _kernel->compute(ctx);
        } catch (const autil::legacy::ExceptionBase &e) {
            NAVI_KERNEL_LOG(ERROR, "kernel compute failed, exception: [%s]", e.GetMessage().c_str());
            ec = EC_ABORT;
        }
    }
    if (ec == EC_NONE && !getAsyncInfo() && ctx.deadLock()) {
        ec = EC_DEADLOCK;
    }
    resetIOSnapshot();
    return ec;
}

void Node::clearControlInput() {
    if (INVALID_INDEX == _controlInputGroupIndex) {
        return;
    }
    InputSnapshot *begin = nullptr;
    InputSnapshot *end = nullptr;
    snapshotGroupInput(_controlInputGroupIndex, begin, end);
    for (; begin != end; begin++) {
        if (begin->data.data.use_count() == 1) {
            destructData(std::move(begin->data.data));
        }
        begin->data.reset();
    }
}

void Node::resetIOSnapshot() {
    for (auto &input: _inputSnapshotVec) {
        if (input.data.data.use_count() == 1) {
            destructData(std::move(input.data.data));
        }
        input.reset();
    }
    _outputSnapshotVec->setReady(false);
}

LocalSubGraph *Node::getGraph() const {
    return _graph;
}

void Node::destructData(DataPtr &&data) const {
    auto worker = _graph->getWorker();
    auto item = new DataDestructItem(worker, std::move(data));
    worker->schedule(item);
}

const KernelCreator *Node::getKernelCreator() const {
    return _kernelCreator;
}

void Node::incNodeSnapshot() {
    // snapshot can't be equal
    atomic_inc(&_nodeSnapshot);
}

int64_t Node::readNodeSnapshot() const {
    return atomic_read(&_nodeSnapshot);
}

int64_t Node::getComputeId() const {
    return _metric->scheduleCount();
}

KernelMetric *Node::getMetric() const {
    return _metric;
}

ErrorCode Node::fork(GraphDef *graphDef, const ForkGraphParam &param) {
    if (!graphDef) {
        NAVI_LOG(ERROR, "null graph def");
        return EC_FORK;
    }
    NAVI_LOG(SCHEDULE1, "fork graph: %s", graphDef->DebugString().c_str());
    autil::ScopedLock lock(_forkLock);
    if (_graph->terminated()) {
        // see terminate
        multi_call::freeProtoMessage(graphDef);
        NAVI_LOG(ERROR, "fork failed, graph terminated");
        return EC_FORK;
    }
    if (_forkGraph) {
        multi_call::freeProtoMessage(graphDef);
        NAVI_LOG(ERROR, "double fork");
        return EC_FORK;
    }
    if (!checkForkGraph(graphDef)) {
        NAVI_LOG(ERROR, "invalid fork graph def");
        return EC_FORK;
    }
    auto errorAsEof = param.errorAsEof;
    auto parentGraph = _graph->getGraph();
    auto graphParam = new GraphParam();
    *graphParam = *_graph->getParam();
    auto &runParams = graphParam->runParams;
    runParams.clearNamedData();
    runParams.setTimeoutMs(std::min(param.timeoutMs, getRemainTimeMs()));
    for (const auto &overrideData : param.overrideDataVec) {
        if (!runParams.overrideEdgeData(overrideData)) {
            NAVI_LOG(ERROR,
                     "override edge data failed, graphId [%d], partId [%d], outputNode [%s], outputPort [%s], data "
                     "count [%lu]",
                     overrideData.graphId,
                     overrideData.partId,
                     overrideData.outputNode.c_str(),
                     overrideData.outputPort.c_str(),
                     overrideData.datas.size());
            return EC_OVERRIDE_EDGE_DATA;
        }
    }
    for (const auto &namedData : param.namedDataVec) {
        if (!runParams.addNamedData(namedData)) {
            NAVI_LOG(
                ERROR, "add named data failed, graphId [%d], name [%s]", namedData.graphId, namedData.name.c_str());
            return EC_ADD_NAMED_DATA;
        }
    }
    _forkGraphDef = graphDef;
    _forkGraph = new Graph(graphParam, parentGraph);
    _forkGraph->setErrorAsEof(errorAsEof);
    auto forkDomain = new GraphDomainFork(_forkGraph, _graph->getBiz(), this,
                                          _graph->getPartCount(),
                                          _graph->getPartId(), errorAsEof);
    GraphDomainPtr domain(forkDomain);
    _forkGraph->setGraphDomain(PARENT_GRAPH_ID, domain);
    _forkGraph->setSubResourceMap(param.subResourceMaps);
    auto ec = _forkGraph->init(_forkGraphDef);
    if (EC_NONE != ec) {
        _forkGraph->notifyFinish(ec, GFT_PARENT_FINISH);
        return ec;
    }
    if (!bindDomain(forkDomain)) {
        return EC_FORK;
    }
    ec = _forkGraph->run();
    if (EC_NONE != ec) {
        _forkGraph->notifyFinish(ec, GFT_PARENT_FINISH);
        return ec;
    }
    NAVI_LOG(DEBUG, "fork graph success");
    return EC_NONE;
}

bool Node::checkForkGraph(const GraphDef *graphDef) const {
    bool hasParentGraph = false;
    auto subCount = graphDef->sub_graphs_size();
    for (int i = 0; i < subCount; i++) {
        auto subGraphId = graphDef->sub_graphs(i).graph_id();
        if (USER_GRAPH_ID == subGraphId) {
            NAVI_LOG(ERROR, "fork graph can't has graph output");
            return false;
        }
        if (PARENT_GRAPH_ID == subGraphId) {
            hasParentGraph = true;
            break;
        }
    }
    if (hasParentGraph) {
        return true;
    } else {
        NAVI_LOG(ERROR,
                 "invalid fork graph, no fromInput or toOutput defined ");
        return false;
    }
}

void Node::terminate(ErrorCode ec) {
    {
        // _graph->terminated is true,
        // lock success means no forking process
        autil::ScopedLock lock(_forkLock);
    }
    auto asyncInfo = getAsyncInfo();
    if (asyncInfo) {
        setAsyncInfo(nullptr);
        asyncInfo->terminate();
    }
    if (_forkGraph) {
        _forkGraph->notifyFinish(ec, GFT_PARENT_FINISH);
    }
}

bool Node::bindDomain(GraphDomainFork *domain) {
    // replace output first
    for (size_t i = 0; i < _outputs.size(); i++) {
        auto edge = _outputs[i];
        const auto &portName = _kernelCreator->getOutputPortName(i);
        auto peer = domain->getOutputPeer(portName);
        if (!peer.isValid()) {
            if (edge && !edge->isEof()) {
                NAVI_LOG(ERROR,
                         "unfinished output must be bound by fork, output: %s",
                         portName.c_str());
                return false;
            }
            continue;
        }
        if (!edge) {
            peer.setEof(this);
            continue;
        }
        edge->bindInputNode(peer);
    }
    for (IndexType index = 0; index < _outputGroups.size(); index++) {
        auto group = _outputGroups[index];
        PortIndex groupIndex(INVALID_INDEX, index);
        auto groupName = _kernelCreator->getOutputPortName(groupIndex);
        auto groupSize = group->getOutputCount();
        for (size_t i = 0; i < groupSize; i++) {
            auto edge = group->getEdge(i);
            auto portName = CommonUtil::getGroupPortName(groupName, i);
            auto peer = domain->getOutputPeer(portName);
            if (!peer.isValid()) {
                if (edge && !edge->isEof()) {
                    NAVI_LOG(ERROR,
                             "unfinished group output must be bound by fork, output: %s, index: %lu",
                             portName.c_str(),
                             i);
                    return false;
                }
                continue;
            }
            if (!edge) {
                peer.setEof(this);
                continue;
            }
            edge->bindInputNode(peer);
        }
    }
    for (size_t i = 0; i < _inputs.size(); i++) {
        const auto &edgeOutputInfo = _inputs[i];
        auto edge = edgeOutputInfo.getEdge();
        auto slotId = edgeOutputInfo.getSlotId();
        auto portName = _kernelCreator->getInputPortName(i);
        auto peer = domain->getInputPeer(portName);
        if (!peer) {
            if (!edge->isEof()) {
                NAVI_LOG(ERROR,
                         "unfinished input must be bound by fork, input: %s",
                         portName.c_str());
                return false;
            }
            continue;
        }
        edge->bindOutputNode(slotId, peer);
    }
    auto inputGroupCount = _kernelCreator->inputGroupSize();
    // filter out control and resource input group
    for (IndexType index = 0; index < inputGroupCount; index++) {
        auto group = _inputGroups[index];
        PortIndex groupIndex(INVALID_INDEX, index);
        auto groupName = _kernelCreator->getInputPortName(groupIndex);
        auto groupSize = group->getInputCount();
        for (size_t i = 0; i < groupSize; i++) {
            auto edgeOutputInfo = group->getEdgeOutputInfo(i);
            auto edge = edgeOutputInfo.getEdge();
            auto slotId = edgeOutputInfo.getSlotId();
            auto portName = CommonUtil::getGroupPortName(groupName, i);
            auto peer = domain->getInputPeer(portName);
            if (!peer) {
                if (!edge->isEof()) {
                    NAVI_LOG(ERROR,
                             "unfinished input group must be bound by fork, input: %s, index: %lu",
                             portName.c_str(),
                             i);
                    return false;
                }
                continue;
            }
            edge->bindOutputNode(slotId, peer);
        }
    }
    if (!domain->checkBind()) {
        return false;
    }
    domain->flushData();
    return true;
}

bool Node::getInput(PortIndex index, StreamData &streamData) {
    IndexType inputIndex = 0;
    auto inputInfo = getInputInfo(index, inputIndex);
    if (!inputInfo.isValid()) {
        return false;
    }
    const auto &inputSnapshot = doFillInput(inputInfo, inputIndex);
    streamData = inputSnapshot.data;
    return true;
}

const InputSnapshot &Node::doFillInput(const EdgeOutputInfo &info,
                                       IndexType inputIndex)
{
    assert(inputIndex < _inputSnapshotVec.size());
    auto &inputSnapshot = _inputSnapshotVec[inputIndex];
    if (!inputSnapshot.valid) {
        auto ret = info.getData(inputSnapshot.data.data, inputSnapshot.data.eof, this);
        inputSnapshot.data.hasValue = ret;
        inputSnapshot.valid = true;
    }
    return inputSnapshot;
}

size_t Node::getInputGroupCount() const {
    const auto *kernelDef = _kernelCreator->def();
    return kernelDef->input_groups().size();
}

bool Node::getInputGroupSize(IndexType group, size_t &size) {
    if (group < _inputGroups.size()) {
        size = _inputGroups[group]->getInputCount();
        return true;
    } else {
        return false;
    }
}

bool Node::getOutputGroupSize(IndexType group, size_t &size) {
    if (group < _outputGroups.size()) {
        size = _outputGroups[group]->getOutputCount();
        return true;
    } else {
        return false;
    }
}

size_t Node::getOutputGroupCount() const {
    return _outputGroups.size();
}

ErrorCode Node::getScopeErrorCode() const {
    return _graph->getScopeErrorCode(getScope());
}

int64_t Node::getRemainTimeMs() const {
    return _graph->getGraph()->getRemainTimeMs();
}

const TimeoutChecker *Node::getTimeoutChecker() const {
    return _graph->getGraph()->getTimeoutChecker();
}

bool Node::snapshotGroupInput(IndexType group,
                              InputSnapshot *&begin,
                              InputSnapshot *&end)
{
    if (group < _inputGroups.size()) {
        const auto &inputGroup = _inputGroups[group];
        auto size = inputGroup->getInputCount();
        auto inputIndexStart = inputGroup->getInputIndexStart();
        auto inputIndex = inputIndexStart;
        for (size_t i = 0; i < size; i++) {
            doFillInput(inputGroup->getEdgeOutputInfo(i), inputIndex);
            inputIndex++;
        }
        begin = &_inputSnapshotVec[inputIndexStart];
        end = begin + size;
        return true;
    } else {
        return false;
    }
}

bool Node::setOutput(PortIndex index, const DataPtr &data, bool eof) {
    IndexType outputIndex = 0;
    Edge *edge = nullptr;
    if (!getOutputEdge(index, edge, outputIndex)) {
        return false;
    }
    if (!edge) {
        // output not connected
        return true;
    }
    if (_outputSnapshotVec->isReady(outputIndex)) {
        NAVI_LOG(ERROR, "output data override, group [%d] index [%d] edge[%p]",
                 index.group, index.index, edge);
        _graph->notifyFinish(this, EC_DATA_OVERRIDE);
        return false;
    }
    if (_outputSnapshotVec->isFinish(outputIndex)) {
        NAVI_LOG(ERROR,
                 "output data after eof, group [%d] index [%d] edge "
                 "[%p], new data [%p], new eof [%d], %ld",
                 index.group, index.index, edge, data.get(), eof, getComputeId());
        _graph->notifyFinish(this, EC_DATA_AFTER_EOF);
        return false;
    }
    _outputSnapshotVec->setReady(outputIndex, true);
    _outputSnapshotVec->setFinish(outputIndex, eof);
    return edge->setData(this, data, eof);
}

EdgeOutputInfo Node::getInputInfo(PortIndex index, IndexType &inputIndex) const {
    if (_forkGraph) {
        NAVI_LOG(ERROR, "input not available after fork");
        return {};
    }
    if (!index.isGroup()) {
        if (index.index < _inputs.size()) {
            inputIndex = index.index;
            return _inputs[index.index];
        } else {
            NAVI_LOG(ERROR, "invalid input port index [%d], node input size [%lu]",
                     index.index, _inputs.size());
            return {};
        }
    } else {
        if (index.group < _inputGroups.size()) {
            auto group = _inputGroups[index.group];
            auto inputCount = group->getInputCount();
            if (index.index < inputCount) {
                inputIndex = group->getInputIndexStart() + index.index;
                return group->getEdgeOutputInfo(index.index);
            } else {
                NAVI_LOG(ERROR,
                         "invalid input port index [%d], group [%d] input size [%lu]",
                         index.index, index.group, inputCount);
                return {};
            }
        } else {
            NAVI_LOG(ERROR,
                     "invalid input group index [%d], node input group count [%lu]",
                     index.group, _inputGroups.size());
            return {};
        }
    }
}

bool Node::getOutputEdge(PortIndex index, Edge *&edge,
                         IndexType &outputIndex) const
{
    if (_forkGraph) {
        NAVI_LOG(ERROR, "output not available after fork");
        return false;
    }
    if (!index.isGroup()) {
        if (index.index < _outputs.size()) {
            outputIndex = index.index;
            edge = _outputs[index.index];
            return true;
        } else {
            NAVI_LOG(ERROR,
                     "invalid output port index [%d], node output size [%lu]",
                     index.index, _outputs.size());
            return false;
        }
    } else {
        if (index.group < _outputGroups.size()) {
            auto group = _outputGroups[index.group];
            auto outputCount = group->getOutputCount();
            if (index.index < outputCount) {
                outputIndex = group->getOutputIndexStart() + index.index;
                edge = group->getEdge(index.index);
                return true;
            } else {
                NAVI_LOG(ERROR,
                         "invalid output port index [%d], group [%d] output size [%lu]",
                         index.index, index.group, outputCount);
                return false;
            }
        } else {
            NAVI_LOG(ERROR,
                     "invalid output group index [%d], node output group count [%lu]",
                     index.group, _outputGroups.size());
            return false;
        }
    }
}

Kernel *Node::getKernel() const {
    return _kernel;
}

void Node::setFrozen(bool frozen) {
    _frozen = frozen;
}

bool Node::isFinished() const {
    return SS_FINISH == _scheduleStat;
}

bool Node::getIORange(IoType ioType, const std::string &name, PortIndex &start,
                      PortIndex &end) const
{
    PortInfo portInfo;
    if (IOT_INPUT == ioType) {
        portInfo = _kernelCreator->getInputPortInfo(name);
    } else {
        portInfo = _kernelCreator->getOutputPortInfo(name);
    }
    if (!portInfo.isValid()) {
        NAVI_LOG(ERROR, "no %s port named [%s]", CommonUtil::getIoType(ioType),
                 name.c_str());
        return false;
    }
    if (portInfo.isGroup()) {
        auto groupIndex = portInfo.index.group;
        IndexType portCount = 0;
        if (IOT_INPUT == ioType) {
            portCount = _inputGroups[groupIndex]->getInputCount();
        } else {
            portCount = _outputGroups[groupIndex]->getOutputCount();
        }
        start.group = groupIndex;
        start.index = 0;
        end.group = groupIndex;
        end.index = portCount;
    } else {
        start.group = INVALID_INDEX;
        start.index = portInfo.index.index;
        end.group = INVALID_INDEX;
        end.index = start.index + 1;
    }
    return true;
}

bool Node::getInputNode(PortIndex index, std::string &node) const {
    IndexType inputIndex = 0;
    auto inputInfo = getInputInfo(index, inputIndex);
    if (!inputInfo.isValid()) {
        return false;
    }
    node = inputInfo.getInputNodeName();
    return true;
}

size_t Node::getInputCount() const {
    return _inputSnapshotVec.size();
}

size_t Node::getOutputCount() const {
    auto sum = _outputs.size();
    for (auto group : _outputGroups) {
        sum += group->getOutputCount();
    }
    return sum;
}

void Node::appendResult(NaviResult &result) {
    _graph->getParam()->worker->appendResult(result);
}

void Node::updateTraceLevel(const std::string &levelStr) {
    NAVI_LOG(SCHEDULE1, "update traceLevel[%s]", levelStr.c_str());
    _logger.logger->updateTraceLevel(levelStr);
    _graph->getParam()->runParams.setTraceLevel(levelStr);
}

bool Node::updateTimeoutMs(int64_t timeoutMs) {
    NAVI_LOG(SCHEDULE1, "update timeoutMs[%ld]", timeoutMs);
    return _graph->getGraph()->updateTimeoutMs(timeoutMs);
}

void Node::fillTrace(std::vector<std::string> &traceVec) const {
    TraceCollector collector;
    const auto &userResult = _graph->getParam()->worker->getUserResult();
    const auto &result = userResult->getNaviResult();
    result->fillTrace(collector);

    const auto &logger = _logger.logger;
    logger->collectTrace(collector);

    collector.format(traceVec);
}

LoggingEvent Node::firstErrorEvent() const {
    const auto &userResult = _graph->getParam()->worker->getUserResult();
    const auto &result = userResult->getNaviResult();
    auto event = result->getErrorEvent();
    if (!event.message.empty()) {
        return event;
    }
    return _logger.logger->firstErrorEvent();
}

}
