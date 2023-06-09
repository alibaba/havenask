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
#ifndef NAVI_NODE_H
#define NAVI_NODE_H

#include "navi/common.h"
#include "navi/engine/Edge.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelCreator.h"
#include "navi/engine/KernelMetric.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/proto/KernelDef.pb.h"
#include <map>
#include <string>
#include <vector>

namespace navi {

class ReadyBitMap;
class InputPortGroup;
class OutputPortGroup;
class LocalSubGraph;
class Biz;
class Graph;
class GraphDomainFork;

struct InputSnapshot {
public:
    InputSnapshot() {
        reset();
    }
    void reset() {
        valid = false;
        data.reset(false);
    }
public:
    bool valid;
    StreamData data;
};

class Node
{
private:
    enum SS_STATE : int64_t {
        SS_IDLE,
        SS_SCHEDULING,
        SS_RUNNING,
        SS_FINISH,
    };
public:
    Node(const NaviLoggerPtr &logger, Biz *biz, KernelMetric *metric,
         LocalSubGraph *graph);
    ~Node();
private:
    Node(const Node &);
    Node& operator=(const Node &);
public:
    const std::string &getName() const {
        return _name;
    }
    const std::string &getKernelName() const {
        return _kernelName;
    }
    const NaviObjectLogger &getLogger() const {
        return _logger;
    }
    // kernel context interface
    bool getInput(PortIndex index, StreamData &streamData);
    bool getInputGroupSize(IndexType group, size_t &size);
    size_t getInputGroupCount() const;
    bool getOutputGroupSize(IndexType group, size_t &size);
    bool snapshotGroupInput(IndexType group, InputSnapshot *&begin,
                            InputSnapshot *&end);
    bool setOutput(PortIndex index, const DataPtr &data, bool eof);
    bool outputFinish() const;
    bool inputFinish() const;
    bool inputHasReady() const;
    bool inputFinishConsumed() const;
    size_t getInputCount() const;
    size_t getOutputCount() const;
    bool getIORange(IoType ioType, const std::string &name, PortIndex &start,
                    PortIndex &end) const;
    bool getInputNode(PortIndex index, std::string &node) const;
    size_t getOutputGroupCount() const;
public:
    // engine interface
    bool init(const NodeDef &nodeDef);
    bool bindPort(Port *port);
    bool addInput(const std::string &portName, bool require,
                  const EdgeOutputInfo &edgeOutputInfo, PortIndex &inputIndex,
                  std::string &dataType);
    void setHasInput();
    bool hasInput() const;
    void setHasOutput();
    bool hasOutput() const;
    bool addOutput(Edge *output, const std::string &portName,
                   PortIndex &outputIndex, std::string &dataType);
    bool postInit();
    LocalSubGraph *getLocalGraph() const;
    const std::string &getConfigPath() const;
    Port *getPort() const;
    const ResourceMap &getResourceMap() const;
    bool getPortTypeStr(const std::string &portName, IoType ioType,
                        std::string &typeStr) const;
    void finishPendingOutput();
    bool checkConnection() const;
    bool schedule(const Node *callNode, bool ignoreFrozen = false);
    bool isInline() const;
    size_t outputDegree() const;
    void compute(const ScheduleInfo &schedInfo);
    void incNodeSnapshot();
    int64_t readNodeSnapshot() const;
    LocalSubGraph *getGraph() const;
    const KernelCreator *getKernelCreator() const;
    ErrorCode fork(GraphDef *graphDef, const std::vector<OverrideData> &overrideData);
    void appendResult(NaviResult &result);
    void terminate(ErrorCode ec);
    int64_t getComputeId() const;
    KernelMetric *getMetric() const;

    // bitmap interface
    ReadyBitMap *getInputBitMap(PortIndex inputIndex);
    ReadyBitMap *getOutputBitMap(PortIndex outputIndex);
    ReadyBitMap *initGroupBitMap(bool isInput, IndexType groupIndex,
                                 size_t size);
    AsyncPipePtr createAsyncPipe(bool optional);
    // for test
    Kernel *getKernel() const;
    void setFrozen(bool frozen);
    bool isFinished() const;
private:
    bool initDef(const NodeDef &nodeDef);
    void initBuildinAttrs();
    bool initResourceMap();
    void initIOGroup();
    bool initCreatorStats();
    bool initInputSnapshotVec();
    bool initOutputSnapshotVec();
    void initOutputDegree();
    bool createKernel(const ScheduleInfo &schedInfo);
    bool initKernel(const ScheduleInfo &schedInfo);
    bool initAttribute();
    void deleteKernel(bool inDestruct = false);
    PortIndex doAddInput(const PortInfo &portInfo,
                         const EdgeOutputInfo &outputInfo,
                         bool edgeRequire);
    InputPortGroup *addControlInputGroup();
    InputPortGroup *addResourceInputGroup();
    PortIndex doAddInputGroup(const PortInfo &portInfo,
                              const EdgeOutputInfo &outputInfo);
    PortIndex doAddOutput(const PortInfo &portInfo, Edge *output);
    PortIndex doAddOutputGroup(const PortInfo &portInfo, Edge *output);
    ActivateStrategy getInputActivateStrategy(const InputDef *inputDef,
                                              bool edgeRequire);
    bool checkInput() const;
    bool checkInputGroup() const;
    bool inputOk() const;
    bool outputOk() const;
    bool resourceInputOk() const;
    bool asyncPipeOk() const;
    bool asyncPipeHasReady() const;
    bool asyncPipeFinish() const;
    bool asyncPipeFinishConsumed() const;
    EdgeOutputInfo getInputInfo(PortIndex index, IndexType &inputIndex) const;
    bool getOutputEdge(PortIndex index, Edge *&edge,
                       IndexType &outputIndex) const;
    bool bindDomain(GraphDomainFork *domain);
    void destructData(DataPtr &&data) const;
private:
    bool scheduleLock(const Node *callNode);
    bool doSchedule(const Node *callNode, bool ignoreFrozen, bool &resched);
    ErrorCode doCompute(const ScheduleInfo &schedInfo);
    void clearControlInput();
    const InputSnapshot &doFillInput(const EdgeOutputInfo &info,
                                     IndexType inputIndex);
    void resetIOSnapshot();
    bool checkFinish(ErrorCode ec);
    void propogateEof() const;
    size_t mySwapPoolMallocSize(size_t size);
    size_t mySwapPoolMallocCount(size_t count);

private:
    DECLARE_LOGGER();
    PoolPtr _pool;
    std::string _name;
    std::string _kernelName;
    Biz *_biz;
    LocalSubGraph *_graph;
    Port *_port;
    ResourceMap _resourceMap;
    KernelMetric *_metric;
    std::vector<EdgeOutputInfo> _inputs;
    std::vector<Edge *> _outputs;
    std::vector<Edge *> _controlOutputs;
    bool _hasInput;
    bool _hasOutput;
    ReadyBitMap *_inputReadyMap;
    ReadyBitMap *_asyncPipeReadyMap;
    ReadyBitMap *_asyncPipeConsumedMap;
    ReadyBitMap *_outputReadyMap;
    ReadyBitMap *_controlOutputReadyMap;
    std::vector<InputPortGroup *> _inputGroups;
    std::vector<AsyncPipePtr> *_asyncPipes;
    int64_t _controlInputGroupIndex;
    int64_t _resourceInputGroupIndex;
    std::vector<OutputPortGroup *> _outputGroups;
    std::vector<InputSnapshot> _inputSnapshotVec;
    ReadyBitMap *_outputSnapshotVec;
    size_t _outputDegree;
    SnapshotId _nodeSnapshot;
    volatile SS_STATE _scheduleStat;
    const NodeDef *_def;
    const KernelCreator *_kernelCreator;
    Kernel *_kernel;
    bool _kernelInited;
    CreatorStats *_creatorStats;
    bool _frozen;
    bool _skipConfig;
    bool _skipInit;
    bool _stopAfterInit;
    bool _skipDeleteKernel;
    autil::RecursiveThreadMutex _forkLock;
    GraphDef *_forkGraphDef;
    Graph *_forkGraph;
private:
    static constexpr int64_t INVALID_GROUP_INDEX = -1;
    static constexpr float INLINE_LATENCY_THRESHOLD_US = 300.0f;
};

NAVI_TYPEDEF_PTR(Node);

}

#endif //NAVI_NODE_H
