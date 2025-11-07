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

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "navi/builder/GraphDesc.h" // IWYU pragma: keep
#include "navi/common.h"
#include "navi/proto/GraphDef.pb.h"

namespace iquan {
class DynamicParams;
class ExecConfig;
class IquanDqlRequest;
class PlanOp;
class SqlPlan;
} // namespace iquan
namespace navi {
class GraphBuilder;
} // namespace navi

namespace sql {

namespace plan {

class GraphRoot {
public:
    GraphRoot() = default;
    GraphRoot(const GraphRoot &) = delete;

public:
    GraphRoot *getRoot();

    void addInput(GraphRoot *p);
    void removeInput(GraphRoot *p);
    const std::set<GraphRoot *> &getInputs() const;

    void setOutput(GraphRoot *p);
    GraphRoot *getOutput() const;

    const std::vector<GraphRoot *> &getLeafs() const;
    void addLeaf(GraphRoot *leaf);

    navi::GraphId getGraphId() const;
    void setGraphId(navi::GraphId id);

    navi::GraphBuilder *getBuilder() const;
    void setBuilder(navi::GraphBuilder *p);

    bool getInlineMode() const;
    void setInlineMode(bool mode);

    const std::string &getBizName() const;
    void setBizName(const std::string &name);

    const std::string &getCurDist() const;
    void setCurDist(const std::string &dist);

    const std::string &getRemoteDist() const;
    void setRemoteDist(const std::string &dist);

    bool canDelayDp() const;
    bool canRemoteDelayDp() const;
    void setRemoteDelayDp(bool delay);

    navi::GraphDef *getGraphDef() const;
    void ownGraphDef(navi::GraphDef *p);

    void mergeGraphInfo();

public:
    static void mergeRoot(GraphRoot *a, GraphRoot *b);

private:
    GraphRoot *_parent {nullptr};
    std::set<GraphRoot *> _inputs;
    GraphRoot *_output {nullptr};
    std::vector<GraphRoot *> _leafs;
    bool _inlineMode {false};
    std::string _bizName;
    std::string _curDist;
    std::string _remoteDist;
    bool _delayDp {false};
    bool _remoteDelayDp {false};
    navi::GraphId _graphId {navi::INVALID_GRAPH_ID};
    navi::GraphBuilder *_builder {nullptr};
    std::unique_ptr<navi::GraphDef> _graphDef;
};

class ExchangeNode;
class PlanNode;
class ScanNode;

class NodeVisitor {
public:
    virtual ~NodeVisitor() = default;

public:
    virtual PlanNode *visitNode(PlanNode &node) = 0;
    virtual PlanNode *visitScan(ScanNode &node) = 0;
    virtual PlanNode *visitExchange(ExchangeNode &node) = 0;
};

enum class NodeStatus {
    INIT,
    LINKING,
    LINKED,
    BUILDING,
    FINISH,
};

class PlanNode {
public:
    PlanNode() = default;
    virtual ~PlanNode() = default;
    PlanNode(const PlanNode &) = delete;

public:
    virtual PlanNode *accept(NodeVisitor &visitor);
    virtual void addBuildInput(PlanNode &node, const navi::P &p);

public:
    const std::string &getMergedName() const;

public:
    NodeStatus status {NodeStatus::INIT};
    iquan::PlanOp *op {nullptr};
    GraphRoot *root {nullptr};
    std::map<std::string, std::vector<PlanNode *>> inputMap;
    std::vector<std::string> nodeNames;
    std::string mergedName;
};

class ScanNode : public PlanNode {
public:
    PlanNode *accept(NodeVisitor &visitor) override;
};

class ExchangeNode : public PlanNode {
public:
    PlanNode *accept(NodeVisitor &visitor) override;
    void addBuildInput(PlanNode &node, const navi::P &p) override;

public:
    std::unordered_map<PlanNode *, navi::P> input2buildOutput;
    std::unordered_map<PlanNode *, std::vector<navi::P>> output2buildInputs;
};

class DelayDpInfo {
public:
    DelayDpInfo(GraphRoot *root);
    DelayDpInfo(const DelayDpInfo &) = delete;

public:
    void addInputExchange(ExchangeNode *p);
    void addOutputExchange(ExchangeNode *p);
    bool prepareSubGraph();
    std::map<std::string, std::string> &getWrapperOpAttrs();
    bool replaceSubGraph(plan::PlanNode &node,
                         navi::P buildInput,
                         navi::P buildOutput,
                         const std::function<GraphRoot *()> &rootCreator);

private:
    void initWrapperOpAttr();

private:
    GraphRoot *_root {nullptr};
    std::vector<ExchangeNode *> _inputExs;
    std::vector<ExchangeNode *> _outputExs;
    std::vector<std::string> _inputNames;
    std::vector<std::string> _inputDists;
    std::vector<std::string> _outputNames;
    std::map<std::string, std::string> _opAttrs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace plan

struct GraphTransformEnv {
private:
    GraphTransformEnv();
    ~GraphTransformEnv();

public:
    static const GraphTransformEnv &get();

public:
    bool disableWatermark = false;
    bool useQrsTimestamp = true;
    bool searchNewBiz = true;
};

class GraphTransform : public plan::NodeVisitor {
public:
    class Config {
    public:
        Config(const iquan::ExecConfig &execConfig,
               const iquan::IquanDqlRequest &request,
               const iquan::DynamicParams &innerDynamicParams);

    private:
        const std::string &getRequestParam(const std::string &key,
                                           const std::string &defaultValue = EMPTY_STRING) const;
        bool enableDynamicReplacement() const;

    private:
        const iquan::ExecConfig &_execConfig;
        const iquan::IquanDqlRequest &_request;
        const iquan::DynamicParams &_innerDynamicParams;

    public:
        bool debug {false};
        size_t parallelNum;
        bool resultAllowSoftFailure;
        std::string sourceId;
        std::string sourceSpec;
        std::string taskQueue;
        std::string otherRunGraphParam;
        std::string qrsBizName;
        std::string searcherBizName;
        std::string leaderPreferLevel;
        bool qrsGraphInline {false};
        bool searcherGraphInline {false};
        std::set<std::string> parallelTables;
        std::set<std::string> logicTableOps;
        iquan::DynamicParams const *params {nullptr};
        iquan::DynamicParams const *innerParams {nullptr};
    };

    enum class ErrorCode {
        NONE,
        DUPLICATE_NODE_ID,
        NODE_NOT_EXIST,
        GRAPH_CIRCLE,
        NODE_IN_MULTI_GRAPH,
        LACK_NODE_INPUT,
        MULTI_GROUP_INPUT,
        UNKNOWN,
    };

public:
    GraphTransform(const Config &config);
    ~GraphTransform();

public:
    bool sqlPlan2Graph(iquan::SqlPlan &sqlPlan,
                       navi::GraphDef &graph,
                       std::vector<std::string> &outputPorts,
                       std::vector<std::string> &outputNodes);
    ErrorCode getErrorCode() const;

public:
    plan::PlanNode *visitNode(plan::PlanNode &node) override;
    plan::PlanNode *visitScan(plan::ScanNode &node) override;
    plan::PlanNode *visitExchange(plan::ExchangeNode &node) override;

private:
    std::string getQrsBizName();
    std::string getRemoteBizName(iquan::PlanOp &op);
    std::string getJsonSegment(iquan::PlanOp &op, const char *key);
    size_t getScanParallel(iquan::PlanOp &op);
    plan::PlanNode *preparePlanNode(iquan::SqlPlan &sqlPlan);
    void preprocessOp(iquan::PlanOp &op);
    void preprocessOpName(iquan::PlanOp &op);
    void preprocessOpInputs(iquan::PlanOp &op);
    plan::PlanNode *createPlanNode(iquan::PlanOp &op);
    plan::PlanNode *createPlanNode(const std::string &opName);
    plan::PlanNode *createPlanNode(size_t id, const std::string &opName);
    plan::GraphRoot *addGraphRoot();
    plan::PlanNode *findNode(size_t nodeId);
    plan::PlanNode *linkVisit(size_t nodeId, plan::GraphRoot &root);
    plan::PlanNode *linkVisit(plan::PlanNode &node, plan::GraphRoot &root);
    plan::PlanNode *linkInitNode(plan::PlanNode &node, plan::GraphRoot &root);
    plan::PlanNode *linkSharedNode(plan::PlanNode &node, plan::GraphRoot &curRoot);
    plan::PlanNode *linkExchangeNode(plan::ExchangeNode &node, plan::GraphRoot &root);
    plan::PlanNode *linkCommonNode(plan::PlanNode &node, plan::GraphRoot &root);
    plan::PlanNode *buildVisit(plan::PlanNode &node);
    void buildNode(plan::PlanNode &node, size_t parallel);
    void buildNode(plan::PlanNode &node, const std::string &nodeName);
    void buildMergedNode(plan::PlanNode &node);
    std::string addMergedNode();
    plan::PlanNode *buildSingleInputNode(plan::PlanNode &node);
    plan::PlanNode *buildSinglePlainInputNode(plan::PlanNode &node);
    plan::PlanNode *buildSingleGroupInputNode(plan::PlanNode &node);
    plan::PlanNode *buildMultiInputNode(plan::PlanNode &node);
    plan::PlanNode *buildMultiPlainInputNode(plan::PlanNode &node);
    void switchToSubGraph(plan::PlanNode &node);
    void switchToSubGraph(navi::GraphId id, navi::GraphBuilder *builder);
    navi::GraphId newSubGraph(plan::GraphRoot &root, navi::GraphBuilder *builder);
    navi::GraphId newDelayDpSubGraph(plan::GraphRoot &root);
    void addDelayDpInfo(plan::ExchangeNode &node);
    plan::DelayDpInfo &getDelayDpInfo(plan::GraphRoot &root);
    bool buildDelayDpGraph();
    void addExchangeBorder();
    void addExchangeBorder(navi::P buildOutput,
                           plan::ExchangeNode &exchangeNode,
                           navi::P buildInput,
                           bool sameGraph);
    void addTargetWatermark(plan::ScanNode &node);
    void buildEdge(const std::string &outputNode, const navi::P &buildInput);

public:
    static const std::string EMPTY_STRING;

private:
    const Config &_config;
    size_t _maxNodeId {0};
    std::unordered_map<size_t, plan::PlanNode *> _id2node;
    std::vector<plan::GraphRoot *> _subRoots;
    std::vector<navi::GraphBuilder *> _builders;
    std::vector<plan::ExchangeNode *> _exchangeNodes;
    size_t _mergedNodeCount {0};
    ErrorCode _errorCode {ErrorCode::NONE};
    navi::GraphBuilder *_builder {nullptr};
    std::string _rootBizName;
    navi::GraphBuilder *_rootBuilder {nullptr};
    navi::GraphId _rootGraphId {navi::INVALID_GRAPH_ID};
    std::unordered_map<plan::GraphRoot *, plan::DelayDpInfo> _delayDpInfoMap;
    std::vector<plan::DelayDpInfo *> _delayDpInfos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
