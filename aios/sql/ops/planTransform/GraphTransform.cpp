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
#include "sql/ops/planTransform/GraphTransform.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <rapidjson/document.h>
#include <utility>

#include "autil/DataBuffer.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/util/TypeDefine.h"
#include "iquan/common/Common.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/builder/GraphDesc.h"
#include "navi/proto/GraphDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/TableDistribution.h"
#include "sql/common/WatermarkType.h"
#include "sql/common/common.h"
#include "sql/ops/planTransform/TableScanOpUtil.h"

namespace iquan {
class DynamicParams;
} // namespace iquan

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace navi;
using namespace iquan;

namespace sql {

static std::string TABLE_TYPE_UNKNOWN = "unknown_type";

namespace plan {

AUTIL_LOG_SETUP(plan, DelayDpInfo);

GraphRoot *GraphRoot::getRoot() {
    return _parent == nullptr ? this : _parent;
}

void GraphRoot::addInput(GraphRoot *p) {
    _inputs.insert(p);
}

void GraphRoot::removeInput(GraphRoot *p) {
    _inputs.erase(p);
}

const std::set<GraphRoot *> &GraphRoot::getInputs() const {
    return _inputs;
}

void GraphRoot::setOutput(GraphRoot *p) {
    _output = p;
}

GraphRoot *GraphRoot::getOutput() const {
    return _output;
}

const std::vector<GraphRoot *> &GraphRoot::getLeafs() const {
    return _leafs;
}

void GraphRoot::addLeaf(GraphRoot *leaf) {
    assert(_parent == nullptr);
    leaf->_parent = this;
    _leafs.push_back(leaf);
}

navi::GraphId GraphRoot::getGraphId() const {
    return _graphId;
}

void GraphRoot::setGraphId(navi::GraphId id) {
    _graphId = id;
}

navi::GraphBuilder *GraphRoot::getBuilder() const {
    return _builder;
}

void GraphRoot::setBuilder(navi::GraphBuilder *p) {
    _builder = p;
}

bool GraphRoot::getInlineMode() const {
    return _inlineMode;
}
void GraphRoot::setInlineMode(bool mode) {
    _inlineMode = mode;
}

const std::string &GraphRoot::getBizName() const {
    return _bizName;
}

void GraphRoot::setBizName(const std::string &name) {
    _bizName = name;
}

const std::string &GraphRoot::getCurDist() const {
    return _curDist;
}

void GraphRoot::setCurDist(const std::string &dist) {
    _curDist = dist;
}

const std::string &GraphRoot::getRemoteDist() const {
    return _remoteDist;
}

void GraphRoot::setRemoteDist(const std::string &dist) {
    _remoteDist = dist;
}

bool GraphRoot::canDelayDp() const {
    return _delayDp;
}

bool GraphRoot::canRemoteDelayDp() const {
    return _remoteDelayDp;
}

void GraphRoot::setRemoteDelayDp(bool delay) {
    _remoteDelayDp = delay;
}

navi::GraphDef *GraphRoot::getGraphDef() const {
    return _graphDef.get();
}

void GraphRoot::ownGraphDef(navi::GraphDef *p) {
    _graphDef.reset(p);
}

void GraphRoot::mergeGraphInfo() {
    assert(_parent == nullptr);

    if (_output != nullptr) {
        _output = _output->getRoot();
    }

    if (!_leafs.empty()) {
        set<GraphRoot *> mergedInputs = _inputs;
        for (auto p : _leafs) {
            auto &inputs = p->_inputs;
            mergedInputs.insert(inputs.begin(), inputs.end());
        }
        _inputs.swap(mergedInputs);
    }
    if (!_inputs.empty()) {
        _delayDp = all_of(_inputs.begin(), _inputs.end(), [](auto p) { return p->_remoteDelayDp; });
    }
}

void GraphRoot::mergeRoot(GraphRoot *a, GraphRoot *b) {
    if (a->_leafs.size() < b->_leafs.size()) {
        mergeRoot(b, a);
        return;
    }
    a->addLeaf(b);
    for (auto p : b->_leafs) {
        a->addLeaf(p);
    }
    b->_leafs.clear();
}

PlanNode *PlanNode::accept(NodeVisitor &visitor) {
    return visitor.visitNode(*this);
}

void PlanNode::addBuildInput(PlanNode &node, const navi::P &p) {
    assert(false);
}

const std::string &PlanNode::getMergedName() const {
    if (nodeNames.size() == 1u) {
        return nodeNames[0];
    } else {
        return mergedName;
    }
}

PlanNode *ScanNode::accept(NodeVisitor &visitor) {
    return visitor.visitScan(*this);
}

PlanNode *ExchangeNode::accept(NodeVisitor &visitor) {
    return visitor.visitExchange(*this);
}

void ExchangeNode::addBuildInput(PlanNode &node, const navi::P &p) {
    auto ret = output2buildInputs.try_emplace(&node);
    ret.first->second.emplace_back(p);
}

DelayDpInfo::DelayDpInfo(GraphRoot *root)
    : _root(root) {}

void DelayDpInfo::addInputExchange(ExchangeNode *p) {
    _inputExs.push_back(p);
}

void DelayDpInfo::addOutputExchange(ExchangeNode *p) {
    _outputExs.push_back(p);
}

bool DelayDpInfo::prepareSubGraph() {
    if (_root->getRoot() != _root || _root->getGraphDef() == nullptr) {
        SQL_LOG(WARN, "bug: invalid graph root");
        return false;
    }
    auto builder = _root->getBuilder();
    builder->subGraph(_root->getGraphId());

    auto inputSize = _inputExs.size();
    _inputNames.reserve(inputSize);
    _inputDists.reserve(inputSize);
    for (size_t i = 0; i < inputSize; ++i) {
        auto p = _inputExs[i];
        auto phName = "ph_" + StringUtil::toString(i);
        _inputNames.push_back(phName);
        _inputDists.push_back(p->root->getRemoteDist());
        auto buildOutput
            = builder->node(phName).kernel(PLACEHOLDER_KERNEL_NAME).out(PLACEHOLDER_OUTPUT_PORT);
        for (const auto &inputPair : p->output2buildInputs) {
            for (const auto &buildInput : inputPair.second) {
                buildOutput.to(buildInput).require(true);
            }
        }
    }

    auto outputSize = _outputExs.size();
    _outputNames.reserve(outputSize);
    for (size_t i = 0; i < outputSize; ++i) {
        auto p = _outputExs[i];
        auto exInputSize = p->input2buildOutput.size();
        if (exInputSize != 1u) {
            SQL_LOG(WARN, "bug: invalid exchange input size: %ld", exInputSize);
            return false;
        }
        auto input = p->input2buildOutput.begin()->first;
        _outputNames.push_back(input->getMergedName());
    }

    initWrapperOpAttr();
    return true;
}

void DelayDpInfo::initWrapperOpAttr() {
    DataBuffer buffer;

    buffer.write(_inputNames);
    buffer.serializeToStringWithSection(_opAttrs[DELAY_DP_INPUT_NAMES_ATTR]);
    buffer.clear();

    buffer.write(_inputDists);
    buffer.serializeToStringWithSection(_opAttrs[DELAY_DP_INPUT_DISTS_ATTR]);
    buffer.clear();

    buffer.write(_outputNames);
    buffer.serializeToStringWithSection(_opAttrs[DELAY_DP_OUTPUT_NAMES_ATTR]);
    buffer.clear();

    auto subDef = _root->getGraphDef();
    subDef->SerializeToString(&(_opAttrs[DELAY_DP_GRAPH_ATTR]));
}

std::map<std::string, std::string> &DelayDpInfo::getWrapperOpAttrs() {
    return _opAttrs;
}

bool DelayDpInfo::replaceSubGraph(plan::PlanNode &node,
                                  navi::P buildInput,
                                  navi::P buildOutput,
                                  const std::function<GraphRoot *()> &rootCreator) {
    node.inputMap[DEFAULT_INPUT_PORT].assign(_inputExs.begin(), _inputExs.end());
    for (auto input : _inputExs) {
        input->root->setRemoteDist(node.root->getCurDist());
        input->output2buildInputs.clear();
        auto ret = input->output2buildInputs.try_emplace(&node);
        ret.first->second.emplace_back(buildInput.autoNext());
    }
    for (auto output : _outputExs) {
        auto newRoot = rootCreator();
        if (newRoot == nullptr) {
            SQL_LOG(WARN, "create new GraphRoot failed");
            return false;
        }
        node.root->addLeaf(newRoot);
        newRoot->setBizName(node.root->getBizName());
        newRoot->setCurDist(node.root->getCurDist());
        newRoot->setRemoteDist(output->root->getRemoteDist());
        output->root = newRoot;
        output->inputMap[DEFAULT_INPUT_PORT] = {&node};
        output->input2buildOutput.clear();
        output->input2buildOutput.try_emplace(&node, buildOutput.autoNext());
    }
    return true;
}

} // namespace plan

AUTIL_LOG_SETUP(sql, GraphTransform);
const string GraphTransform::EMPTY_STRING = "";

GraphTransformEnv::GraphTransformEnv() {
    disableWatermark = autil::EnvUtil::getEnv("disableWatermark", disableWatermark);
    useQrsTimestamp = autil::EnvUtil::getEnv("useQrsTimestamp", useQrsTimestamp);
    searchNewBiz = autil::EnvUtil::getEnv("searchNewBiz", true);
}

GraphTransformEnv::~GraphTransformEnv() {}

const GraphTransformEnv &GraphTransformEnv::get() {
    static GraphTransformEnv instance;
    return instance;
}

GraphTransform::Config::Config(const iquan::ExecConfig &execConfig,
                               const iquan::IquanDqlRequest &request,
                               const iquan::DynamicParams &innerDynamicParams)
    : _execConfig(execConfig)
    , _request(request)
    , _innerDynamicParams(innerDynamicParams)
    , parallelNum(_execConfig.parallelConfig.parallelNum)
    , resultAllowSoftFailure(_execConfig.resultAllowSoftFailure)
    , sourceId(getRequestParam(IQUAN_EXEC_SOURCE_ID))
    , sourceSpec(getRequestParam(IQUAN_EXEC_SOURCE_SPEC))
    , taskQueue(getRequestParam(IQUAN_EXEC_TASK_QUEUE))
    , otherRunGraphParam(getRequestParam(IQUAN_EXEC_USER_KV))
    , qrsBizName(getRequestParam(IQUAN_EXEC_QRS_BIZ_NAME, execConfig.thisBizName))
    , searcherBizName(getRequestParam(IQUAN_EXEC_SEARCHER_BIZ_NAME))
    , leaderPreferLevel(getRequestParam(IQUAN_EXEC_LEADER_PREFER_LEVEL)) {
    {
        auto inlineWorker = getRequestParam(IQUAN_EXEC_INLINE_WORKER);
        if (inlineWorker == "qrs") {
            qrsGraphInline = true;
        } else if (inlineWorker == "searcher") {
            searcherGraphInline = true;
        } else if (inlineWorker == "all") {
            qrsGraphInline = true;
            searcherGraphInline = true;
        }
    }
    {
        const auto &tables = _execConfig.parallelConfig.parallelTables;
        parallelTables.insert(tables.begin(), tables.end());
    }
    {
        auto tmp = getRequestParam(IQUAN_EXEC_OVERRIDE_TO_LOGIC_TABLE);
        auto tokens = StringUtil::split(tmp, ",");
        logicTableOps.insert(tokens.begin(), tokens.end());
    }
    params = &_request.dynamicParams;
    innerParams = &_innerDynamicParams;
}

const std::string &GraphTransform::Config::getRequestParam(const std::string &key,
                                                           const std::string &defaultValue) const {
    const auto &execParams = _request.execParams;
    auto iter = execParams.find(key);
    if (iter != execParams.end()) {
        return iter->second;
    } else {
        return defaultValue;
    }
}

static autil::legacy::RapidValue *getJsonValue(iquan::PlanOp &op, const string &key) {
    if (op.jsonAttrs == nullptr) {
        return nullptr;
    }
    auto &jsonAttrs = *op.jsonAttrs;
    if (!jsonAttrs.IsObject()) {
        return nullptr;
    }
    auto iter = jsonAttrs.FindMember(key);
    if (iter != jsonAttrs.MemberEnd()) {
        return &(iter->value);
    } else {
        return nullptr;
    }
}

static string getJsonStringValue(iquan::PlanOp &op, const string &key) {
    auto p = getJsonValue(op, key);
    if (p != nullptr && p->IsString()) {
        return p->GetString();
    } else {
        return GraphTransform::EMPTY_STRING;
    }
}

GraphTransform::GraphTransform(const Config &config)
    : _config(config) {}

GraphTransform::~GraphTransform() {
    for (auto &pair : _id2node) {
        delete pair.second;
    }
    for (auto p : _subRoots) {
        delete p;
    }
    for (auto p : _builders) {
        delete p;
    }
}

static string createDefaultTableDistribution() {
    TableDistribution dist;
    dist.partCnt = 1;
    return ToJsonString(dist, true);
}

static const string ROOT_GRAPH_TABLE_DISTRIBUTION = createDefaultTableDistribution();

bool GraphTransform::sqlPlan2Graph(iquan::SqlPlan &sqlPlan,
                                   navi::GraphDef &graph,
                                   std::vector<std::string> &outputPorts,
                                   std::vector<std::string> &outputNodes) {
    auto rootNode = preparePlanNode(sqlPlan);
    auto root = addGraphRoot();
    if (root == nullptr) {
        return false;
    }
    if (_config.qrsGraphInline) {
        root->setInlineMode(true);
    }
    _rootBizName = getQrsBizName();
    root->setBizName(_rootBizName);
    root->setCurDist(ROOT_GRAPH_TABLE_DISTRIBUTION);
    if (!linkVisit(*rootNode, *root)) {
        return false;
    }
    _rootBuilder = new GraphBuilder(&graph);
    _builders.push_back(_rootBuilder);
    root->mergeGraphInfo();
    _rootGraphId = newSubGraph(*root, _rootBuilder);
    rootNode = buildVisit(*rootNode);
    if (rootNode == nullptr) {
        return false;
    }
    if (!buildDelayDpGraph()) {
        SQL_LOG(WARN, "buildDelayDpGraph failed");
        return false;
    }
    addExchangeBorder();
    outputNodes = rootNode->nodeNames;
    outputPorts.assign(outputNodes.size(), DEFAULT_OUTPUT_PORT);
    return true;
}

GraphTransform::ErrorCode GraphTransform::getErrorCode() const {
    return _errorCode;
}

plan::PlanNode *GraphTransform::visitNode(plan::PlanNode &node) {
    SQL_LOG(TRACE3, "visit common nodeId:%ld", node.op->id);
    switch (node.inputMap.size()) {
    case 0:
        buildNode(node, 1);
        return &node;
    case 1:
        return buildSingleInputNode(node);
    default:
        return buildMultiInputNode(node);
    }
}

plan::PlanNode *GraphTransform::visitScan(plan::ScanNode &node) {
    SQL_LOG(TRACE3, "visit scan nodeId:%ld", node.op->id);
    addTargetWatermark(node);
    auto parallel = getScanParallel(*(node.op));
    buildNode(node, parallel);
    return &node;
}

plan::PlanNode *GraphTransform::visitExchange(plan::ExchangeNode &node) {
    SQL_LOG(TRACE3, "visit exchange nodeId:%ld", node.op->id);
    auto childIter = node.inputMap.begin();
    if (childIter == node.inputMap.end()) {
        _errorCode = ErrorCode::LACK_NODE_INPUT;
        SQL_LOG(WARN, "exchange nodeId:%ld has no input", node.op->id);
        return nullptr;
    }
    auto &inputNodes = childIter->second;
    if (inputNodes.empty()) {
        _errorCode = ErrorCode::LACK_NODE_INPUT;
        SQL_LOG(WARN, "exchange nodeId:%ld has no input", node.op->id);
        return nullptr;
    }
    auto curBuilder = _builder;
    auto curGraphId = curBuilder->currentSubGraph();
    auto root = node.root->getRoot();
    auto graphId = root->getGraphId();
    if (graphId == navi::INVALID_GRAPH_ID) {
        root->mergeGraphInfo();
        if (root->canDelayDp()) {
            newDelayDpSubGraph(*root);
        } else if (root->getBizName() != _rootBizName) {
            newSubGraph(*root, _rootBuilder);
        } else {
            switchToSubGraph(_rootGraphId, _rootBuilder);
        }
    } else {
        switchToSubGraph(graphId, root->getBuilder());
    }

    auto p = buildVisit(*(inputNodes[0]));
    if (p == nullptr) {
        return nullptr;
    }
    buildMergedNode(*p);
    auto buildOutput = _builder->node(p->getMergedName()).out(DEFAULT_OUTPUT_PORT);
    node.input2buildOutput.try_emplace(p, buildOutput);

    switchToSubGraph(curGraphId, curBuilder);
    _exchangeNodes.push_back(&node);
    addDelayDpInfo(node);
    return &node;
}

std::string GraphTransform::getQrsBizName() {
    return _config.qrsBizName;
}

std::string GraphTransform::getRemoteBizName(iquan::PlanOp &op) {
    if (_config.searcherBizName.empty()) {
        auto name = getJsonStringValue(op, "node_name");
        if (name.empty()) {
            name = getJsonStringValue(op, "db_name");
            if (name == SQL_DEFAULT_EXTERNAL_DATABASE_NAME) {
                return getQrsBizName();
            }
        }
        auto tableName = getJsonStringValue(op, "table_name");
        if (GraphTransformEnv::get().searchNewBiz && tableName.empty() == false) {
            return name + "." + tableName + ".write";
        }
        return name + "." + isearch::DEFAULT_SQL_BIZ_NAME;
    } else {
        return _config.searcherBizName;
    }
}

std::string GraphTransform::getJsonSegment(iquan::PlanOp &op, const char *key) {
    auto p = getJsonValue(op, key);
    if (p == nullptr) {
        return EMPTY_STRING;
    }
    return op.jsonAttr2Str(_config.params, _config.innerParams, p);
}

size_t GraphTransform::getScanParallel(iquan::PlanOp &op) {
    if (_config.parallelNum > 1) {
        if (_config.parallelTables.empty()) {
            return _config.parallelNum;
        }
        auto tableName = getJsonStringValue(op, SCAN_TABLE_NAME_ATTR);
        if (_config.parallelTables.count(tableName) > 0) {
            return _config.parallelNum;
        }
    }
    return 1;
}

plan::PlanNode *GraphTransform::preparePlanNode(iquan::SqlPlan &sqlPlan) {
    size_t nodeNum = sqlPlan.opList.size();
    if (nodeNum == 0u) {
        return nullptr;
    }
    _id2node.reserve(nodeNum * 2);
    plan::PlanNode *node = nullptr;
    for (auto &op : sqlPlan.opList) {
        preprocessOp(op);
        node = createPlanNode(op);
        if (node == nullptr) {
            return nullptr;
        }
    }
    return node;
}

void GraphTransform::preprocessOp(iquan::PlanOp &op) {
    preprocessOpName(op);
    preprocessOpInputs(op);
}

void GraphTransform::preprocessOpName(iquan::PlanOp &op) {
    op.patchInt64Attrs[iquan::OP_ID] = iquan::BASE_OP_ID + op.id;
    op.patchStrAttrs[IQUAN_EXEC_ATTR_SOURCE_SPEC] = _config.sourceSpec;
    auto &opName = op.opName;
    if (opName == "TableScanOp") {
        std::string tableType = TABLE_TYPE_UNKNOWN;
        auto it = op.jsonAttrs->FindMember("table_type");
        if (it != op.jsonAttrs->MemberEnd()) {
            if (it->value.IsString()) {
                tableType = it->value.GetString();
            }
        }
        opName = TableScanOpUtil::getScanKernelName(tableType);
        SQL_LOG(TRACE3, "get kernel[%s] for table type[%s]", opName.c_str(), tableType.c_str());
    } else if (opName == "AggregateOp") {
        opName = "sql.AggKernel";
    } else if (opName == "TableFunctionScanOp") {
        opName = "sql.TvfKernel";
    } else if (opName == "LookupJoinOp") {
        std::string tableType = TABLE_TYPE_UNKNOWN;
        auto it = op.jsonAttrs->FindMember("build_node");
        if (it != op.jsonAttrs->MemberEnd() && it->value.IsObject()) {
            const auto &valueMap = it->value;
            auto typeIt = valueMap.FindMember("table_type");
            if (typeIt != valueMap.MemberEnd() && typeIt->value.IsString()) {
                tableType = typeIt->value.GetString();
            }
        }
        if (IQUAN_TABLE_TYPE_EXTERNAL == tableType) {
            opName = "sql.ExternalLookupJoinKernel";
        } else {
            opName = "sql.LookupJoinKernel";
        }
    } else {
        if (autil::StringUtil::endsWith(opName, "Op")) {
            opName = opName.substr(0, opName.size() - 2) + "Kernel";
        }
        opName = "sql." + opName;
    }
    if (_config.logicTableOps.count(opName) > 0) {
        op.opName = "sql.LogicalTableScanKernel";
        op.patchStrAttrs["table_type"] = "logical";
    }
}

void GraphTransform::preprocessOpInputs(iquan::PlanOp &op) {
    auto &inputs = op.inputs;
    if (inputs.size() == 1 && inputs.find("input0") == inputs.end()) {
        auto iter = inputs.begin();
        inputs["input0"] = iter->second;
        inputs.erase(iter);
    }
}

plan::PlanNode *GraphTransform::createPlanNode(iquan::PlanOp &op) {
    auto p = createPlanNode(op.id, op.opName);
    if (p == nullptr) {
        return nullptr;
    }
    p->op = &op;
    return p;
}

plan::PlanNode *GraphTransform::createPlanNode(const std::string &opName) {
    ++_maxNodeId;
    return createPlanNode(_maxNodeId, opName);
}

plan::PlanNode *GraphTransform::createPlanNode(size_t id, const std::string &opName) {
    auto &p = _id2node[id];
    if (p != nullptr) {
        _errorCode = ErrorCode::DUPLICATE_NODE_ID;
        SQL_LOG(WARN, "duplicate node id:%ld, opName:%s", id, opName.c_str());
        return nullptr;
    }
    if (opName == SCAN_OP || opName == ASYNC_SCAN_OP) {
        p = new plan::ScanNode();
    } else if (opName == EXCHANGE_OP) {
        p = new plan::ExchangeNode();
    } else {
        p = new plan::PlanNode();
    }
    if (id > _maxNodeId) {
        _maxNodeId = id;
    }
    return p;
}

plan::GraphRoot *GraphTransform::addGraphRoot() {
    auto root = new plan::GraphRoot();
    _subRoots.push_back(root);
    return root;
}

plan::PlanNode *GraphTransform::findNode(size_t nodeId) {
    auto iter = _id2node.find(nodeId);
    if (iter == _id2node.end()) {
        _errorCode = ErrorCode::NODE_NOT_EXIST;
        SQL_LOG(WARN, "nodeId:%ld not exist", nodeId);
        return nullptr;
    }
    return iter->second;
}

plan::PlanNode *GraphTransform::linkVisit(size_t nodeId, plan::GraphRoot &root) {
    auto p = findNode(nodeId);
    if (p == nullptr) {
        return nullptr;
    }
    return linkVisit(*p, root);
}

plan::PlanNode *GraphTransform::linkVisit(plan::PlanNode &node, plan::GraphRoot &root) {
    auto nodeId = node.op->id;
    plan::PlanNode *p = nullptr;
    SQL_LOG(TRACE3, "link nodeId:%ld", nodeId);
    switch (node.status) {
    case plan::NodeStatus::LINKING:
        _errorCode = ErrorCode::GRAPH_CIRCLE;
        SQL_LOG(WARN, "nodeId:%ld is linking, detect graph circle", nodeId);
        break;
    case plan::NodeStatus::INIT:
        SQL_LOG(TRACE3, "first link nodeId:%ld", nodeId);
        node.status = plan::NodeStatus::LINKING;
        node.root = &root;
        p = linkInitNode(node, root);
        node.status = plan::NodeStatus::LINKED;
        break;
    case plan::NodeStatus::LINKED:
        SQL_LOG(TRACE3, "share link nodeId:%ld", nodeId);
        p = linkSharedNode(node, root);
        break;
    default:
        _errorCode = ErrorCode::UNKNOWN;
        SQL_LOG(WARN, "unknown error");
        break;
    }
    return p;
}

plan::PlanNode *GraphTransform::linkInitNode(plan::PlanNode &node, plan::GraphRoot &root) {
    auto p = dynamic_cast<plan::ExchangeNode *>(&node);
    if (p != nullptr) {
        return linkExchangeNode(*p, root);
    } else {
        return linkCommonNode(node, root);
    }
}

plan::PlanNode *GraphTransform::linkSharedNode(plan::PlanNode &node, plan::GraphRoot &curRoot) {
    auto nodeId = node.op->id;
    if (dynamic_cast<plan::ExchangeNode *>(&node) != nullptr) {
        SQL_LOG(TRACE3, "exchange nodeId:%ld has share", nodeId);
        return &node;
    }

    if (node.root == &curRoot) {
        SQL_LOG(TRACE3, "nodeId:%ld shared in same graph", nodeId);
        return &node;
    }

    auto curShared = curRoot.getRoot();
    auto nodeShared = node.root->getRoot();
    bool curEmptyLeaf = curShared->getLeafs().empty();
    bool nodeEmptyLeaf = nodeShared->getLeafs().empty();
    if (curEmptyLeaf == true && nodeEmptyLeaf == true) {
        SQL_LOG(TRACE3, "nodeId:%ld and current root all have no share", nodeId);
        curShared->addLeaf(nodeShared);
    } else if (curEmptyLeaf == false && nodeEmptyLeaf == false) {
        SQL_LOG(TRACE3, "nodeId:%ld and current root all have share", nodeId);
        plan::GraphRoot::mergeRoot(curShared, nodeShared);
    } else if (nodeEmptyLeaf == false) {
        SQL_LOG(TRACE3, "only nodeId:%ld root has share, add current to it", nodeId);
        nodeShared->addLeaf(curShared);
    } else {
        SQL_LOG(TRACE3, "only current root has share, add nodeId:%ld to it", nodeId);
        curShared->addLeaf(nodeShared);
    }
    return &node;
}

plan::PlanNode *GraphTransform::linkExchangeNode(plan::ExchangeNode &node, plan::GraphRoot &root) {
    SQL_LOG(TRACE3, "link exchange node");
    auto subRoot = addGraphRoot();
    root.addInput(subRoot);
    subRoot->setOutput(&root);
    if (subRoot == nullptr) {
        return nullptr;
    }
    if (_config.searcherGraphInline) {
        subRoot->setInlineMode(true);
    }
    auto &op = *(node.op);
    auto remoteBizName=getRemoteBizName(op);
    SQL_LOG(DEBUG, "getRemoteBizName is %s",remoteBizName.c_str());
    subRoot->setBizName(remoteBizName);
    subRoot->setCurDist(getJsonSegment(op, "table_distribution"));
    auto remoteDist = getJsonSegment(op, "output_distribution");
    if (remoteDist.empty()) {
        remoteDist = root.getCurDist();
    }
    subRoot->setRemoteDist(remoteDist);
    auto remoteDelayDp = getJsonSegment(op, "output_prunable");
    if (remoteDelayDp == "1") {
        SQL_LOG(TRACE3, "enable remote_delay_deploy, node:%ld", op.id);
        subRoot->setRemoteDelayDp(true);
    }
    node.root = subRoot;
    return linkCommonNode(node, *subRoot);
}

plan::PlanNode *GraphTransform::linkCommonNode(plan::PlanNode &node, plan::GraphRoot &root) {
    SQL_LOG(TRACE3, "link common node");
    for (const auto &pair : node.op->inputs) {
        const auto &inputIds = pair.second;
        auto &inputNodes = node.inputMap[pair.first];
        inputNodes.reserve(inputIds.size());
        for (auto nodeId : inputIds) {
            auto p = linkVisit(nodeId, root);
            if (p == nullptr) {
                return nullptr;
            }
            inputNodes.push_back(p);
        }
    }
    return &node;
}

plan::PlanNode *GraphTransform::buildVisit(plan::PlanNode &node) {
    auto nodeId = node.op->id;
    plan::PlanNode *p = nullptr;
    switch (node.status) {
    case plan::NodeStatus::LINKED:
        SQL_LOG(TRACE3, "visit build nodeId:%ld", nodeId);
        node.status = plan::NodeStatus::BUILDING;
        p = node.accept(*this);
        node.status = plan::NodeStatus::FINISH;
        break;
    case plan::NodeStatus::FINISH:
        SQL_LOG(TRACE3, "share finish nodeId:%ld", nodeId);
        p = &node;
        break;
    default:
        _errorCode = ErrorCode::UNKNOWN;
        SQL_LOG(WARN, "unknown error");
        break;
    }
    return p;
}

void GraphTransform::buildNode(plan::PlanNode &node, size_t parallel) {
    SQL_LOG(TRACE3, "build nodeId:%ld, parallel:%ld", node.op->id, parallel);
    if (parallel <= 1u) {
        auto nodeName = StringUtil::toString(node.op->id);
        node.nodeNames = {nodeName};
        buildNode(node, nodeName);
    } else {
        node.nodeNames.resize(parallel);
        for (size_t i = 0; i < parallel; ++i) {
            auto nodeName = StringUtil::toString(node.op->id) + "_" + StringUtil::toString(i);
            node.nodeNames[i] = nodeName;
            node.op->patchInt64Attrs[PARALLEL_NUM_ATTR] = parallel;
            node.op->patchInt64Attrs[PARALLEL_INDEX_ATTR] = i;
            buildNode(node, nodeName);
        }
    }
}

void GraphTransform::buildMergedNode(plan::PlanNode &node) {
    auto parallel = node.nodeNames.size();
    if (parallel <= 1u) {
        return;
    }
    SQL_LOG(TRACE3, "merge parallel:%ld for nodeId:%ld", parallel, node.op->id);
    node.mergedName = addMergedNode();
    auto buildInput = _builder->node(node.mergedName).in(DEFAULT_INPUT_PORT);
    for (size_t i = 0; i < parallel; ++i) {
        buildEdge(node.nodeNames[i], buildInput.autoNext());
    }
    return;
}

std::string GraphTransform::addMergedNode() {
    auto nodeName = UNION_OP + "_" + StringUtil::toString(_mergedNodeCount);
    ++_mergedNodeCount;
    SQL_LOG(TRACE3, "add merge node:%s", nodeName.c_str());
    _builder->node(nodeName).kernel(UNION_OP);
    return nodeName;
}

void GraphTransform::buildNode(plan::PlanNode &node, const std::string &nodeName) {
    _builder->node(nodeName)
        .kernel(node.op->opName)
        .jsonAttrs(node.op->jsonAttr2Str(_config.params, _config.innerParams))
        .binaryAttrsFromMap(node.op->binaryAttrs);
}

plan::PlanNode *GraphTransform::buildSingleInputNode(plan::PlanNode &node) {
    auto childIter = node.inputMap.begin();
    const auto &inputNodes = childIter->second;
    switch (inputNodes.size()) {
    case 0:
        buildNode(node, 1);
        return &node;
    case 1:
        return buildSinglePlainInputNode(node);
    default:
        return buildSingleGroupInputNode(node);
    }
}

plan::PlanNode *GraphTransform::buildSinglePlainInputNode(plan::PlanNode &node) {
    SQL_LOG(TRACE3, "build single plain input node");
    auto childIter = node.inputMap.begin();
    const auto &inputPort = childIter->first;
    auto p = buildVisit(*(childIter->second[0]));
    if (p == nullptr) {
        return nullptr;
    }
    auto inputSize = p->nodeNames.size();
    if (inputSize > 1u) {
        buildNode(node, inputSize);
        for (size_t i = 0; i < inputSize; ++i) {
            auto buildInput = _builder->node(node.nodeNames[i]).in(inputPort);
            buildEdge(p->nodeNames[i], buildInput);
        }
    } else {
        buildNode(node, 1);
        auto buildInput = _builder->node(node.nodeNames[0]).in(inputPort);
        if (inputSize == 1u) {
            buildEdge(p->nodeNames[0], buildInput);
        } else {
            p->addBuildInput(node, buildInput);
        }
    }
    return &node;
}

plan::PlanNode *GraphTransform::buildSingleGroupInputNode(plan::PlanNode &node) {
    SQL_LOG(TRACE3, "build single group input node");
    auto childIter = node.inputMap.begin();
    const auto &inputPort = childIter->first;
    auto &inputNodes = childIter->second;
    for (auto p : inputNodes) {
        if (buildVisit(*p) == nullptr) {
            return nullptr;
        }
    }
    buildNode(node, 1);
    const auto &nodeName = node.nodeNames[0];
    auto port = _builder->node(nodeName).in(inputPort);
    auto groupSize = inputNodes.size();
    for (size_t i = 0; i < groupSize; ++i) {
        auto buildInput = port.autoNext();
        auto p = inputNodes[i];
        auto inputSize = p->nodeNames.size();
        if (inputSize > 1u) {
            buildMergedNode(*p);
        }
        if (inputSize == 0u) {
            p->addBuildInput(node, buildInput);
        } else {
            buildEdge(p->getMergedName(), buildInput);
        }
    }
    return &node;
}

plan::PlanNode *GraphTransform::buildMultiInputNode(plan::PlanNode &node) {
    for (const auto &pair : node.inputMap) {
        const auto &inputNodes = pair.second;
        if (inputNodes.size() != 1u) {
            _errorCode = ErrorCode::MULTI_GROUP_INPUT;
            SQL_LOG(WARN, "detect multi group input, not supported");
            return nullptr;
        }
    }
    return buildMultiPlainInputNode(node);
}

plan::PlanNode *GraphTransform::buildMultiPlainInputNode(plan::PlanNode &node) {
    SQL_LOG(TRACE3, "build multi plain input node");
    struct InputInfo {
        const string &inputPort;
        plan::PlanNode *p;
        size_t parallel;
        InputInfo(const std::string &inputPort_, plan::PlanNode *p_, size_t parallel_)
            : inputPort(inputPort_)
            , p(p_)
            , parallel(parallel_) {}
    };
    vector<InputInfo> inputInfos;
    inputInfos.reserve(node.inputMap.size());
    size_t maxParallel = 1;
    for (auto &pair : node.inputMap) {
        auto p = buildVisit(*(pair.second[0]));
        if (p == nullptr) {
            return nullptr;
        }
        size_t inputParallel = p->nodeNames.size();
        if (maxParallel > 1 && inputParallel > 1) {
            buildMergedNode(*p);
            inputParallel = 1;
        }
        inputInfos.emplace_back(pair.first, p, inputParallel);
        maxParallel = (inputParallel > maxParallel ? inputParallel : maxParallel);
    }
    buildNode(node, maxParallel);
    unordered_map<plan::PlanNode *, vector<navi::P>> node2merge;
    for (size_t i = 0; i < maxParallel; ++i) {
        auto buildNode = _builder->node(node.nodeNames[i]);
        for (const auto &inputInfo : inputInfos) {
            auto buildInput = buildNode.in(inputInfo.inputPort);
            if (inputInfo.parallel == 1u) {
                buildEdge(inputInfo.p->getMergedName(), buildInput);
            } else if (inputInfo.parallel == 0u) {
                auto ret = node2merge.try_emplace(inputInfo.p);
                ret.first->second.emplace_back(buildInput);
            } else {
                buildEdge(inputInfo.p->nodeNames[i], buildInput);
            }
        }
    }
    for (const auto &pair : node2merge) {
        auto p = pair.first;
        const auto &buildInputs = pair.second;
        auto finalBuildInput = buildInputs[0];
        if (buildInputs.size() > 1u) {
            const auto &mergedName = addMergedNode();
            for (const auto &buildInput : buildInputs) {
                buildEdge(mergedName, buildInput);
            }
            finalBuildInput = _builder->node(mergedName).in(DEFAULT_INPUT_PORT).autoNext();
        }
        p->addBuildInput(node, finalBuildInput);
    }
    return &node;
}

void GraphTransform::switchToSubGraph(plan::PlanNode &node) {
    auto root = node.root->getRoot();
    switchToSubGraph(root->getGraphId(), root->getBuilder());
}

void GraphTransform::switchToSubGraph(navi::GraphId id, navi::GraphBuilder *builder) {
    SQL_LOG(TRACE3, "switchToSubGraph: builder: %p, graphId: %d", builder, id);
    _builder = builder;
    builder->subGraph(id);
}

navi::GraphId GraphTransform::newSubGraph(plan::GraphRoot &root, navi::GraphBuilder *builder) {
    const auto &bizName = root.getBizName();
    _builder = builder;
    auto graphId = _builder->newSubGraph(bizName);
    builder->errorHandleStrategy(_config.resultAllowSoftFailure ? EHS_ERROR_AS_EOF
                                                                : EHS_ERROR_AS_FATAL);
    SQL_LOG(
        TRACE3, "new subGraph: %s, builder: %p, graphId: %d", bizName.c_str(), builder, graphId);
    root.setGraphId(graphId);
    root.setBuilder(_builder);
    if (root.getInlineMode()) {
        _builder->inlineMode(true);
    }
    const auto &curDist = root.getCurDist();
    if (!curDist.empty()) {
        _builder->subGraphAttr("table_distribution", curDist);
    }
    return graphId;
}

navi::GraphId GraphTransform::newDelayDpSubGraph(plan::GraphRoot &root) {
    auto p = new navi::GraphDef();
    root.ownGraphDef(p);
    auto builder = new GraphBuilder(p);
    _builders.push_back(builder);
    return newSubGraph(root, builder);
}

void GraphTransform::addDelayDpInfo(plan::ExchangeNode &node) {
    auto curRoot = node.root->getRoot();
    if (curRoot->canDelayDp()) {
        auto &info = getDelayDpInfo(*curRoot);
        info.addOutputExchange(&node);
        SQL_LOG(TRACE3, "delay dp info %p add output node %ld", &info, node.op->id);
    }

    auto outputRoot = node.root->getOutput();
    if (!outputRoot) {
        return;
    }
    outputRoot = outputRoot->getRoot();
    if (outputRoot->canDelayDp()) {
        auto &info = getDelayDpInfo(*outputRoot);
        info.addInputExchange(&node);
        SQL_LOG(TRACE3, "delay dp info %p add input node %ld", &info, node.op->id);
    }
}

plan::DelayDpInfo &GraphTransform::getDelayDpInfo(plan::GraphRoot &root) {
    auto p = &root;
    auto ret = _delayDpInfoMap.try_emplace(p, p);
    auto &info = ret.first->second;
    if (ret.second) {
        _delayDpInfos.push_back(&info);
    }
    return info;
}

bool GraphTransform::buildDelayDpGraph() {
    size_t newNodeCount = 0;
    auto rootRoot = _subRoots[0]->getRoot();
    for (auto info : _delayDpInfos) {
        SQL_LOG(TRACE3, "build delay dp %p begin", info);
        if (!info->prepareSubGraph()) {
            return false;
        }
        switchToSubGraph(_rootGraphId, _rootBuilder);
        auto newNodeName = DELAY_DP_OP + "_" + StringUtil::toString(newNodeCount);
        ++newNodeCount;
        auto &opAttrs = info->getWrapperOpAttrs();
        if (_config.debug) {
            opAttrs[DELAY_DP_DEBUG_ATTR] = "1";
        } else {
            opAttrs[DELAY_DP_DEBUG_ATTR] = "0";
        }
        auto newNode = _builder->node(newNodeName).kernel(DELAY_DP_OP).binaryAttrsFromMap(opAttrs);

        auto p = createPlanNode(DELAY_DP_OP);
        if (p == nullptr) {
            SQL_LOG(WARN, "allocate plan node failed");
            return false;
        }
        p->status = plan::NodeStatus::FINISH;
        p->root = rootRoot;
        p->nodeNames = {newNodeName};
        auto buildInput = newNode.in(DEFAULT_INPUT_PORT);
        auto buildOutput = newNode.out(DEFAULT_OUTPUT_PORT);
        auto rootCreator = [this]() -> plan::GraphRoot * { return this->addGraphRoot(); };
        if (!info->replaceSubGraph(*p, buildInput, buildOutput, rootCreator)) {
            return false;
        }
        SQL_LOG(TRACE3, "build delay dp graph end");
    }
    return true;
}

void GraphTransform::addExchangeBorder() {
    _builder = _rootBuilder;
    for (auto p : _exchangeNodes) {
        auto &node = *p;
        SQL_LOG(TRACE3, "link exchange border, nodeId:%ld", node.op->id);
        auto graphId = node.root->getRoot()->getGraphId();
        const auto &buildOutput = node.input2buildOutput.begin()->second;
        for (const auto &pair : node.output2buildInputs) {
            auto remoteGraphId = pair.first->root->getRoot()->getGraphId();
            bool sameGraph = (graphId == remoteGraphId);
            for (const auto &buildInput : pair.second) {
                addExchangeBorder(buildOutput, node, buildInput, sameGraph);
            }
        }
    }
}

void GraphTransform::addExchangeBorder(navi::P buildOutput,
                                       plan::ExchangeNode &exchangeNode,
                                       navi::P buildInput,
                                       bool sameGraph) {
    if (sameGraph) {
        if (exchangeNode.root->getRoot()->getGraphId() == _rootGraphId) {
            buildInput.from(buildOutput).require(true);
        } else {
            _builder->subGraph(_rootGraphId);
            auto identityName = addMergedNode();
            auto innerBuildOutput = _builder->node(identityName).out(DEFAULT_OUTPUT_PORT);
            buildInput.from(innerBuildOutput).require(true).merge("sql.TableMergeKernel");
            _builder->subGraphAttr("table_distribution", ROOT_GRAPH_TABLE_DISTRIBUTION);
            auto innerBuildInput = _builder->node(identityName).in(DEFAULT_INPUT_PORT).autoNext();
            addExchangeBorder(buildOutput, exchangeNode, innerBuildInput, false);
        }
    } else {
        auto buildEdge = buildInput.from(buildOutput).require(true);
        buildEdge.merge("sql.TableMergeKernel");
        buildEdge.split("sql.TableSplitKernel")
            .attr("table_distribution", exchangeNode.root->getRemoteDist());
    }
}

void GraphTransform::addTargetWatermark(plan::ScanNode &node) {
    static const bool disableWatermark = GraphTransformEnv::get().disableWatermark;
    static const bool useQrsTimestamp = GraphTransformEnv::get().useQrsTimestamp;

    const auto &leaderPreferLevel = _config.leaderPreferLevel;
    int64_t watermark = 0;
    if (disableWatermark || !autil::StringUtil::fromString(leaderPreferLevel, watermark)
        || watermark == 0) {
        // disabled, pass
    } else {
        switch (watermark) {
        case 1:
            node.op->patchInt64Attrs["target_watermark"]
                = useQrsTimestamp ? TimeUtility::currentTime() : 0;
            node.op->patchInt64Attrs["target_watermark_type"] = WatermarkType::WM_TYPE_SYSTEM_TS;
            break;
        default:
            node.op->patchInt64Attrs["target_watermark"] = watermark;
            node.op->patchInt64Attrs["target_watermark_type"] = WatermarkType::WM_TYPE_MANUAL;
        }
    }
}

void GraphTransform::buildEdge(const std::string &outputNode, const navi::P &buildInput) {
    _builder->node(outputNode).out(DEFAULT_OUTPUT_PORT).to(buildInput).require(true);
}

} // namespace sql
