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
#include "navi/tester/KernelTester.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/Node.h"
#include "navi/log/Layout.h"
#include "navi/tester/NaviTesterInputKernel.h"
#include "navi/tester/NaviTesterResource.h"
#include "navi/tester/TestKernelConfigContext.h"
#include "navi/tester/TesterDef.h"
#include "navi/util/CommonUtil.h"

namespace navi {

KernelTester::KernelTester(const std::string &logPrefix,
                           const NaviPtr &navi,
                           const KernelTestInfo &info)
    : _logger(this, logPrefix.c_str(), NAVI_TLS_LOGGER->logger)
    , _threadId(std::this_thread::get_id())
    , _navi(navi)
    , _info(info)
    , _testerResource(new NaviTesterResource())
{
    _testerResource->initLogger(logPrefix, _logger.logger);
}

KernelTester::~KernelTester() {
    if (_result) {
        _result->abort();
    }
    NAVI_LOG(ERROR, "tester destruct");
}

bool KernelTester::init() {
    auto graphDef = buildTestGraph();
    if (!graphDef)  {
        NAVI_LOG(ERROR, "build test graph failed");
        return false;
    }
    if (!runGraph(graphDef)) {
        return false;
    }
    return true;
}

GraphDef *KernelTester::buildTestGraph() {
    auto graphDef = new GraphDef();
    GraphBuilder builder(graphDef);
    auto graphId = builder.newSubGraph(NAVI_TESTER_BIZ);
    builder.subGraph(graphId)
        .testerMode(true);
    auto testerNode = builder.node(NAVI_TESTER_NODE)
                          .kernel(_info.kernelName)
                          .jsonAttrs(_info.attrs)
                          .binaryAttrsFromMap(_info.binaryAttrs)
                          .skipConfig(_info.skipConfig)
                          .skipInit(_info.skipInit)
                          .stopAfterInit(true)
                          .skipDeleteKernel(_info.skipDeleteKernel);
    auto controlNode = builder.node(NAVI_TESTER_CONTROL_NODE)
                       .kernel(NAVI_TESTER_CONTROL_KERNEL)
                       .stopAfterInit(true);
    controlNode.out(NAVI_TESTER_CONTROL_KERNEL_FAKE_OUTPUT).asGraphOutput("");
    for (const auto &input : _info.inputs) {
        builder.node(input)
            .kernel(NAVI_TESTER_INPUT_KERNEL)
            .stopAfterInit(true)
            .out(NAVI_TESTER_INPUT_KERNEL_OUTPUT)
            .to(testerNode.in(input));
    }
    for (const auto &output : _info.outputs) {
        builder.node(output)
            .kernel(NAVI_TESTER_OUTPUT_KERNEL)
            .stopAfterInit(true)
            .in(NAVI_TESTER_OUTPUT_KERNEL_INPUT)
            .from(testerNode.out(output));
    }
    return graphDef;
}

bool KernelTester::runGraph(GraphDef *graphDef) {
    ResourceMap resourceMap;
    resourceMap.addResource(
        std::dynamic_pointer_cast<Resource>(_testerResource));
    resourceMap.addResource(*_info.resources);
    RunGraphParams params;
    _result = _navi->runLocalGraph(graphDef, params, resourceMap);
    auto ec = getErrorCode();
    if (EC_NONE != ec) {
        NAVI_LOG(ERROR, "run graph failed, ec [%s], msg [%s]",
                 CommonUtil::getErrorString(ec), getErrorMessage().c_str());
        return false;
    }
    return true;
}

// user interface
bool KernelTester::setInput(const std::string &portName, const DataPtr &data) {
    return setInput(portName, data, false);
}

bool KernelTester::setInputEof(const std::string &portName) {
    return setInput(portName, nullptr, true);
}

bool KernelTester::setInput(const std::string &portName, const DataPtr &data,
                            bool eof)
{
    NAVI_LOG(DEBUG, "setInput, port [%s] data [%p] eof [%d]", portName.c_str(),
             data.get(), eof);
    if (checkFail()) {
        return false;
    }
    return _testerResource->setInput(portName, data, eof);
}

bool KernelTester::compute() {
    NAVI_LOG(DEBUG, "begin compute");
    if (std::this_thread::get_id() != _threadId) {
        NAVI_LOG(ERROR, "DO NOT RUN IN ANOTHER THREAD");
        return false;
    }

    if (checkFail()) {
        return false;
    }
    if (!_testerResource->validate()) {
        NAVI_LOG(ERROR, "validate failed");
        return false;
    }
    if (EC_NONE != computeKernel()) {
        return false;
    }
    return true;
}

bool KernelTester::getOutput(const std::string &portName, DataPtr &data, bool &eof) {
    auto ret = _testerResource->getOutput(portName, data, eof);
    NAVI_LOG(DEBUG, "get output, ret [%d] port [%s] data[%p] eof[%d]", ret,
             portName.c_str(), data.get(), eof);
    return ret;
}

bool KernelTester::isFinished() const {
    auto node = _testerResource->getTestNode();
    if (!node) {
        return true;
    }
    return node->isFinished();
}

Kernel *KernelTester::getKernel() const {
    auto node = _testerResource->getTestNode();
    if (!node) {
        return nullptr;
    }
    return node->getKernel();
}

KernelConfigContextPtr KernelTester::getConfigContext() const {
    return TestKernelConfigContext::buildConfigContext(_info);
}

KernelInitContextPtr KernelTester::getInitContext() const {
    auto node = _testerResource->getTestNode();
    if (!node) {
        return nullptr;
    }
    return KernelInitContextPtr(new KernelInitContext(node));
}

KernelComputeContextPtr KernelTester::getComputeContext() const {
    auto node = _testerResource->getTestNode();
    if (!node) {
        return nullptr;
    }
    return KernelComputeContextPtr(new KernelComputeContext(node));
}

ErrorCode KernelTester::getErrorCode() const {
    if (!_result) {
        return EC_INIT_GRAPH;
    }
    return _result->getNaviResult()->ec;
}

bool KernelTester::hasError() const {
    return EC_NONE != getErrorCode();
}

std::string KernelTester::getErrorMessage() const {
    return getErrorEvent().message;
}

LoggingEvent KernelTester::getErrorEvent() const {
    return _logger.logger->firstErrorEvent();
}

// inner method
bool KernelTester::checkFail() const {
    auto ec = getErrorCode();
    if (EC_NONE != ec) {
        NAVI_LOG(ERROR, "kernel already failed, ec [%s] msg [%s]",
                 CommonUtil::getErrorString(ec), getErrorMessage().c_str());
        return true;
    }
    return false;
}

Node *KernelTester::getNode() const {
    return _testerResource->getTestNode();
}

ErrorCode KernelTester::computeKernel() {
    auto testNode = _testerResource->getTestNode();
    auto testComputeId = testNode->getComputeId();
    auto ret = testNode->schedule(testNode, true);
    if (!ret) {
        NAVI_LOG(INFO, "kernel not activated");
        return EC_KERNEL;
    }
    auto newTestComputeId = testNode->getComputeId();
    if (newTestComputeId != testComputeId + 1) {
        NAVI_LOG(ERROR,
                 "bug!!! kernel not compute after activation, old computeId "
                 "[%ld], new computeId [%ld]",
                 testComputeId, newTestComputeId);
        return EC_UNKNOWN;
    }
    return EC_NONE;
}

std::ostream &operator<<(std::ostream &os, const KernelTester &tester) {
    PatternLayout layout;
    layout.setLogPattern(DEFAULT_LOG_PATTERN);
    os << "ec: " << CommonUtil::getErrorString(tester.getErrorCode())
       << std::endl
       << "msg: ";
    if (tester.hasError()) {
        os << layout.format(tester.getErrorEvent());
    }
    return os;
}

}
