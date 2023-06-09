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
#include "navi/engine/KernelInitContext.h"

#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Node.h"

namespace navi {

KernelInitContext::KernelInitContext(Node *node)
    : _node(node)
    , _subGraph(node->getLocalGraph())
    , _port(node->getPort())
    , _resourceMap(node->getResourceMap()) {}

TimeoutChecker *KernelInitContext::getTimeoutChecker() const {
    auto worker = _subGraph->getWorker();
    assert(worker);
    return worker->getTimeoutChecker();
}

PoolPtr KernelInitContext::getPool() const {
    return _subGraph->getPool();
}

bool KernelInitContext::getInputNode(PortIndex index, std::string &node) {
    return _node->getInputNode(index, node);
}

bool KernelInitContext::getIORange(IoType ioType, const std::string &name, PortIndex &start, PortIndex &end) const {
    return _node->getIORange(ioType, name, start, end);
}

bool KernelInitContext::getInputGroupSize(IndexType group, size_t &size) {
    auto groupCount = _node->getInputGroupCount();
    if (group >= groupCount) {
        NAVI_KERNEL_LOG(ERROR,
                        "group index [%u] overflow, node [%s] kernel [%s] "
                        "input group count [%lu]",
                        group,
                        _node->getName().c_str(),
                        _node->getKernelName().c_str(),
                        groupCount);
        return false;
    }
    return _node->getInputGroupSize(group, size);
}

bool KernelInitContext::getOutputGroupSize(IndexType group, size_t &size) {
    return _node->getOutputGroupSize(group, size);
}

const ResourceMap &KernelInitContext::getDependResourceMap() const {
    return _resourceMap;
}

const std::string &KernelInitContext::getConfigPath() const {
    return _node->getConfigPath();
}

const std::string &KernelInitContext::getThisBizName() const {
    return _node->getGraph()->getBizName();
}

NaviPartId KernelInitContext::getThisPartId() const {
    return _node->getGraph()->getPartId();
}

AsyncPipePtr KernelInitContext::createAsyncPipe(bool optional) {
    return _node->createAsyncPipe(optional);
}

} // namespace navi
