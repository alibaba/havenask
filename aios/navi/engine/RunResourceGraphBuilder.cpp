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
#include "navi/engine/RunResourceGraphBuilder.h"
#include "navi/ops/ResourceKernelDef.h"

namespace navi {

RunResourceGraphBuilder::RunResourceGraphBuilder(
        CreatorManager *creatorManager,
        const MultiLayerResourceMap &multiLayerResourceMap,
        int32_t scope,
        const std::string &requireNode,
        const std::unordered_map<std::string, std::string> &replaceMap,
        GraphBuilder &builder)
    : _creatorManager(creatorManager)
    , _multiLayerResourceMap(multiLayerResourceMap)
    , _scope(scope)
    , _requireNode(requireNode)
    , _replaceMap(replaceMap)
    , _builder(builder)
    , _graphId(builder.currentSubGraph())
{
}

RunResourceGraphBuilder::~RunResourceGraphBuilder() {
    _builder.subGraph(_graphId);
}

const std::string &RunResourceGraphBuilder::getReplaceR(const std::string &inputResource) const {
    auto it = _replaceMap.find(inputResource);
    if (_replaceMap.end() == it) {
        return inputResource;
    }
    const auto &to = it->second;
    if (!_creatorManager->supportReplace(inputResource, to)) {
        NAVI_KERNEL_LOG(ERROR,
                        "can't replace [%s] by [%s], not enabled, call enableReplaceR first",
                        inputResource.c_str(),
                        to.c_str());
        throw autil::legacy::ExceptionBase("error, can't replace");
    }
    return to;
}

N RunResourceGraphBuilder::build(const std::string &inputResource) {
    const auto &resource = getReplaceR(inputResource);
    auto stage = _creatorManager->getResourceStage(resource);
    auto nodeName = getResourceNodeName(resource, stage);
    if (RS_GRAPH == stage) {
        _builder.addRSGraphResourceGraph();
    }
    if (_builder.hasNode(nodeName)) {
        auto n = _builder.resourceNode(nodeName).integerAttr(RESOURCE_ATTR_REQUIRE, 1);
        return n;
    }
    NAVI_KERNEL_LOG(TRACE3, "add resource create [%s]", resource.c_str());
    auto n = _builder.resourceNode(nodeName)
                 .kernel(RESOURCE_CREATE_KERNEL)
                 .integerAttr(RESOURCE_ATTR_REQUIRE, 1)
                 .attr(RESOURCE_ATTR_NAME, resource);
    if (RS_KERNEL == stage) {
        n.attr(RESOURCE_ATTR_REQUIRE_NODE, _requireNode);
    }
    if (_multiLayerResourceMap.hasResource(resource)) {
        return n;
    }
    addDependResourceRecur(n, resource);
    return n;
}

void RunResourceGraphBuilder::addDependResourceRecur(N n, const std::string &resource) {
    auto dependResourceMapPtr =
        _creatorManager->getResourceDependResourceMap(resource);
    if (!dependResourceMapPtr) {
        NAVI_KERNEL_LOG(DEBUG, "resource [%s] not registered",
                        resource.c_str());
        return;
    }
    auto inputPort = n.in(NODE_RESOURCE_INPUT_PORT);
    for (const auto &pair : *dependResourceMapPtr) {
        const auto &dependName = getReplaceR(pair.first);
        if (_multiLayerResourceMap.hasResource(dependName)) {
            NAVI_KERNEL_LOG(DEBUG,
                            "resource [%s] depend [%s] in multiLayerResourceMap, ignore",
                            resource.c_str(),
                            dependName.c_str());
            continue;
        }
        auto stage = _creatorManager->getResourceStage(dependName);
        auto nodeName = getResourceNodeName(dependName, stage);
        auto nodeExist = _builder.hasNode(nodeName);
        if (!nodeExist) {
            auto dependN = _builder.resourceNode(nodeName)
                               .kernel(RESOURCE_CREATE_KERNEL)
                               .attr(RESOURCE_ATTR_NAME, dependName);
            dependN.out(RESOURCE_CREATE_OUTPUT).to(inputPort.autoNext());
            if (RS_KERNEL == stage) {
                dependN.attr(RESOURCE_ATTR_REQUIRE_NODE, _requireNode);
            }
            NAVI_KERNEL_LOG(DEBUG, "link resource [%s] depend [%s]",
                            resource.c_str(), dependName.c_str());
            addDependResourceRecur(dependN, dependName);
        } else {
            _builder.resourceNode(nodeName).out(RESOURCE_CREATE_OUTPUT).to(inputPort.autoNext());
            NAVI_KERNEL_LOG(DEBUG, "resource [%s] depend [%s] already exist",
                            resource.c_str(), dependName.c_str());
        }
    }
}

std::string RunResourceGraphBuilder::getResourceNodeName(const std::string &resource, ResourceStage stage) const {
    switch (stage) {
    case RS_SUB_GRAPH:
        return resource;
    case RS_SCOPE:
        return resource + "@scope_" + std::to_string(_scope);
    case RS_KERNEL:
        if (_requireNode.empty()) {
            throw autil::legacy::ExceptionBase("error, can't build kernel resource");
        }
        return resource + "@kernel_" + _requireNode;
    default:
        return resource;
    }
}
}

