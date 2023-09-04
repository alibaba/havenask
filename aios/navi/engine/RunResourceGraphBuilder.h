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

#include "navi/builder/GraphBuilder.h"
#include "navi/engine/CreatorManager.h"
#include "navi/engine/MultiLayerResourceMap.h"

namespace navi {

class RunResourceGraphBuilder
{
public:
    RunResourceGraphBuilder(CreatorManager *creatorManager,
                            const MultiLayerResourceMap &multiLayerResourceMap,
                            int32_t scope,
                            const std::string &requireNode,
                            const std::unordered_map<std::string, std::string> &replaceMap,
                            GraphBuilder &builder);
    ~RunResourceGraphBuilder();
    RunResourceGraphBuilder(const RunResourceGraphBuilder &) = delete;
    RunResourceGraphBuilder &operator=(const RunResourceGraphBuilder &) = delete;
public:
    N build(const std::string &resource);
private:
    const std::string &getReplaceR(const std::string &inputResource) const;
    void addDependResourceRecur(N n, const std::string &inputResource);
    std::string getResourceNodeName(const std::string &resource, ResourceStage stage) const;
private:
    CreatorManager *_creatorManager;
    const MultiLayerResourceMap &_multiLayerResourceMap;
    int32_t _scope;
    const std::string &_requireNode;
    const std::unordered_map<std::string, std::string> &_replaceMap;
    GraphBuilder &_builder;
    GraphId _graphId;
};

}

