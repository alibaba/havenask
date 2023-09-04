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

namespace navi {

typedef std::unordered_map<ResourceStage, std::map<std::string, bool> >
    ResourceStageMap;

class InitResourceGraphBuilder {
private:
    struct SubGraphInfo {
        ResourceStage stage = RS_UNKNOWN;
        NaviPartId partId = INVALID_NAVI_PART_ID;
    };
    struct RequireNodeInfo {
        GraphId graphId = INVALID_GRAPH_ID;
        std::string node;
    };
public:
    InitResourceGraphBuilder(const std::string &bizName,
                             CreatorManager *creatorManager,
                             const ResourceStageMap &resourceStageMap,
                             NaviPartId partCount,
                             const std::vector<NaviPartId> &partIds,
                             GraphBuilder &builder);
    ~InitResourceGraphBuilder();
    InitResourceGraphBuilder(const InitResourceGraphBuilder &) = delete;
    InitResourceGraphBuilder &
    operator=(const InitResourceGraphBuilder &) = delete;
public:
    bool build(GraphId buildinGraphId, GraphId &resultGraphId);
private:
    bool addResourceToGraph(const std::string &name, bool require,
                            const RequireNodeInfo &requireInfo);
    bool addResource(GraphId graphId,
                     const std::string &name,
                     const std::string &producer,
                     bool require,
                     const RequireNodeInfo &requireInfo);
    bool addDependResource(GraphId graphId, const std::string &name);
    bool addReplaceResource(GraphId graphId, const std::string &name);
    GraphId addBuildinSubGraph();
    GraphId addBizSubGraph();
    void addBizPartSubGraph();
    GraphId addRunGraphSubGraph(GraphId requireGraph);
    bool addSaveToPublish(GraphId from);
    void addFlushInput(N n);
    SubGraphInfo getGraphInfo(GraphId graphId) const;
    void validate(const ResourceCreator *creator,
                  const RequireNodeInfo &requireInfo);
private:
    const std::string &_bizName;
    bool _isBuildin;
    CreatorManager *_creatorManager;
    const ResourceStageMap &_resourceStageMap;
    NaviPartId _partCount;
    std::vector<NaviPartId> _partIds;
    GraphBuilder &_builder;
    GraphId _buildinGraphId;
    GraphId _bizGraphId;
    std::map<NaviPartId, GraphId> _partGraphIds;
    std::unordered_map<GraphId, SubGraphInfo> _graphInfoMap;
    bool _hasError;
};

}

