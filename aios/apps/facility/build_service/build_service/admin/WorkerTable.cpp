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
#include "build_service/admin/WorkerTable.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, WorkerTable);

WorkerTable::WorkerTable(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper) : WorkerTableBase(zkRoot, zkWrapper)
{
}

WorkerTable::~WorkerTable() {}

proto::WorkerNodes WorkerTable::getActiveNodes() const
{
    struct CollectNodesFunctor {
        bool operator()(proto::WorkerNodeBasePtr node)
        {
            nodes.push_back(node);
            return true;
        }
        std::vector<proto::WorkerNodeBasePtr> nodes;
    };
    CollectNodesFunctor collector;
    forEachActiveNode(collector);
    return collector.nodes;
}

proto::WorkerNodes WorkerTable::getAllNodes() const
{
    std::vector<proto::WorkerNodeBasePtr> nodes;
    for (auto it = _jobNodesMap.begin(); it != _jobNodesMap.end(); ++it) {
        const auto& nodesVec = it->second;
        for (auto nodeIt = nodesVec.begin(); nodeIt != nodesVec.end(); ++nodeIt) {
            nodes.push_back(*nodeIt);
        }
    }
    for (auto it = _workerNodesMap.begin(); it != _workerNodesMap.end(); ++it) {
        const auto& nodesVec = it->second;
        for (auto nodeIt = nodesVec.begin(); nodeIt != nodesVec.end(); ++nodeIt) {
            nodes.push_back(*nodeIt);
        }
    }
    return nodes;
}

}} // namespace build_service::admin
