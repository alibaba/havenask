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
#ifndef ISEARCH_BS_WORKERTABLE_H
#define ISEARCH_BS_WORKERTABLE_H

#include "build_service/admin/WorkerTableBase.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class WorkerTable : public WorkerTableBase
{
public:
    WorkerTable(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper);
    ~WorkerTable();

private:
    WorkerTable(const WorkerTable&);
    WorkerTable& operator=(const WorkerTable&);

public:
    void clearAllNodes()
    {
        _jobNodesMap.clear();
        _workerNodesMap.clear();
    }

public:
    proto::WorkerNodes& getWorkerNodes(const std::string& taskId) { return _workerNodesMap[taskId]; }

    const proto::WorkerNodes& getWorkerNodes(const std::string& taskId) const
    {
        proto::WorkerNodesMap::const_iterator it = _workerNodesMap.find(taskId);
        if (it == _workerNodesMap.end()) {
            static proto::WorkerNodes emptyNodes;
            return emptyNodes;
        }
        return it->second;
    }

    proto::JobNodes& getJobNodes(const std::string& clusterName) { return _jobNodesMap[clusterName]; }

    const proto::JobNodes& getJobNodes(const std::string& clusterName) const
    {
        proto::JobNodesMap::const_iterator it = _jobNodesMap.find(clusterName);
        if (it == _jobNodesMap.end()) {
            static proto::JobNodes emptyJobNodes;
            return emptyJobNodes;
        }
        return it->second;
    }

public:
    proto::WorkerNodes getActiveNodes() const override;
    proto::WorkerNodes getAllNodes() const override;

    template <typename functorType>
    void forEachActiveNode(functorType& functor) const;

    template <typename MapType, typename functorType>
    bool forEachActiveNodeInMap(const MapType& nodesMap, functorType& functor) const;

public:
    // only for test
    const proto::WorkerNodesMap& getWorkerNodesMap() const { return _workerNodesMap; }

private:
    bool isActiveNode(const proto::WorkerNodeBasePtr node) const
    {
        if (node->keepAliveAfterFinish()) {
            return true;
        }
        return !node->isFinished() && !node->isSuspended();
    }

private:
    proto::JobNodesMap _jobNodesMap;
    proto::WorkerNodesMap _workerNodesMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(WorkerTable);

////////////////////////////////////////////////////////

template <typename functorType>
void WorkerTable::forEachActiveNode(functorType& functor) const
{
    if (!forEachActiveNodeInMap<proto::JobNodesMap, functorType>(_jobNodesMap, functor)) {
        return;
    }

    if (!forEachActiveNodeInMap<proto::WorkerNodesMap, functorType>(_workerNodesMap, functor)) {
        return;
    }
}

template <typename MapType, typename functorType>
bool WorkerTable::forEachActiveNodeInMap(const MapType& nodesMap, functorType& functor) const
{
    for (typename MapType::const_iterator it = nodesMap.begin(); it != nodesMap.end(); ++it) {
        typedef typename MapType::mapped_type VecType;
        typedef typename VecType::const_iterator VecIterType;
        const VecType& nodesVec = it->second;
        for (VecIterType nodeIt = nodesVec.begin(); nodeIt != nodesVec.end(); ++nodeIt) {
            if (isActiveNode(*nodeIt)) {
                if (!functor(*nodeIt)) {
                    return false;
                }
            }
        }
    }
    return true;
}

}} // namespace build_service::admin

#endif // ISEARCH_BS_WORKERTABLE_H
