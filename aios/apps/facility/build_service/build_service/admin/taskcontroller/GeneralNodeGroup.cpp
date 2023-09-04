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
#include "build_service/admin/taskcontroller/GeneralNodeGroup.h"

#include "build_service/admin/taskcontroller/OperationTopoManager.h"

namespace build_service::admin {

BS_LOG_SETUP(admin, GeneralNodeGroup);

void GeneralNodeGroup::addNode(TaskController::Node* node)
{
    _nodes.push_back(node);
    proto::OperationTarget target;
    target.set_taskname(_taskName);
    target.set_tasktype(_taskType);
    _newTargets.push_back(target);
    _runningOpCounts.resize(_nodes.size(), /*invalid count*/ 0);
}

size_t GeneralNodeGroup::getMinRunningOpCount() const
{
    size_t minRunningOpCount = -1;
    for (auto cnt : _runningOpCounts) {
        minRunningOpCount = std::min(minRunningOpCount, cnt);
    }
    return minRunningOpCount;
}

bool GeneralNodeGroup::hasStatus() const
{
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto& node = _nodes[i];
        if (!node->statusDescription.empty()) {
            return true;
        }
    }
    return false;
}

bool GeneralNodeGroup::handleFinishedOp(proto::GeneralTaskWalRecord* walRecord)
{
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto node = _nodes[i];
        proto::OperationCurrent opCurrent;
        if (node->statusDescription.empty()) {
            BS_LOG(DEBUG, "node [%s] status empty", node->roleName.c_str());
            continue;
        }
        if (!opCurrent.ParseFromString(node->statusDescription)) {
            BS_LOG(ERROR, "parse op current from node[%s] failed", node->roleName.c_str());
            return false;
        }

        proto::OperationTarget oldTarget;
        std::string content;
        if (node->taskTarget.getTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content)) {
            if (!oldTarget.ParseFromString(content)) {
                BS_LOG(ERROR, "parse old target failed, content[%s]", content.c_str());
                return false;
            }
        }
        for (auto& result : opCurrent.opresults()) {
            if (result.status() == proto::OP_FINISHED) {
                _topoManager->finish(result.id(), opCurrent.workerepochid(), result.resultinfo());
                BS_LOG(DEBUG, "handle plan, finish op[%ld] from [%s]", result.id(), opCurrent.workerepochid().c_str());
                auto opFinish = walRecord->add_opfinish();
                opFinish->set_opid(result.id());
                opFinish->set_workerepochid(opCurrent.workerepochid());
                opFinish->set_resultinfo(result.resultinfo());
            }
        }

        auto& newTarget = _newTargets[i];
        for (const auto& desc : oldTarget.ops()) {
            if (!_topoManager->isFinished(desc.id())) {
                *newTarget.add_ops() = desc;
                ++_runningOpCounts[i];
            }
        }
    }
    return true;
}

void GeneralNodeGroup::syncPendingOp(uint32_t nodeRunningOpLimit)
{
    std::map<int64_t, proto::OperationDescription> opId2OpDesc;
    std::map<int64_t, std::vector<size_t>> opId2NodeIds; // nodes which already have the op
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto node = _nodes[i];
        if (node->statusDescription.empty()) {
            BS_LOG(DEBUG, "node [%s] status empty", node->roleName.c_str());
            continue;
        }
        auto& newTarget = _newTargets[i];
        for (const auto& op : newTarget.ops()) {
            if (!_topoManager->isFinished(op.id())) {
                auto it = opId2NodeIds.find(op.id());
                if (it != opId2NodeIds.end()) {
                    it->second.push_back(i);
                } else {
                    std::vector<size_t> nodeIds = {i};
                    opId2NodeIds.insert(it, std::make_pair(op.id(), nodeIds));
                    opId2OpDesc[op.id()] = op;
                }
            }
        }
    }
    for (const auto& [opId, nodeIds] : opId2NodeIds) {
        if (nodeIds.size() >= _nodes.size()) {
            // op already in all nodes
            continue;
        }
        for (size_t i = 0; i < _nodes.size(); ++i) {
            auto node = _nodes[i];
            if (node->statusDescription.empty()) {
                BS_LOG(DEBUG, "node [%s] status empty", node->roleName.c_str());
                continue;
            }
            if (_runningOpCounts[i] >= nodeRunningOpLimit) {
                continue;
            }
            if (std::find(nodeIds.begin(), nodeIds.end(), i) == nodeIds.end()) {
                auto& newTarget = _newTargets[i];
                auto it = opId2OpDesc.find(opId);
                assert(it != opId2OpDesc.end());
                *newTarget.add_ops() = it->second;
                ++_runningOpCounts[i];
            }
        }
    }

    return;
}

void GeneralNodeGroup::dispatchExecutableOp(const proto::OperationDescription* op, uint32_t nodeRunningOpLimit,
                                            proto::GeneralTaskWalRecord* walRecord)
{
    assert(_runningOpCounts.size() == _newTargets.size());
    assert(_runningOpCounts.size() == _nodes.size());
    for (size_t i = 0; i < _nodes.size(); ++i) {
        if (_nodes[i]->statusDescription.empty()) {
            continue;
        }
        if (_runningOpCounts[i] >= nodeRunningOpLimit) {
            continue;
        }
        auto& target = _newTargets[i];
        *target.add_ops() = *op;
        BS_LOG(DEBUG, "handle plan: run op[%ld] by node[%u:%lu]", op->id(), _nodeId, i);
        auto opRun = walRecord->add_oprun();
        opRun->set_opid(op->id());
        opRun->set_nodeid(_nodeId);
        ++_runningOpCounts[i];
    }
}

void GeneralNodeGroup::serializeTarget() const
{
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto node = _nodes[i];
        if (node->statusDescription.empty()) {
            continue;
        }
        std::string content;
        [[maybe_unused]] auto r = _newTargets[i].SerializeToString(&content);
        BS_LOG(DEBUG, "handle plan: node[%lu] target: %s", i, _newTargets[i].ShortDebugString().c_str());
        BS_LOG(DEBUG, "handle plan: node[%lu] targetStr: %s", i, content.c_str());
        assert(r);
        node->taskTarget.updateTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content);
    }
    return;
}

void GeneralNodeGroup::updateTarget() const
{
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto node = _nodes[i];
        std::string content;
        [[maybe_unused]] auto r = _newTargets[i].SerializeToString(&content);
        assert(r);
        node->taskTarget.updateTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content);
    }
    return;
}

void GeneralNodeGroup::recoverNodeTarget(const proto::OperationDescription& opDesc)
{
    assert(_nodes.size() == _newTargets.size());
    for (size_t i = 0; i < _nodes.size(); ++i) {
        auto& target = _newTargets[i];
        *target.add_ops() = opDesc;
    }
}
} // namespace build_service::admin
