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
#include "build_service/admin/taskcontroller/OperationTopoManager.h"

#include <algorithm>
#include <assert.h>
#include <google/protobuf/stubs/port.h>
#include <type_traits>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/jsonizable.h"

namespace build_service::admin {

BS_LOG_SETUP(admin, OperationTopoManager);

bool OperationTopoManager::init(const proto::OperationPlan& newplan)
{
    _opMap.clear();
    _executableOperations.clear();
    std::unique_ptr<proto::OperationDescription> endTaskOp;
    if (newplan.has_endtaskop()) {
        endTaskOp = std::make_unique<proto::OperationDescription>(newplan.endtaskop());
        endTaskOp->clear_depends();
        _endTaskOpId = endTaskOp->id();
    }
    for (const auto& desc : newplan.ops()) {
        auto op = std::make_unique<OpDef>();
        op->desc = desc;
        _opMap[desc.id()] = std::move(op);
        if (endTaskOp) {
            endTaskOp->add_depends(desc.id());
        }
    }
    if (endTaskOp) {
        auto op = std::make_unique<OpDef>();
        op->desc = *endTaskOp;
        _opMap[endTaskOp->id()] = std::move(op);
    }

    for (const auto& [opId, opDef] : _opMap) {
        const auto& desc = opDef->desc;
        const auto& depends = desc.depends();
        if (depends.empty()) {
            _executableOperations[desc.id()] = desc;
        }
        for (int64_t depend : depends) {
            auto iter = _opMap.find(depend);
            if (iter == _opMap.end()) {
                BS_LOG(ERROR, "depend [%ld] of op type[%s]id[%ld] not exist", depend, desc.type().c_str(), desc.id());
                return false;
            }
            (iter->second)->fanouts.push_back(desc.id());
        }
    }

    if (!checkPlan(_opMap)) {
        BS_LOG(ERROR, "invalid plan");
        return false;
    }
    return true;
}

bool OperationTopoManager::checkPlan(const OpMap& opMap) const
{
    std::vector<OpDef*> sorted;
    sorted.reserve(opMap.size());

    std::vector<size_t> readyDepends;
    readyDepends.resize(opMap.size(), 0);

    int front = 0;
    int back = 0;

    for (auto& [id, op] : opMap) {
        if (op->desc.depends().empty()) {
            sorted.push_back(op.get());
            ++back;
        }
    }
    while (front != back) {
        auto ready_node = sorted[front];
        for (auto fanout : ready_node->fanouts) {
            ++readyDepends[fanout];
            auto iter = opMap.find(fanout);
            assert(iter != opMap.end());
            if (readyDepends[fanout] == (iter->second)->desc.depends_size()) {
                sorted.push_back((iter->second).get());
                ++back;
            }
        }
        ++front;
    }
    if (back != opMap.size()) {
        BS_LOG(ERROR, "the plan cannot be sorted in topological order.");
        return false;
    }
    return true;
}

bool OperationTopoManager::isFinished(int64_t opId) const
{
    auto iter = _opMap.find(opId);
    if (iter == _opMap.end()) {
        return false;
    }
    return iter->second->status == proto::OP_FINISHED;
}

void OperationTopoManager::run(int64_t opId, uint32_t nodeId)
{
    auto iter = _opMap.find(opId);
    assert(iter != _opMap.end());
    if (iter->second->status == proto::OP_FINISHED) {
        return;
    }
    iter->second->status = proto::OP_RUNNING;
    iter->second->assignedNodeId = static_cast<int64_t>(nodeId);
    _executableOperations.erase(opId);
}

void OperationTopoManager::finish(int64_t opId, std::string workerEpochId, std::string resultInfo)
{
    if (_endTaskOpId == opId) {
        _taskResultInfo = resultInfo;
    }
    auto iter = _opMap.find(opId);
    assert(iter != _opMap.end());
    if (iter->second->status == proto::OP_FINISHED) {
        return;
    }
    iter->second->status = proto::OP_FINISHED;
    iter->second->finishedWorkerEpochId = workerEpochId;
    _executableOperations.erase(opId);
    ++_finishedOpCount;
    for (auto fanout : iter->second->fanouts) {
        auto fanoutIter = _opMap.find(fanout);
        assert(fanoutIter != _opMap.end());
        auto op = (fanoutIter->second).get();
        if (++(op->readyDepends) == op->desc.depends_size()) {
            for (auto depend : op->desc.depends()) {
                auto dependIter = _opMap.find(depend);
                assert(dependIter != _opMap.end());
                op->desc.add_dependworkerepochids(dependIter->second->finishedWorkerEpochId);
            }
            _executableOperations[op->desc.id()] = op->desc;
        }
    }
}

void OperationTopoManager::cancelRunningOps()
{
    for (const auto& [opId, opDef] : _opMap) {
        if (opDef->status == proto::OP_RUNNING) {
            opDef->status = proto::OP_PENDING;
            _executableOperations[opId] = opDef->desc;
        }
    }
}

const std::map<int64_t, proto::OperationDescription>& OperationTopoManager::getNewExecutableOperations() const
{
    return _executableOperations;
}

std::string OperationTopoManager::DebugString() const
{
    std::vector<std::pair<int64_t, int32_t>> ops;
    for (const auto& [opId, opDef] : _opMap) {
        ops.push_back(std::make_pair(opId, opDef->status));
    }
    return autil::legacy::ToJsonString(ops, /*isCompact=*/true);
}

} // namespace build_service::admin
