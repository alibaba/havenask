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

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/base64.h"
#include "build_service/admin/taskcontroller/OperationTopoManager.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/ProtoUtil.h"

namespace build_service::admin {

BS_LOG_SETUP(admin, GeneralNodeGroup);

GeneralNodeGroup::GeneralNodeGroup(const std::string& taskType, const std::string& taskName, uint32_t nodeId,
                                   OperationTopoManager* topoManager, bool enableTargetBase64Encode,
                                   const std::string& partitionWorkRoot)
    : _taskType(taskType)
    , _taskName(taskName)
    , _nodeId(nodeId)
    , _topoManager(topoManager)
    , _enableTargetBase64Encode(enableTargetBase64Encode)
    , _partitionWorkRoot(partitionWorkRoot)
{
    _target.set_tasktype(_taskType);
    _target.set_taskname(_taskName);
}

bool GeneralNodeGroup::collectFinishOp(proto::GeneralTaskWalRecord* walRecord)
{
    for (const auto& node : _nodes) {
        if (node->statusDescription.empty()) {
            continue;
        }
        proto::OperationCurrent opCurrent;
        if (!proto::ProtoUtil::parseBase64EncodedPbStr(node->statusDescription, &opCurrent)) {
            BS_LOG(ERROR, "parse op current from node[%s] failed, status desc[%s]", node->roleName.c_str(),
                   node->statusDescription.c_str());
            return false;
        }
        for (const auto& result : opCurrent.opresults()) {
            if (result.status() != proto::OP_FINISHED) {
                continue;
            }
            _topoManager->finish(result.id(), opCurrent.workerepochid(), result.resultinfo());
            auto opFinish = walRecord->add_opfinish();
            opFinish->set_opid(result.id());
            opFinish->set_workerepochid(opCurrent.workerepochid());
            opFinish->set_resultinfo(result.resultinfo());
            BS_LOG(INFO, "[%s] finish op[%ld] from [%s]", _partitionWorkRoot.c_str(), result.id(),
                   opCurrent.workerepochid().c_str());
        }
    }
    return true;
}

bool GeneralNodeGroup::updateTarget()
{
    if (_nodes.empty()) {
        BS_LOG(ERROR, "nodes is empty, no target to be updated");
        return false;
    }
    auto& node = _nodes[0];
    std::string content;
    if (!node->taskTarget.getTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content)) {
        BS_LOG(ERROR, "not found BS_GENERAL_TASK_OPERATION_TARGET in target description");
        return false;
    }
    proto::OperationTarget oldTarget;
    if (!proto::ProtoUtil::parseBase64EncodedPbStr(content, &oldTarget)) {
        BS_LOG(ERROR, "parse old general task operation failed. content [%s]", content.c_str());
        return false;
    }
    std::set<int64_t> opIds;
    for (const auto& op : _target.ops()) {
        opIds.insert(op.id());
    }
    for (const auto& op : oldTarget.ops()) {
        if (_topoManager->isFinished(op.id())) {
            continue;
        }
        if (opIds.find(op.id()) != opIds.end()) {
            continue;
        }
        assignOp(&op, /*walRecord=*/nullptr);
    }
    return true;
}

void GeneralNodeGroup::assignOp(const proto::OperationDescription* op, proto::GeneralTaskWalRecord* walRecord)
{
    *_target.add_ops() = *op;
    _runningOpCount++;
    if (walRecord) {
        auto opRun = walRecord->add_oprun();
        opRun->set_opid(op->id());
        opRun->set_nodeid(_nodeId);
    }
    BS_LOG(INFO, "[%s] run op[%ld] by node[%u]", _partitionWorkRoot.c_str(), op->id(), _nodeId);
}

void GeneralNodeGroup::serializeTarget()
{
    std::string content;
    [[maybe_unused]] auto r = _target.SerializeToString(&content);
    assert(r);
    if (_enableTargetBase64Encode) {
        content = autil::legacy::Base64EncodeFast(content);
    }
    for (const auto& node : _nodes) {
        node->taskTarget.updateTargetDescription(config::BS_GENERAL_TASK_OPERATION_TARGET, content);
    }
}

void GeneralNodeGroup::resume()
{
    for (const auto& node : _nodes) {
        node->isSuspended = false;
    }
}

void GeneralNodeGroup::suspend()
{
    for (const auto& node : _nodes) {
        node->isSuspended = true;
    }
}

} // namespace build_service::admin
