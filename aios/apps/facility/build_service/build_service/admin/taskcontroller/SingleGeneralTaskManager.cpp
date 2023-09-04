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
#include "build_service/admin/taskcontroller/SingleGeneralTaskManager.h"

#include <queue>

#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/ProtoJsonizer.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/util/URLParser.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

namespace build_service::admin {
BS_LOG_SETUP(admin, SingleGeneralTaskManager);

SingleGeneralTaskManager::SingleGeneralTaskManager(const std::string& id, const TaskResourceManagerPtr& resourceManager)
    : _id(id)
    , _resourceManager(resourceManager)
    , _currentState(State::RUNNING)
    , _lastParallelNum(0)
    , _isRecover(false)
{
}

bool SingleGeneralTaskManager::recover()
{
    bool planExist = false;
    auto ec = indexlib::file_system::FslibWrapper::IsExist(_planPath, planExist).Code();
    if (ec != indexlib::file_system::FSEC_OK) {
        BS_LOG(ERROR, "test plan[%s] existance failed", _planPath.c_str());
        return false;
    }
    if (!planExist) {
        BS_LOG(ERROR, "test plan[%s] not exist", _planPath.c_str());
        finish();
        return true;
    }
    proto::OperationPlan plan;
    if (!loadPlan(&plan)) {
        BS_LOG(ERROR, "load plan[%s] failed", _planPath.c_str());
        return false;
    }
    if (plan.has_taskname()) {
        _taskName = plan.taskname();
    }
    if (plan.has_tasktype()) {
        _taskType = plan.tasktype();
    }
    _topoManager = std::make_unique<OperationTopoManager>();
    if (!_topoManager->init(plan)) {
        BS_LOG(ERROR, "init new plan failed");
        return false;
    }
    size_t recordCnt = 0;
    _lastParallelNum = 0;
    static constexpr size_t retryLimit = 100;
    size_t retryCount = 0;
    while (true) {
        std::string recordStr;
        auto r = _wal->ReadRecord(recordStr);
        if (!r) {
            BS_LOG(WARN, "read wal failed");
            if (++retryCount > retryLimit) {
                BS_LOG(ERROR, "retry [%lu] exceed limit[%lu]", retryCount, retryLimit);
                return false;
            }
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(10ms);
            continue;
        }
        if (recordStr.empty()) {
            if (_wal->IsRecovered()) {
                break;
            }
            continue;
        }
        proto::GeneralTaskWalRecord record;
        if (!record.ParseFromString(recordStr)) {
            BS_LOG(ERROR, "read wal record[%lu] failed", recordCnt);
            return false;
        }
        for (const auto& opFinish : record.opfinish()) {
            _topoManager->finish(opFinish.opid(), opFinish.workerepochid(), opFinish.resultinfo());
        }
        if (_lastParallelNum != record.parallelnum()) {
            _topoManager->cancelRunningOps();
        }
        _lastParallelNum = record.parallelnum();
        for (const auto& opRun : record.oprun()) {
            _topoManager->run(opRun.opid(), opRun.nodeid());
        }
        ++recordCnt;
    }
    BS_LOG(INFO, "plan [%s] redo [%lu] records", _id.c_str(), recordCnt);
    _isRecover = true;
    {
        std::lock_guard<std::mutex> lock(_infoMutex);
        _taskInfo.totalOpCount = _topoManager->totalOpCount();
        _taskInfo.finishedOpCount = _topoManager->finishedOpCount();
    }
    if (_topoManager->finishedOpCount() == _topoManager->totalOpCount()) {
        finish();
    }
    return true;
}

bool SingleGeneralTaskManager::operator==(const SingleGeneralTaskManager& other) const { return true; }

bool SingleGeneralTaskManager::init(const KeyValueMap& initParam, const proto::OperationPlan* plan)
{
    auto branchIdStr = getValueFromKeyValueMap(initParam, "branch_id", /*default*/ "0");
    _taskInfo.branchId = autil::StringUtil::numberFromString<uint64_t>(branchIdStr);
    auto baseVersionIdStr = getValueFromKeyValueMap(initParam, "base_version_id", /*default*/ "");
    if (baseVersionIdStr.empty()) {
        BS_LOG(ERROR, "[base_version_id] not specified in initParam");
        return false;
    }
    _taskInfo.baseVersionId = autil::StringUtil::numberFromString<indexlibv2::versionid_t>(baseVersionIdStr);
    _taskInfo.clusterName = getValueFromKeyValueMap(initParam, "cluster_name", /*default*/ "");
    if (_taskInfo.clusterName.empty()) {
        BS_LOG(ERROR, "[cluster_name] not specified in initParam");
        return false;
    }

    indexlib::file_system::WAL::WALOption walOption;
    _partitionWorkRoot = getValueFromKeyValueMap(initParam, "task_work_root", /*default*/ "");
    if (_partitionWorkRoot.empty()) {
        BS_LOG(ERROR, "[task_work_root] not specified in initParam");
        return false;
    }
    _partitionWorkRoot = fslib::util::URLParser::eraseParam(_partitionWorkRoot, /*key*/ "ec");
    _planPath = indexlib::util::PathUtil::JoinPath(_partitionWorkRoot, "plan");
    walOption.workDir = indexlib::util::PathUtil::JoinPath(_partitionWorkRoot, "wal");
    walOption.isCheckSum = false;
    _wal = std::make_unique<indexlib::file_system::WAL>(walOption);
    if (!_wal->Init()) {
        BS_LOG(ERROR, "init wal at [%s] failed", walOption.workDir.c_str());
        return false;
    }
    if (plan) {
        if (plan->has_taskname()) {
            _taskName = plan->taskname();
        }
        if (plan->has_tasktype()) {
            _taskType = plan->tasktype();
        }
        _topoManager = std::make_unique<OperationTopoManager>();
        if (!_topoManager->init(*plan)) {
            BS_LOG(ERROR, "init new plan failed");
            return false;
        }
        if (!storePlan(*plan)) {
            BS_LOG(ERROR, "store plan failed");
            return false;
        }
    } else {
        if (!recover()) {
            BS_LOG(ERROR, "recover failed");
            return false;
        }
    }
    return true;
}

bool SingleGeneralTaskManager::storePlan(const proto::OperationPlan& plan)
{
    std::string content = proto::ProtoJsonizer::toJsonString(plan);
    auto ec = indexlib::file_system::FslibWrapper::AtomicStore(_planPath, content).Code();
    return ec == indexlib::file_system::FSEC_OK || ec == indexlib::file_system::FSEC_EXIST;
}

bool SingleGeneralTaskManager::loadPlan(proto::OperationPlan* plan)
{
    std::string content;
    auto ec = indexlib::file_system::FslibWrapper::AtomicLoad(_planPath, content).Code();
    if (ec != indexlib::file_system::FSEC_OK) {
        BS_LOG(ERROR, "load [%s] failed: error code[%d]", _planPath.c_str(), ec);
        return false;
    }
    return proto::ProtoJsonizer::fromJsonString(content, plan);
}

void SingleGeneralTaskManager::recoverNodeTargets(std::vector<GeneralNodeGroup>& nodeGroups)
{
    assert(nodeGroups.size() == _lastParallelNum);
    for (auto& [opId, opDef] : *_topoManager) {
        if (opDef->status != proto::OP_RUNNING) {
            continue;
        }
        assert(opDef->assignedNodeId >= 0 && static_cast<size_t>(opDef->assignedNodeId) < nodeGroups.size());
        assert(opDef->assignedNodeId < nodeGroups.size());
        nodeGroups[opDef->assignedNodeId].recoverNodeTarget(opDef->desc);
    }
    for (auto& nodeGroup : nodeGroups) {
        nodeGroup.updateTarget();
    }
}

void SingleGeneralTaskManager::prepareNodeGroup(TaskController::Nodes* nodes,
                                                std::vector<GeneralNodeGroup>& nodeGroups) const
{
    for (auto& node : *nodes) {
        if (node.sourceNodeId == -1) { // for main node
            GeneralNodeGroup nodeGroup(_taskType, _taskName, node.nodeId, _topoManager.get());
            nodeGroup.addNode(&node);
            nodeGroups.push_back(std::move(nodeGroup));
        }
    }
    for (auto& node : *nodes) {
        if (node.sourceNodeId != -1) { // for backup node
            assert(node.sourceNodeId < nodeGroups.size());
            auto& nodeGroup = nodeGroups[node.sourceNodeId];
            nodeGroup.addNode(&node);
        }
    }
}

bool SingleGeneralTaskManager::operate(TaskController::Nodes* nodes, uint32_t parallelNum, uint32_t nodeRunningOpLimit)
{
    if (_currentState == State::FINISH) {
        return true;
    }
    assert(nodes);
    assert(parallelNum > 0);
    std::vector<GeneralNodeGroup> nodeGroups;
    prepareNodeGroup(nodes, nodeGroups);
    if (nodeGroups.size() != parallelNum) {
        // backup node will not be created after upc
        nodes->clear();
        nodes->reserve(parallelNum);
        nodeGroups.clear();
        nodeGroups.reserve(parallelNum);
        for (size_t i = 0; i < parallelNum; ++i) {
            TaskController::Node node;
            node.nodeId = i;
            node.instanceIdx = i;
            nodes->push_back(node);
        }
        prepareNodeGroup(nodes, nodeGroups);
    }
    if (_isRecover && _lastParallelNum == parallelNum) {
        recoverNodeTargets(nodeGroups);
        _isRecover = false;
    }
    if (_lastParallelNum != parallelNum) {
        _topoManager->cancelRunningOps();
    }

    handlePlan(parallelNum, nodeRunningOpLimit, nodeGroups);

    _lastParallelNum = parallelNum;
    return _currentState == State::FINISH;
}

void SingleGeneralTaskManager::handlePlan(uint32_t parallelNum, uint32_t nodeRunningOpLimit,
                                          std::vector<GeneralNodeGroup>& nodeGroups)
{
    BS_LOG(DEBUG, "handle plan[%s], node size [%lu], parallelNum[%u]", _id.c_str(), nodeGroups.size(), parallelNum);
    assert(parallelNum == nodeGroups.size());

    auto cmp = [](const auto& lhs, const auto& rhs) {
        return lhs->getMinRunningOpCount() > rhs->getMinRunningOpCount();
    };
    std::priority_queue<GeneralNodeGroup*, std::vector<GeneralNodeGroup*>, decltype(cmp)> nodeGroupQueue(cmp);

    proto::GeneralTaskWalRecord walRecord;
    walRecord.set_parallelnum(parallelNum);
    for (auto& nodeGroup : nodeGroups) {
        if (!nodeGroup.handleFinishedOp(&walRecord)) {
            BS_LOG(ERROR, "handle finished op failed.");
            return;
        }
        nodeGroup.syncPendingOp(nodeRunningOpLimit);
        if (nodeGroup.hasStatus()) {
            nodeGroupQueue.push(&nodeGroup);
        }
    }

    int64_t remainOpCount = 0;
    {
        std::lock_guard<std::mutex> lock(_infoMutex);
        _taskInfo.totalOpCount = _topoManager->totalOpCount();
        _taskInfo.finishedOpCount = _topoManager->finishedOpCount();
        remainOpCount = _taskInfo.totalOpCount - _taskInfo.finishedOpCount;
    }
    reportMetric(remainOpCount);
    if (_topoManager->finishedOpCount() == _topoManager->totalOpCount()) {
        finish();
        return;
    }
    const auto& executableOps = _topoManager->getCurrentExecutableOperations();
    BS_LOG(DEBUG, "handle plan, executable ops size [%lu]", executableOps.size());
    // dispatch to nodes
    std::vector<const proto::OperationDescription*> ops;
    for (auto& [opId, desc] : executableOps) {
        ops.push_back(std::addressof(desc));
    }
    for (auto op : ops) {
        if (nodeGroupQueue.empty()) {
            break;
        }
        auto nodeGroup = nodeGroupQueue.top();
        nodeGroupQueue.pop();
        if (nodeGroup->getMinRunningOpCount() < nodeRunningOpLimit) {
            nodeGroup->dispatchExecutableOp(op, nodeRunningOpLimit, &walRecord);
            nodeGroupQueue.push(nodeGroup);
        } else {
            BS_LOG(DEBUG, "handle plan: reach limit");
            break;
        }
    }
    BS_LOG(DEBUG, "handle plan: finish[%d], run[%d]", walRecord.opfinish_size(), walRecord.oprun_size());
    if (walRecord.opfinish_size() != 0 || walRecord.oprun_size() != 0) {
        std::string content;
        auto r = walRecord.SerializeToString(&content);
        assert(r);
        r = _wal->AppendRecord(content);
        if (!r) {
            BS_LOG(ERROR, "write wal failed");
            return;
        }
    }
    for (auto opRun : walRecord.oprun()) {
        _topoManager->run(opRun.opid(), opRun.nodeid());
    }
    for (auto& nodeGroup : nodeGroups) {
        nodeGroup.serializeTarget();
    }
}

std::string SingleGeneralTaskManager::getTaskInfo() const
{
    proto::GeneralTaskInfo info;
    {
        std::lock_guard<std::mutex> lock(_infoMutex);
        info = _taskInfo;
    }
    return indexlib::file_system::JsonUtil::ToString(info).Value();
}

void SingleGeneralTaskManager::finish()
{
    _wal.reset();
    {
        std::lock_guard<std::mutex> lock(_infoMutex);
        if (_topoManager) {
            _taskInfo.result = _topoManager->getTaskResultInfo();
        }
    }
    _currentState = State::FINISH;
    BS_LOG(INFO, "task plan [%s] finished, total op count[%lu]", _id.c_str(),
           _topoManager ? _topoManager->totalOpCount() : 0);
}

void SingleGeneralTaskManager::supplementLableInfo(KeyValueMap& info) const
{
    info["task_epoch_id"] = _id;
    info["partition_work_root"] = _partitionWorkRoot;
    info["plan_path"] = _planPath;

    if (!_taskType.empty()) {
        info["plan_task_type"] = _taskType;
    }
    if (!_taskName.empty()) {
        info["plan_task_name"] = _taskName;
    }
    info["current_state"] = (_currentState == State::FINISH) ? "finish" : "running";
    std::lock_guard<std::mutex> lock(_infoMutex);
    info["base_version_id"] = autil::StringUtil::toString(_taskInfo.baseVersionId);
    info["branch_id"] = autil::StringUtil::toString(_taskInfo.branchId);
    info["operation_count"] = "total:" + autil::StringUtil::toString(_taskInfo.totalOpCount) +
                              ";finish:" + autil::StringUtil::toString(_taskInfo.finishedOpCount) + ";remain:" +
                              autil::StringUtil::toString(_taskInfo.totalOpCount - _taskInfo.finishedOpCount);
    info["cluster_name"] = autil::StringUtil::toString(_taskInfo.clusterName);
    info["task_result"] = autil::StringUtil::toString(_taskInfo.result);
}

void SingleGeneralTaskManager::reportMetric(int64_t remainOpCount)
{
    CheckpointMetricReporterPtr reporter;
    _resourceManager->getResource(reporter);
    if (!reporter) {
        return;
    }
    kmonitor::MetricsTags tags;
    tags.AddTag("cluster", _taskInfo.clusterName);
    tags.AddTag("branch_id", autil::StringUtil::toString(_taskInfo.branchId));
    tags.AddTag("epoch_id", autil::StringUtil::toString(_id));
    tags.AddTag("base_version", autil::StringUtil::toString(_taskInfo.baseVersionId));
    reporter->reportIndexTaskRemainOperationCount(remainOpCount, tags);
}

} // namespace build_service::admin
