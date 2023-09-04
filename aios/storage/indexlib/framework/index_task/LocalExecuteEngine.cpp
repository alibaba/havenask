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
#include "indexlib/framework/index_task/LocalExecuteEngine.h"

#include "future_lite/MoveWrapper.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperation.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, LocalExecuteEngine);

LocalExecuteEngine::LocalExecuteEngine(future_lite::Executor* executor,
                                       std::unique_ptr<IIndexOperationCreator> operationCreator)
    : _executor(executor)
    , _operationCreator(std::move(operationCreator))
{
}

namespace {

class ScheduleAwaiter
{
private:
    IndexOperation* _op;
    future_lite::Executor* _executor;
    const IndexTaskContext& _context;

    Status _status;

public:
    ScheduleAwaiter(IndexOperation* op, future_lite::Executor* e, const IndexTaskContext& context)
        : _op(op)
        , _executor(e)
        , _context(context)
    {
    }

    constexpr bool await_ready() const { return false; }
    void await_suspend(std::coroutine_handle<> h)
    {
        _executor->schedule([this, h]() mutable {
            _status = _op->ExecuteWithLog(_context);
            h.resume();
        });
    }
    Status await_resume() const { return _status; };
};

} // namespace

future_lite::coro::Lazy<Status> LocalExecuteEngine::Schedule(const IndexOperationDescription& desc,
                                                             IndexTaskContext* context)
{
    auto op = _operationCreator->CreateOperation(desc);
    if (!op) {
        AUTIL_LOG(ERROR, "create operation id[%ld], type[%s] failed", desc.GetId(), desc.GetType().c_str());
        co_return Status::Corruption();
    }
    if (_executor) {
        ScheduleAwaiter awaiter(op.get(), _executor, *context);
        co_return co_await awaiter;
    } else {
        auto status = op->ExecuteWithLog(*context);
        co_return status;
    }
}

future_lite::coro::Lazy<Status> LocalExecuteEngine::ScheduleTask(const IndexTaskPlan& taskPlan,
                                                                 IndexTaskContext* context)
{
    // TODO(hanyao): run task in another thread/executor
    auto planStr = indexlib::file_system::JsonUtil::ToString(taskPlan, true).Value();
    AUTIL_LOG(INFO, "start task plan[%s]", planStr.c_str());
    auto nodeMap = InitNodeMap(taskPlan);
    if (nodeMap.size() != taskPlan.GetOpDescs().size()) {
        AUTIL_LOG(ERROR, "init nodes failed, topo size[%lu], desc size[%lu]", nodeMap.size(),
                  taskPlan.GetOpDescs().size());
        co_return Status::Corruption();
    }
    auto stages = ComputeTopoStages(nodeMap);
    if (stages.empty() && !taskPlan.GetOpDescs().empty()) {
        AUTIL_LOG(ERROR, "compute topo failed");
        co_return Status::Corruption();
    }

    for (auto stage : stages) {
        std::vector<future_lite::coro::RescheduleLazy<Status>> ops;
        std::vector<std::unique_ptr<IndexTaskContext>> sessionContexts;
        for (auto node : stage) {
            sessionContexts.push_back(std::make_unique<IndexTaskContext>(*context));
            auto status = FillDependOpFences(node->desc, sessionContexts.back().get());
            if (!status.IsOK()) {
                co_return status;
            }
            ops.push_back(Schedule(node->desc, sessionContexts.back().get()).via(_executor));
        }
        auto results = co_await future_lite::coro::collectAll(std::move(ops));
        for (auto& r : results) {
            if (!r.value().IsOK()) {
                AUTIL_LOG(ERROR, "execute op failed: %s", r.value().ToString().c_str());
                co_return r.value();
            }
        }
    }
    auto endTaskOpDesc = taskPlan.GetEndTaskOpDesc();
    if (endTaskOpDesc) {
        co_return co_await Schedule(*endTaskOpDesc, context);
    }
    co_return Status::OK();
}

LocalExecuteEngine::NodeMap LocalExecuteEngine::InitNodeMap(const IndexTaskPlan& taskPlan)
{
    NodeMap nodeMap;
    for (auto nodeDesc : taskPlan.GetOpDescs()) {
        auto node = std::make_unique<NodeDef>();
        node->desc = nodeDesc;
        nodeMap[nodeDesc.GetId()] = std::move(node);
    }

    for (auto nodeDesc : taskPlan.GetOpDescs()) {
        for (const IndexOperationId depend : nodeDesc.GetDepends()) {
            auto iter = nodeMap.find(depend);
            if (iter == nodeMap.end()) {
                AUTIL_LOG(ERROR, "depend[%ld] not exist", depend);
                return NodeMap();
            }
            (iter->second)->fanouts.push_back(nodeDesc.GetId());
        }
    }
    return nodeMap;
}

std::list<std::vector<LocalExecuteEngine::NodeDef*>>
LocalExecuteEngine::ComputeTopoStages(const LocalExecuteEngine::NodeMap& nodeMap)
{
    std::list<std::vector<NodeDef*>> stages;
    std::vector<NodeDef*> start;
    size_t nodeCount = 0;
    for (auto& [id, node] : nodeMap) {
        if (node->desc.GetDepends().empty()) {
            start.push_back(node.get());
            ++nodeCount;
        }
    }

    if (!start.empty()) {
        stages.push_back(std::move(start));
    }
    auto currentStage = stages.begin();
    while (currentStage != stages.end()) {
        std::vector<NodeDef*> nextNodes;
        auto& currentStageNodes = *currentStage;
        for (auto node : currentStageNodes) {
            for (auto fanout : node->fanouts) {
                auto nodeIter = nodeMap.find(fanout);
                if (nodeIter == nodeMap.end()) {
                    AUTIL_LOG(ERROR, "task error, id[%ld] not found", fanout);
                    return {};
                }
                auto fanoutNode = (nodeIter->second).get();
                if (++(fanoutNode->readyDepends) == fanoutNode->desc.GetDepends().size()) {
                    nextNodes.push_back(fanoutNode);
                    ++nodeCount;
                }
            }
        }
        if (!nextNodes.empty()) {
            stages.push_back(std::move(nextNodes));
        }
        ++currentStage;
    }
    if (nodeCount != nodeMap.size()) {
        AUTIL_LOG(ERROR, "the task couldn't sorted in topological order");
        return {};
    }
    return stages;
}
Status LocalExecuteEngine::FillDependOpFences(const IndexOperationDescription& desc, IndexTaskContext* context)
{
    const auto& depends = desc.GetDepends();
    bool useOpFenceDir = desc.UseOpFenceDir();
    std::map<IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> ops;
    for (const auto& opId : depends) {
        auto [status, fenceRoot] = context->GetOpFenceRoot(opId, useOpFenceDir);
        if (!status.IsOK()) {
            return status;
        }
        ops[opId] = fenceRoot;
    }
    context->SetFinishedOpFences(std::move(ops));
    return Status::OK();
}

} // namespace indexlibv2::framework
