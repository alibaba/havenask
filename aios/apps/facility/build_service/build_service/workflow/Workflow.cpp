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
#include "build_service/workflow/Workflow.h"

#include <memory>
#include <string>

#include "alog/Logger.h"

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, Workflow);

Workflow::Workflow(ProducerBase* producer, ConsumerBase* consumer) : _workflowItem(createItem(producer, consumer)) {}

Workflow::Workflow(WorkflowItemBase* item) : _workflowItem(item) {}

Workflow::Workflow(Workflow&& workflow)
{
    _workflowItem = workflow._workflowItem;
    workflow._workflowItem = nullptr;
}

Workflow::~Workflow()
{
    if (_workflowItem) {
        stop(StopOption::SO_NORMAL);
        delete _workflowItem;
    }
}

bool Workflow::start(WorkflowMode mode, const WorkflowThreadPoolPtr& pool)
{
    _workflowItem->start(mode);
    if (pool && pool->pushWorkItem(_workflowItem)) {
        BS_LOG(INFO, "use shared WorkflowThreadPool");
        _threadPool = pool;
        return true;
    } else if (pool) {
        BS_LOG(WARN, "pushWorkItem to shared WorkflowTheadPool fail!");
    }

    BS_LOG(INFO, "use self-own WorkflowThreadPool");
    _threadPool.reset(new WorkflowThreadPool(1, 1));
    if (!_threadPool->start("Bs" + std::to_string(int(mode)))) {
        BS_LOG(ERROR, "start WorkflowThreadPool fail!");
        return false;
    }
    return _threadPool->pushWorkItem(_workflowItem);
}

void Workflow::stop(StopOption stopOption) { _workflowItem->stop(stopOption); }

bool Workflow::isFinished() const { return _workflowItem->isFinished(); }

bool Workflow::hasFatalError() const { return _workflowItem->hasFatalError(); }
bool Workflow::needReconstruct() const { return _workflowItem->needReconstruct(); }

}} // namespace build_service::workflow
